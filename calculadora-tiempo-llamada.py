import io
import requests
import pandas as pd
import streamlit as st
from datetime import time

st.set_page_config(
    page_title="Primera llamada por asignación (flow Pipedrive)",
    layout="wide"
)

st.title("📞 Primera llamada por asignación usando Flow de Pipedrive")
st.write(
    "Sube un Excel para obtener los negocios a analizar. "
    "La app usa el flow API de Pipedrive como fuente de verdad para reconstruir "
    "creación del negocio, reaperturas, reasignaciones y actividades, "
    "y calcula la primera llamada/contacto tras cada tramo operativo."
)

uploaded = st.file_uploader("Sube tu Excel (.xlsx)", type=["xlsx"])

contact_mode = st.radio(
    "Qué quieres medir",
    ["Primera llamada saliente", "Primer contacto (llamada + WhatsApp)"],
    horizontal=True,
)

apply_filter_1day = st.checkbox(
    "Excluir tramos cuyo primer contacto tarde 1 día o más",
    value=False
)

hide_segments_without_contact = st.checkbox(
    "Ocultar tramos sin contacto",
    value=False
)

st.subheader("🔐 Configuración API Pipedrive")
api_token = st.text_input("API token", type="password")
company_domain = st.text_input("Subdominio de Pipedrive", placeholder="tuempresa")

COL_DEAL_ID = "Negocio - ID"
COL_CREATED = "Negocio - Negocio creado el"

ONE_DAY_SECONDS = 86400
LOCAL_TIMEZONE = "Europe/Madrid"

HOLIDAYS_2026 = {
    pd.Timestamp("2026-01-01").date(),
    pd.Timestamp("2026-01-06").date(),
    pd.Timestamp("2026-04-03").date(),
    pd.Timestamp("2026-05-01").date(),
    pd.Timestamp("2026-08-15").date(),
    pd.Timestamp("2026-10-12").date(),
    pd.Timestamp("2026-11-01").date(),
    pd.Timestamp("2026-12-08").date(),
    pd.Timestamp("2026-12-25").date(),
}

TEAM_SCHEDULE = {
    0: [(time(9, 0), time(20, 0))],
    1: [(time(9, 0), time(20, 0))],
    2: [(time(9, 0), time(20, 0))],
    3: [(time(9, 0), time(20, 0))],
    4: [(time(9, 0), time(20, 0))],
    5: [(time(12, 30), time(20, 0))],
}


def clean_text(value) -> str:
    if pd.isna(value) or value is None:
        return ""
    return str(value).strip()


def to_madrid_ts(value):
    if value in (None, "", pd.NaT):
        return pd.NaT
    ts = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(ts):
        return pd.NaT
    return ts.tz_convert(LOCAL_TIMEZONE).tz_localize(None)


def get_activity_datetime_local(activity_data: dict) -> pd.Timestamp:
    due_date = clean_text(activity_data.get("due_date"))
    due_time = clean_text(activity_data.get("due_time"))

    if due_date and due_time:
        dt_utc = pd.to_datetime(f"{due_date} {due_time}", errors="coerce", utc=True)
        if pd.notna(dt_utc):
            return dt_utc.tz_convert(LOCAL_TIMEZONE).tz_localize(None)

    if due_date:
        dt_utc = pd.to_datetime(f"{due_date} 00:00:00", errors="coerce", utc=True)
        if pd.notna(dt_utc):
            return dt_utc.tz_convert(LOCAL_TIMEZONE).tz_localize(None)

    for field in ["marked_as_done_time", "add_time", "update_time", "timestamp"]:
        value = activity_data.get(field)
        ts = to_madrid_ts(value)
        if pd.notna(ts):
            return ts

    return pd.NaT


def is_holiday(ts: pd.Timestamp) -> bool:
    return ts.date() in HOLIDAYS_2026


def get_day_windows(ts: pd.Timestamp):
    if is_holiday(ts):
        return []

    weekday = ts.weekday()
    windows = TEAM_SCHEDULE.get(weekday, [])

    return [
        (
            pd.Timestamp.combine(ts.date(), start_t),
            pd.Timestamp.combine(ts.date(), end_t),
        )
        for start_t, end_t in windows
    ]


def move_to_next_work_moment(ts: pd.Timestamp) -> pd.Timestamp:
    if pd.isna(ts):
        return ts

    cur = ts

    for _ in range(370):
        windows = get_day_windows(cur)

        if not windows:
            cur = pd.Timestamp(cur.date()) + pd.Timedelta(days=1)
            cur = cur.replace(hour=0, minute=0, second=0, microsecond=0)
            continue

        for start_dt, end_dt in windows:
            if cur <= start_dt:
                return start_dt
            if start_dt <= cur < end_dt:
                return cur

        cur = pd.Timestamp(cur.date()) + pd.Timedelta(days=1)
        cur = cur.replace(hour=0, minute=0, second=0, microsecond=0)

    return ts


def business_seconds_between(start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> float:
    if pd.isna(start_ts) or pd.isna(end_ts):
        return float("nan")
    if end_ts < start_ts:
        return float("nan")

    cur = move_to_next_work_moment(start_ts)
    total_seconds = 0.0

    for _ in range(370):
        if cur >= end_ts:
            break

        windows = get_day_windows(cur)
        if not windows:
            cur = pd.Timestamp(cur.date()) + pd.Timedelta(days=1)
            cur = cur.replace(hour=0, minute=0, second=0, microsecond=0)
            continue

        progressed = False

        for start_dt, end_dt in windows:
            if cur < start_dt:
                cur = start_dt

            if start_dt <= cur < end_dt:
                segment_end = min(end_dt, end_ts)
                total_seconds += (segment_end - cur).total_seconds()
                cur = segment_end
                progressed = True

                if cur >= end_ts:
                    break

        if cur >= end_ts:
            break

        if not progressed or all(cur >= end_dt for _, end_dt in windows):
            cur = pd.Timestamp(cur.date()) + pd.Timedelta(days=1)
            cur = cur.replace(hour=0, minute=0, second=0, microsecond=0)

    return total_seconds


def format_duration_exact(seconds: float) -> str:
    if pd.isna(seconds):
        return ""
    sign = "-" if seconds < 0 else ""
    total_seconds = abs(int(seconds))

    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)

    if days > 0:
        return f"{sign}{days}d {hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{sign}{hours:02d}:{minutes:02d}:{secs:02d}"


@st.cache_data(show_spinner=False)
def fetch_deal_flow(_api_token: str, _company_domain: str, deal_id: int) -> dict:
    url = f"https://{_company_domain}.pipedrive.com/api/v1/deals/{deal_id}/flow?api_token={_api_token}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def get_contact_pattern(selected_mode: str) -> str:
    if selected_mode == "Primera llamada saliente":
        return r"llamada saliente"
    return r"llamada saliente|whatsapp chat"


def get_result_labels(selected_mode: str):
    if selected_mode == "Primera llamada saliente":
        return {
            "title": "✅ Primera llamada saliente por tramo operativo",
            "metric_count": "Tramos con 1ª llamada",
            "time_col": "tiempo_hasta_primera_llamada",
            "download_name": "primera_llamada_por_tramo_flow.xlsx",
        }
    return {
        "title": "✅ Primer contacto por tramo operativo",
        "metric_count": "Tramos con 1er contacto",
        "time_col": "tiempo_hasta_primer_contacto",
        "download_name": "primer_contacto_por_tramo_flow.xlsx",
    }


def extract_created_time_from_flow(flow_json: dict, fallback_created: pd.Timestamp) -> pd.Timestamp:
    created_candidates = []

    for item in flow_json.get("data", []) or []:
        obj = item.get("object")
        data = item.get("data", {}) or {}

        if obj == "deal":
            for field in ["add_time", "log_time", "update_time"]:
                ts = to_madrid_ts(data.get(field))
                if pd.notna(ts):
                    created_candidates.append(ts)

        if obj == "dealChange" and data.get("field_key") == "add_time":
            ts = to_madrid_ts(data.get("log_time"))
            if pd.notna(ts):
                created_candidates.append(ts)

    if created_candidates:
        return min(created_candidates)

    return fallback_created


def extract_owner_changes(flow_json: dict) -> pd.DataFrame:
    rows = []

    for item in flow_json.get("data", []) or []:
        obj = item.get("object")
        data = item.get("data", {}) or {}

        if obj == "dealChange" and data.get("field_key") == "user_id":
            event_time = to_madrid_ts(data.get("log_time"))
            old_owner = clean_text((data.get("additional_data") or {}).get("old_value_formatted"))
            new_owner = clean_text((data.get("additional_data") or {}).get("new_value_formatted"))

            if pd.notna(event_time) and new_owner:
                rows.append({
                    "event_time": event_time,
                    "old_owner": old_owner,
                    "new_owner": new_owner,
                })

    if not rows:
        return pd.DataFrame(columns=["event_time", "old_owner", "new_owner"])

    return pd.DataFrame(rows).sort_values("event_time").reset_index(drop=True)


def extract_reopen_events(flow_json: dict) -> pd.DataFrame:
    """
    Detecta reaperturas reales: lost -> open / Perdido -> Abierto
    """
    rows = []

    for item in flow_json.get("data", []) or []:
        if item.get("object") != "dealChange":
            continue

        data = item.get("data", {}) or {}
        if data.get("field_key") != "status":
            continue

        old_value = clean_text(data.get("old_value")).lower()
        new_value = clean_text(data.get("new_value")).lower()

        if old_value == "lost" and new_value == "open":
            event_time = to_madrid_ts(data.get("log_time"))
            if pd.notna(event_time):
                rows.append({
                    "event_time": event_time,
                    "event_type": "reopened",
                })

    if not rows:
        return pd.DataFrame(columns=["event_time", "event_type"])

    return pd.DataFrame(rows).sort_values("event_time").reset_index(drop=True)


def extract_initial_owner_from_flow(flow_json: dict) -> str:
    candidates = []

    for item in flow_json.get("data", []) or []:
        if item.get("object") != "activity":
            continue

        data = item.get("data", {}) or {}
        owner_name = clean_text(data.get("owner_name"))
        if not owner_name:
            continue

        ts = get_activity_datetime_local(data)
        if pd.isna(ts):
            fallback_times = [
                to_madrid_ts(data.get("add_time")),
                to_madrid_ts(data.get("marked_as_done_time")),
                to_madrid_ts(data.get("update_time")),
            ]
            ts = next((x for x in fallback_times if pd.notna(x)), pd.NaT)

        if pd.notna(ts):
            candidates.append((ts, owner_name))

    if not candidates:
        return ""

    candidates = sorted(candidates, key=lambda x: x[0])
    return candidates[0][1]


def extract_first_lead_to_advanced_stage(flow_json: dict):
    changes = []

    for item in flow_json.get("data", []) or []:
        if item.get("object") != "dealChange":
            continue

        data = item.get("data", {}) or {}
        if data.get("field_key") != "stage_id":
            continue

        add = data.get("additional_data") or {}
        old_stage = clean_text(add.get("old_value_formatted"))
        new_stage = clean_text(add.get("new_value_formatted"))

        old_stage_norm = old_stage.lower().strip()
        new_stage_norm = new_stage.lower().strip()

        if old_stage_norm == "lead" and new_stage_norm not in {"", "lead"}:
            ts = to_madrid_ts(data.get("log_time"))
            if pd.notna(ts):
                changes.append({
                    "stage_change_time": ts,
                    "old_stage": old_stage,
                    "new_stage": new_stage,
                })

    if not changes:
        return pd.NaT, "", ""

    first_change = sorted(changes, key=lambda x: x["stage_change_time"])[0]
    return first_change["stage_change_time"], first_change["old_stage"], first_change["new_stage"]


def extract_flow_activities(flow_json: dict, selected_mode: str) -> pd.DataFrame:
    rows = []
    pattern = get_contact_pattern(selected_mode)

    for item in flow_json.get("data", []) or []:
        if item.get("object") != "activity":
            continue

        data = item.get("data", {}) or {}
        subject = clean_text(data.get("subject"))

        if not subject:
            continue

        if not pd.Series([subject]).str.contains(pattern, case=False, na=False).iloc[0]:
            continue

        activity_time = get_activity_datetime_local(data)
        if pd.isna(activity_time):
            continue

        rows.append({
            "activity_time": activity_time,
            "activity_subject": subject,
            "activity_id": data.get("id"),
            "activity_type": clean_text(data.get("type")),
            "activity_done": data.get("done"),
            "owner_name": clean_text(data.get("owner_name")),
            "assigned_to_user_id": data.get("assigned_to_user_id"),
            "user_id": data.get("user_id"),
            "due_date": clean_text(data.get("due_date")),
            "due_time": clean_text(data.get("due_time")),
            "add_time_raw": clean_text(data.get("add_time")),
            "marked_as_done_time_raw": clean_text(data.get("marked_as_done_time")),
        })

    if not rows:
        return pd.DataFrame(columns=[
            "activity_time", "activity_subject", "activity_id", "activity_type",
            "activity_done", "owner_name", "assigned_to_user_id", "user_id",
            "due_date", "due_time", "add_time_raw", "marked_as_done_time_raw"
        ])

    return pd.DataFrame(rows).sort_values("activity_time").reset_index(drop=True)


def has_contact_before_stage_change(flow_activities: pd.DataFrame, stage_change_time: pd.Timestamp) -> bool:
    if flow_activities.empty or pd.isna(stage_change_time):
        return False

    prior = flow_activities[flow_activities["activity_time"] < stage_change_time].copy()
    return len(prior) > 0


def build_assignment_segments(
    deal_id: int,
    deal_created: pd.Timestamp,
    owner_changes: pd.DataFrame,
    reopen_events: pd.DataFrame,
    initial_owner: str = ""
) -> pd.DataFrame:
    rows = []

    initial_owner = clean_text(initial_owner)

    if initial_owner:
    rows.append({
        "deal_id": deal_id,
        "segment_start": move_to_next_work_moment(deal_created),
        "segment_source": "initial_owner_inferred",
        "from_owner": "",
        "to_owner": initial_owner,
        "agent_owner": initial_owner,
    })

    for _, ch in owner_changes.iterrows():
        rows.append({
            "deal_id": deal_id,
            "segment_start": ch["event_time"],
            "segment_source": "owner_reassignment",
            "from_owner": ch["old_owner"],
            "to_owner": ch["new_owner"],
            "agent_owner": ch["new_owner"],
        })

    for _, rp in reopen_events.iterrows():
        rp_time = rp["event_time"]

        owner_at_reopen = ""
        prior_changes = owner_changes[owner_changes["event_time"] <= rp_time].copy()
        if len(prior_changes) > 0:
            owner_at_reopen = clean_text(prior_changes.iloc[-1]["new_owner"])
        elif initial_owner:
            owner_at_reopen = initial_owner

        if owner_at_reopen:
            rows.append({
                "deal_id": deal_id,
                "segment_start": move_to_next_work_moment(rp_time),
                "segment_source": "reopened",
                "from_owner": "",
                "to_owner": owner_at_reopen,
                "agent_owner": owner_at_reopen,
            })

    if not rows:
        return pd.DataFrame(columns=[
            "deal_id", "segment_start", "segment_source",
            "from_owner", "to_owner", "agent_owner", "segment_end", "deal_created"
        ])

    seg = pd.DataFrame(rows).sort_values("segment_start").reset_index(drop=True)

    # quitar duplicados exactos por inicio + owner + source
    seg = seg.drop_duplicates(subset=["segment_start", "agent_owner", "segment_source"]).copy()

    # si hay varios eventos casi simultáneos, nos quedamos con el más operativo
    source_priority = {
        "reopened": 3,
        "owner_reassignment": 2,
        "initial_owner_inferred": 1,
    }
    seg["source_priority"] = seg["segment_source"].map(source_priority).fillna(0)

    seg = seg.sort_values(["segment_start", "source_priority"], ascending=[True, False]).reset_index(drop=True)

    # quitar duplicados consecutivos del mismo owner si el inicio es el mismo o casi mismo momento lógico
    cleaned_rows = []
    for _, row in seg.iterrows():
        if not cleaned_rows:
            cleaned_rows.append(row.to_dict())
            continue

        prev = cleaned_rows[-1]
        same_owner = clean_text(prev["agent_owner"]) == clean_text(row["agent_owner"])
        same_start = pd.Timestamp(prev["segment_start"]) == pd.Timestamp(row["segment_start"])

        if same_owner and same_start:
            if row["source_priority"] > prev["source_priority"]:
                cleaned_rows[-1] = row.to_dict()
        else:
            cleaned_rows.append(row.to_dict())

    seg = pd.DataFrame(cleaned_rows).sort_values("segment_start").reset_index(drop=True)

    seg["segment_end"] = seg["segment_start"].shift(-1)
    seg["deal_created"] = deal_created

    if "source_priority" in seg.columns:
        seg = seg.drop(columns=["source_priority"])

    return seg


def assign_owner_to_flow_activities(activities_df: pd.DataFrame, segments_df: pd.DataFrame) -> pd.DataFrame:
    acts = activities_df.copy()
    acts["real_owner"] = pd.NA
    acts["segment_index_owner"] = pd.NA

    if acts.empty or segments_df.empty:
        return acts

    for idx, seg in segments_df.iterrows():
        start = seg["segment_start"]
        end = seg["segment_end"]
        owner = seg["agent_owner"]
        segment_index = idx + 1

        if pd.isna(end):
            mask = acts["activity_time"] >= start
        else:
            mask = (acts["activity_time"] >= start) & (acts["activity_time"] < end)

        acts.loc[mask, "real_owner"] = owner
        acts.loc[mask, "segment_index_owner"] = segment_index

    return acts


def compute_from_flow(deals_df: pd.DataFrame, apply_filter_1day: bool, selected_mode: str):
    labels = get_result_labels(selected_mode)

    rows = []
    debug_segments = []
    debug_activities = []
    excluded_stage_without_contact = []

    deal_ids = (
        pd.to_numeric(deals_df[COL_DEAL_ID], errors="coerce")
        .dropna()
        .astype(int)
        .drop_duplicates()
        .tolist()
    )

    deal_created_map = {}
    if COL_CREATED in deals_df.columns:
        tmp = deals_df[[COL_DEAL_ID, COL_CREATED]].copy()
        tmp[COL_DEAL_ID] = pd.to_numeric(tmp[COL_DEAL_ID], errors="coerce").astype("Int64")
        tmp[COL_CREATED] = pd.to_datetime(tmp[COL_CREATED], errors="coerce")
        tmp = tmp.dropna(subset=[COL_DEAL_ID]).copy()
        deal_created_map = tmp.groupby(COL_DEAL_ID)[COL_CREATED].min().to_dict()

    progress = st.progress(0)
    total = len(deal_ids)

    for i, deal_id in enumerate(deal_ids, start=1):
        fallback_created = deal_created_map.get(deal_id, pd.NaT)

        try:
            flow_json = fetch_deal_flow(api_token, company_domain, deal_id)
        except Exception as e:
            rows.append({
                "deal_id": deal_id,
                "segment_index": pd.NA,
                "segment_source": "flow_error",
                "from_owner": "",
                "to_owner": "",
                "agent_owner": "",
                "deal_created": fallback_created,
                "segment_start": pd.NaT,
                "segment_start_adjusted": pd.NaT,
                "segment_end": pd.NaT,
                "first_contact_time": pd.NaT,
                "first_contact_subject": "",
                "delta_sec": float("nan"),
                "has_contact": False,
                "flow_error": str(e),
            })
            progress.progress(i / total if total else 1)
            continue

        deal_created = extract_created_time_from_flow(flow_json, fallback_created)
        owner_changes = extract_owner_changes(flow_json)
        reopen_events = extract_reopen_events(flow_json)
        flow_activities = extract_flow_activities(flow_json, selected_mode)

        first_stage_change_time, old_stage, new_stage = extract_first_lead_to_advanced_stage(flow_json)

        if pd.notna(first_stage_change_time):
            had_contact_before = has_contact_before_stage_change(flow_activities, first_stage_change_time)

            if not had_contact_before:
                excluded_stage_without_contact.append({
                    "deal_id": deal_id,
                    "deal_created": deal_created,
                    "first_stage_change_time": first_stage_change_time,
                    "old_stage": old_stage,
                    "new_stage": new_stage,
                    "motivo_exclusion": f"Pasa de {old_stage} a {new_stage} sin contacto previo en flow",
                })
                progress.progress(i / total if total else 1)
                continue

        initial_owner = extract_initial_owner_from_flow(flow_json)

        segments = build_assignment_segments(
            deal_id=deal_id,
            deal_created=deal_created,
            owner_changes=owner_changes,
            reopen_events=reopen_events,
            initial_owner=initial_owner
        )

        if not segments.empty:
            seg_dbg = segments.copy()
            seg_dbg["deal_id"] = deal_id
            debug_segments.append(seg_dbg)

        if not flow_activities.empty:
            acts_dbg = flow_activities.copy()
            if not segments.empty:
                acts_dbg = assign_owner_to_flow_activities(acts_dbg, segments)
            acts_dbg["deal_id"] = deal_id
            debug_activities.append(acts_dbg)

        if segments.empty:
            progress.progress(i / total if total else 1)
            continue

        flow_activities = assign_owner_to_flow_activities(flow_activities, segments)

        for seg_idx, seg in segments.iterrows():
            segment_start = seg["segment_start"]
            segment_end = seg["segment_end"]
            agent_owner = seg["agent_owner"]
            from_owner = seg["from_owner"]
            to_owner = seg["to_owner"]

            segment_start_adjusted = move_to_next_work_moment(segment_start)

            candidate = flow_activities[
                (flow_activities["real_owner"] == agent_owner) &
                (flow_activities["activity_time"] >= segment_start_adjusted)
            ].copy()

            if pd.notna(segment_end):
                candidate = candidate[candidate["activity_time"] < segment_end].copy()

            candidate = candidate.sort_values(["activity_time", "activity_subject"])

            if candidate.empty:
                rows.append({
                    "deal_id": deal_id,
                    "segment_index": seg_idx + 1,
                    "segment_source": seg["segment_source"],
                    "from_owner": from_owner,
                    "to_owner": to_owner,
                    "agent_owner": agent_owner,
                    "deal_created": deal_created,
                    "segment_start": segment_start,
                    "segment_start_adjusted": segment_start_adjusted,
                    "segment_end": segment_end,
                    "first_contact_time": pd.NaT,
                    "first_contact_subject": "",
                    "delta_sec": float("nan"),
                    "has_contact": False,
                    "flow_error": "",
                })
                continue

            first_contact = candidate.iloc[0]
            first_contact_time = first_contact["activity_time"]
            first_contact_subject = first_contact["activity_subject"]
            delta_sec = business_seconds_between(segment_start_adjusted, first_contact_time)

            rows.append({
                "deal_id": deal_id,
                "segment_index": seg_idx + 1,
                "segment_source": seg["segment_source"],
                "from_owner": from_owner,
                "to_owner": to_owner,
                "agent_owner": agent_owner,
                "deal_created": deal_created,
                "segment_start": segment_start,
                "segment_start_adjusted": segment_start_adjusted,
                "segment_end": segment_end,
                "first_contact_time": first_contact_time,
                "first_contact_subject": first_contact_subject,
                "delta_sec": delta_sec,
                "has_contact": True,
                "flow_error": "",
            })

        progress.progress(i / total if total else 1)

    res = pd.DataFrame(rows)

    if res.empty:
        return (
            pd.DataFrame(),
            pd.DataFrame(),
            "",
            "",
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(excluded_stage_without_contact),
            labels,
        )

    if apply_filter_1day:
        res = res[(res["delta_sec"].isna()) | (res["delta_sec"] < ONE_DAY_SECONDS)].copy()

    if selected_mode == "Primera llamada saliente":
        res["tiempo_hasta_primera_llamada"] = res["delta_sec"].apply(format_duration_exact)
    else:
        res["tiempo_hasta_primer_contacto"] = res["delta_sec"].apply(format_duration_exact)

    res = res.sort_values(["deal_id", "segment_start"]).reset_index(drop=True)

    res_with_contact = res[res["has_contact"] == True].copy()

    if not res_with_contact.empty:
        agent_stats = (
            res_with_contact.groupby("agent_owner", dropna=False)
            .agg(
                asignaciones_con_contacto=("deal_id", "count"),
                media_seg=("delta_sec", "mean"),
                mediana_seg=("delta_sec", "median"),
            )
            .reset_index()
        )
        agent_stats["media"] = agent_stats["media_seg"].apply(format_duration_exact)
        agent_stats["mediana"] = agent_stats["mediana_seg"].apply(format_duration_exact)
        agent_stats = agent_stats.sort_values("media_seg", na_position="last")

        media_total = format_duration_exact(res_with_contact["delta_sec"].mean())
        mediana_total = format_duration_exact(res_with_contact["delta_sec"].median())
    else:
        agent_stats = pd.DataFrame()
        media_total = ""
        mediana_total = ""

    debug_segments_df = pd.concat(debug_segments, ignore_index=True) if debug_segments else pd.DataFrame()
    debug_activities_df = pd.concat(debug_activities, ignore_index=True) if debug_activities else pd.DataFrame()
    excluded_df = pd.DataFrame(excluded_stage_without_contact)

    return (
        res,
        agent_stats,
        media_total,
        mediana_total,
        debug_segments_df,
        debug_activities_df,
        excluded_df,
        labels,
    )


def to_excel_bytes(
    res: pd.DataFrame,
    agent_stats: pd.DataFrame,
    debug_segments: pd.DataFrame,
    debug_activities: pd.DataFrame,
    excluded_df: pd.DataFrame
) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        res.to_excel(writer, index=False, sheet_name="por_asignacion")
        if len(agent_stats) > 0:
            agent_stats.to_excel(writer, index=False, sheet_name="resumen_por_agente")
        if len(debug_segments) > 0:
            debug_segments.to_excel(writer, index=False, sheet_name="debug_segmentos")
        if len(debug_activities) > 0:
            debug_activities.to_excel(writer, index=False, sheet_name="debug_actividades_flow")
        if len(excluded_df) > 0:
            excluded_df.to_excel(writer, index=False, sheet_name="excluidos_sin_contacto")
    return output.getvalue()


if uploaded:
    try:
        df = pd.read_excel(uploaded)
    except Exception as e:
        st.error(f"No he podido leer el Excel: {e}")
        st.stop()

    if COL_DEAL_ID not in df.columns:
        st.error(f"Falta la columna necesaria: {COL_DEAL_ID}")
        st.write("Columnas detectadas:", list(df.columns))
        st.stop()

    if not api_token or not company_domain:
        st.warning("Necesitas API token y subdominio para reconstruir el timeline real desde flow.")
        st.stop()

    (
        res,
        agent_stats,
        media_total,
        mediana_total,
        debug_segments,
        debug_activities,
        excluded_df,
        labels,
    ) = compute_from_flow(
        df,
        apply_filter_1day,
        contact_mode
    )

    res_to_show = res.copy()
    if hide_segments_without_contact and len(res_to_show) > 0:
        res_to_show = res_to_show[res_to_show["has_contact"] == True].copy()

    col1, col2, col3 = st.columns(3)
    col1.metric(labels["metric_count"], f"{len(res[res['has_contact'] == True]):,}".replace(",", "."))
    col2.metric("Media total", media_total)
    col3.metric("Mediana total", mediana_total)

    st.subheader(labels["title"])
    st.dataframe(res_to_show, use_container_width=True)

    if len(agent_stats) > 0:
        st.subheader("👤 Resumen por agente")
        st.dataframe(
            agent_stats[["agent_owner", "asignaciones_con_contacto", "media", "mediana"]],
            use_container_width=True
        )

    if len(excluded_df) > 0:
        st.subheader("⚠️ Deals excluidos: salen de Lead sin contacto previo")
        st.dataframe(excluded_df, use_container_width=True)

    with st.expander("🔎 Debug segmentos reconstruidos desde flow"):
        if len(debug_segments) > 0:
            st.dataframe(
                debug_segments.sort_values(["deal_id", "segment_start"]),
                use_container_width=True
            )
        else:
            st.info("No hay segmentos para mostrar.")

    with st.expander("🔎 Debug actividades leídas del flow"):
        if len(debug_activities) > 0:
            st.dataframe(
                debug_activities.sort_values(["deal_id", "activity_time"]),
                use_container_width=True
            )
        else:
            st.info("No hay actividades del flow para mostrar.")

    xlsx_bytes = to_excel_bytes(
        res,
        agent_stats,
        debug_segments,
        debug_activities,
        excluded_df
    )
    st.download_button(
        "⬇️ Descargar Excel con resultados",
        data=xlsx_bytes,
        file_name=labels["download_name"],
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
else:
    st.info("Sube un Excel con al menos la columna 'Negocio - ID'.")
