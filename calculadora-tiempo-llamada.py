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
    "Sube un Excel solo para obtener los negocios a analizar. "
    "La app usa el flow API de Pipedrive como fuente de verdad para reconstruir: "
    "creación del negocio, cambios de propietario y actividades, "
    "y calcula la primera llamada/contacto tras cada asignación."
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
            "title": "✅ Primera llamada saliente por asignación",
            "metric_count": "Asignaciones con 1ª llamada",
            "time_col": "tiempo_hasta_primera_llamada",
            "download_name": "primera_llamada_por_asignacion_flow.xlsx",
        }
    return {
        "title": "✅ Primer contacto por asignación",
        "metric_count": "Asignaciones con 1er contacto",
        "time_col": "tiempo_hasta_primer_contacto",
        "download_name": "primer_contacto_por_asignacion_flow.xlsx",
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


def extract_flow_activities(flow_json: dict, selected_mode: str) -> pd.DataFrame:
    rows = []
    pattern = get_contact_pattern(selected_mode)

    for item in flow_json.get("data", []) or []:
        obj = item.get("object")
        data = item.get("data", {}) or {}

        if obj != "activity":
            continue

        subject = clean_text(data.get("subject"))
        if not subject:
            continue

        if not pd.Series([subject]).str.contains(pattern, case=False, na=False).iloc[0]:
            continue

        # prioridad temporal
        event_time = (
            to_madrid_ts(data.get("due_date"))
            or to_madrid_ts(data.get("marked_as_done_time"))
            or to_madrid_ts(data.get("add_time"))
            or to_madrid_ts(data.get("update_time"))
        )

        if pd.isna(event_time):
            continue

        rows.append({
            "activity_time": event_time,
            "activity_subject": subject,
            "activity_id": data.get("id"),
            "activity_type": clean_text(data.get("type")),
            "activity_done": data.get("done"),
        })

    if not rows:
        return pd.DataFrame(columns=[
            "activity_time", "activity_subject", "activity_id", "activity_type", "activity_done"
        ])

    return pd.DataFrame(rows).sort_values("activity_time").reset_index(drop=True)


def build_assignment_segments(
    deal_id: int,
    deal_created: pd.Timestamp,
    owner_changes: pd.DataFrame
) -> pd.DataFrame:
    """
    Creamos tramos SOLO desde cambios de propietario.
    En tu caso:
    Central -> TOÑI  => tramo TOÑI empieza aquí
    TOÑI -> Mayra    => tramo Mayra empieza aquí
    """
    rows = []

    for _, ch in owner_changes.iterrows():
        rows.append({
            "deal_id": deal_id,
            "segment_start": ch["event_time"],
            "segment_source": "owner_reassignment",
            "from_owner": ch["old_owner"],
            "to_owner": ch["new_owner"],
            "agent_owner": ch["new_owner"],
        })

    if not rows:
        return pd.DataFrame(columns=[
            "deal_id", "segment_start", "segment_source",
            "from_owner", "to_owner", "agent_owner", "segment_end"
        ])

    seg = pd.DataFrame(rows).sort_values("segment_start").reset_index(drop=True)

    # quitar duplicados consecutivos del mismo owner
    seg["prev_owner"] = seg["agent_owner"].shift(1)
    seg = seg[(seg.index == 0) | (seg["agent_owner"] != seg["prev_owner"])].copy()
    seg = seg.drop(columns=["prev_owner"]).reset_index(drop=True)

    seg["segment_end"] = seg["segment_start"].shift(-1)
    seg["deal_created"] = deal_created

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
        deal_created_map = (
            tmp.groupby(COL_DEAL_ID)[COL_CREATED].min().to_dict()
        )

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
        flow_activities = extract_flow_activities(flow_json, selected_mode)

        segments = build_assignment_segments(
            deal_id=deal_id,
            deal_created=deal_created,
            owner_changes=owner_changes
        )

        if not segments.empty:
            segments_dbg = segments.copy()
            debug_segments.append(segments_dbg)

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

    debug_segments_df = (
        pd.concat(debug_segments, ignore_index=True) if debug_segments else pd.DataFrame()
    )
    debug_activities_df = (
        pd.concat(debug_activities, ignore_index=True) if debug_activities else pd.DataFrame()
    )

    return res, agent_stats, media_total, mediana_total, debug_segments_df, debug_activities_df, labels


def to_excel_bytes(
    res: pd.DataFrame,
    agent_stats: pd.DataFrame,
    debug_segments: pd.DataFrame,
    debug_activities: pd.DataFrame
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

    res, agent_stats, media_total, mediana_total, debug_segments, debug_activities, labels = compute_from_flow(
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

    with st.expander("🔎 Debug segmentos reconstruidos desde flow"):
        if len(debug_segments) > 0:
            st.dataframe(debug_segments.sort_values(["deal_id", "segment_start"]), use_container_width=True)
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

    xlsx_bytes = to_excel_bytes(res, agent_stats, debug_segments, debug_activities)
    st.download_button(
        "⬇️ Descargar Excel con resultados",
        data=xlsx_bytes,
        file_name=labels["download_name"],
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
else:
    st.info("Sube un Excel con al menos la columna 'Negocio - ID'.")
