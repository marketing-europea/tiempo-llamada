import io
import requests
import pandas as pd
import streamlit as st
from datetime import time

st.set_page_config(page_title="Primera llamada por lead (Pipedrive)", layout="wide")

st.title("📞 Tiempo hasta la primera llamada saliente por lead")
st.write(
    "Sube un Excel exportado de Pipedrive (Actividades). "
    "La app calcula, para cada negocio, la PRIMERA actividad cuyo asunto contiene "
    "'Llamada saliente', y mide el tiempo hasta esa primera llamada. "
    "Si el tiempo base supera 30 minutos, consulta el flow del deal y solo usa la reasignación "
    "si el primer evento relevante tras la creación es un cambio de propietario al agente que llama."
)

uploaded = st.file_uploader("Sube tu Excel (.xlsx)", type=["xlsx"])
apply_filter_1day = st.checkbox("Excluir primeras llamadas con 1 día o más de diferencia", value=False)

st.subheader("🔐 Configuración API Pipedrive")
api_token = st.text_input("API token", type="password")
company_domain = st.text_input("Subdominio de Pipedrive", placeholder="tuempresa")

COL_DEAL_ID = "Negocio - ID"
COL_CREATED = "Negocio - Negocio creado el"
COL_DUE_DATE = "Actividad - Fecha de vencimiento"
COL_SUBJECT = "Actividad - Asunto"
COL_OWNER = "Negocio - Propietario"

ONE_DAY_SECONDS = 86400
FLOW_THRESHOLD_SECONDS = 30 * 60  # 30 minutos

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


def normalize_name(value) -> str:
    if pd.isna(value) or value is None:
        return ""
    return " ".join(str(value).strip().lower().split())


# Horarios por agente
AGENT_SCHEDULES = {
    # Solvo
    "jose luis vicuña": {
        0: [(time(12, 30), time(20, 0))],
        1: [(time(12, 30), time(20, 0))],
        2: [(time(12, 30), time(20, 0))],
        3: [(time(12, 30), time(20, 0))],
        4: [(time(12, 30), time(20, 0))],
        5: [(time(12, 30), time(20, 0))],
    },
    "solvo": {
        0: [(time(12, 30), time(20, 0))],
        1: [(time(12, 30), time(20, 0))],
        2: [(time(12, 30), time(20, 0))],
        3: [(time(12, 30), time(20, 0))],
        4: [(time(12, 30), time(20, 0))],
        5: [(time(12, 30), time(20, 0))],
    },

    # Toñi
    "toñi": {
        0: [(time(9, 0), time(14, 0))],
        1: [(time(9, 0), time(14, 0))],
        2: [(time(9, 0), time(14, 0))],
        3: [(time(9, 0), time(14, 0))],
        4: [(time(9, 0), time(14, 0))],
    },

    # Meri
    "meri": {
        0: [(time(9, 30), time(13, 0)), (time(16, 0), time(20, 30))],
        1: [(time(9, 30), time(13, 0)), (time(16, 0), time(20, 30))],
        2: [(time(9, 30), time(13, 0)), (time(16, 0), time(20, 30))],
        3: [(time(9, 30), time(13, 0)), (time(16, 0), time(20, 30))],
        4: [(time(9, 30), time(15, 0))],
    },

    # Isabel, Carolina y Jesús
    "isabel": {
        0: [(time(9, 0), time(14, 30)), (time(16, 0), time(18, 30))],
        1: [(time(9, 0), time(14, 30)), (time(16, 0), time(18, 30))],
        2: [(time(9, 0), time(14, 30)), (time(16, 0), time(18, 30))],
        3: [(time(9, 0), time(14, 30)), (time(16, 0), time(18, 30))],
        4: [(time(9, 0), time(14, 30))],
    },
    "isabel tortosa vivas": {
        0: [(time(9, 0), time(14, 30)), (time(16, 0), time(18, 30))],
        1: [(time(9, 0), time(14, 30)), (time(16, 0), time(18, 30))],
        2: [(time(9, 0), time(14, 30)), (time(16, 0), time(18, 30))],
        3: [(time(9, 0), time(14, 30)), (time(16, 0), time(18, 30))],
        4: [(time(9, 0), time(14, 30))],
    },
    "carolina": {
        0: [(time(9, 0), time(14, 30)), (time(16, 0), time(18, 30))],
        1: [(time(9, 0), time(14, 30)), (time(16, 0), time(18, 30))],
        2: [(time(9, 0), time(14, 30)), (time(16, 0), time(18, 30))],
        3: [(time(9, 0), time(14, 30)), (time(16, 0), time(18, 30))],
        4: [(time(9, 0), time(14, 30))],
    },
    "jesús": {
        0: [(time(9, 0), time(14, 30)), (time(16, 0), time(18, 30))],
        1: [(time(9, 0), time(14, 30)), (time(16, 0), time(18, 30))],
        2: [(time(9, 0), time(14, 30)), (time(16, 0), time(18, 30))],
        3: [(time(9, 0), time(14, 30)), (time(16, 0), time(18, 30))],
        4: [(time(9, 0), time(14, 30))],
    },
}

DEFAULT_SCHEDULE = {
    0: [(time(9, 0), time(18, 0))],
    1: [(time(9, 0), time(18, 0))],
    2: [(time(9, 0), time(18, 0))],
    3: [(time(9, 0), time(18, 0))],
    4: [(time(9, 0), time(18, 0))],
}


def get_schedule_for_agent(agent_name: str):
    norm = normalize_name(agent_name)

    if norm in AGENT_SCHEDULES:
        return AGENT_SCHEDULES[norm]

    if "isabel" in norm:
        return AGENT_SCHEDULES["isabel"]
    if "carolina" in norm:
        return AGENT_SCHEDULES["carolina"]
    if "jesús" in norm or "jesus" in norm:
        return AGENT_SCHEDULES["jesús"]
    if "toñi" in norm or "toni" in norm:
        return AGENT_SCHEDULES["toñi"]
    if "meri" in norm:
        return AGENT_SCHEDULES["meri"]
    if "jose luis vicuña" in norm or "solvo" in norm:
        return AGENT_SCHEDULES["jose luis vicuña"]

    return DEFAULT_SCHEDULE


def is_holiday(ts: pd.Timestamp) -> bool:
    return ts.date() in HOLIDAYS_2026


def get_day_windows(ts: pd.Timestamp, agent_name: str):
    if is_holiday(ts):
        return []

    weekday = ts.weekday()
    schedule = get_schedule_for_agent(agent_name)
    windows = schedule.get(weekday, [])

    return [
        (
            pd.Timestamp.combine(ts.date(), start_t),
            pd.Timestamp.combine(ts.date(), end_t),
        )
        for start_t, end_t in windows
    ]


def move_to_next_work_moment(ts: pd.Timestamp, agent_name: str) -> pd.Timestamp:
    cur = ts

    for _ in range(370):
        windows = get_day_windows(cur, agent_name)

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


def adjust_creation_time_for_agent(ts: pd.Timestamp, agent_name: str) -> pd.Timestamp:
    if pd.isna(ts):
        return ts
    return move_to_next_work_moment(ts, agent_name)


def business_seconds_between(start_ts: pd.Timestamp, end_ts: pd.Timestamp, agent_name: str) -> float:
    if pd.isna(start_ts) or pd.isna(end_ts):
        return float("nan")
    if end_ts < start_ts:
        return float("nan")

    cur = move_to_next_work_moment(start_ts, agent_name)
    total_seconds = 0.0

    for _ in range(370):
        if cur >= end_ts:
            break

        windows = get_day_windows(cur, agent_name)
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


def extract_relevant_flow_events(flow_json: dict, call_time: pd.Timestamp) -> pd.DataFrame:
    rows = []

    for item in flow_json.get("data", []) or []:
        obj = item.get("object")
        data = item.get("data", {}) or {}

        event_time = pd.NaT
        event_type = None
        owner_to = None

        if obj == "dealChange" and data.get("field_key") == "user_id":
            event_time = pd.to_datetime(data.get("log_time"), errors="coerce")
            event_type = "owner_change"
            owner_to = normalize_name((data.get("additional_data") or {}).get("new_value_formatted"))

        elif obj == "activity":
            due_date = data.get("due_date")
            marked_done = data.get("marked_as_done_time")
            add_time = data.get("add_time")

            event_time = pd.to_datetime(due_date or marked_done or add_time, errors="coerce")
            event_type = "activity"

        if pd.notna(event_time) and event_time <= call_time and event_type is not None:
            rows.append({
                "event_time": event_time,
                "event_type": event_type,
                "owner_to": owner_to,
            })

    if not rows:
        return pd.DataFrame(columns=["event_time", "event_type", "owner_to"])

    events = pd.DataFrame(rows).sort_values("event_time").reset_index(drop=True)
    return events


def get_start_time_real_from_flow(flow_json: dict, created_adjusted: pd.Timestamp, call_owner: str, call_time: pd.Timestamp):
    """
    Solo reasigna si el primer evento relevante tras la creación ajustada
    es un cambio de propietario al agente que hace la llamada.
    Si el primer evento relevante es una actividad, mantiene created_adjusted.
    """
    call_owner_norm = normalize_name(call_owner)
    events = extract_relevant_flow_events(flow_json, call_time)

    if len(events) == 0:
        return created_adjusted, pd.NaT, "created_adjusted"

    events = events[events["event_time"] >= created_adjusted].copy()
    if len(events) == 0:
        return created_adjusted, pd.NaT, "created_adjusted"

    first_event = events.iloc[0]

    if first_event["event_type"] == "owner_change" and first_event["owner_to"] == call_owner_norm:
        return first_event["event_time"], first_event["event_time"], "owner_reassignment_immediate"

    return created_adjusted, pd.NaT, "created_adjusted"


def compute_first_outbound_call(df: pd.DataFrame, apply_filter_1day: bool):
    df = df.copy()

    df[COL_DEAL_ID] = pd.to_numeric(df[COL_DEAL_ID], errors="coerce").astype("Int64")
    df[COL_CREATED] = pd.to_datetime(df[COL_CREATED], errors="coerce")
    df[COL_DUE_DATE] = pd.to_datetime(df[COL_DUE_DATE], errors="coerce")
    df[COL_SUBJECT] = df[COL_SUBJECT].astype(str).str.strip()

    df = df.dropna(subset=[COL_DEAL_ID, COL_CREATED, COL_DUE_DATE, COL_SUBJECT]).copy()
    if contact_mode == "Primera llamada saliente":
    df = df[df[COL_SUBJECT].str.contains("llamada saliente", case=False, na=False)].copy()
    else:
    df = df[
        df[COL_SUBJECT].str.contains("llamada saliente|whatsapp chat", case=False, na=False)
    ].copy()

    # El agente que llama
    df["call_owner"] = df[COL_OWNER]

    # Ajuste inicial según horario del agente
    df["created_adjusted"] = df.apply(
        lambda row: adjust_creation_time_for_agent(row[COL_CREATED], row["call_owner"]),
        axis=1
    )

    # Tiempo base en horas hábiles del agente
    df["delta_sec_base"] = df.apply(
        lambda row: business_seconds_between(
            row["created_adjusted"],
            row[COL_DUE_DATE],
            row["call_owner"]
        ),
        axis=1
    )

    df = df[df["delta_sec_base"] >= 0].copy()

    # Orden cronológico por lead
    df = df.sort_values([COL_DEAL_ID, COL_DUE_DATE, COL_SUBJECT]).copy()

    # Primera llamada por lead único
    first_calls = df.drop_duplicates(subset=[COL_DEAL_ID], keep="first").copy()

    first_calls = first_calls.rename(columns={
        COL_DUE_DATE: "first_call_time",
        COL_SUBJECT: "first_call_subject",
    })

    real_start_times = []
    start_sources = []
    reassignment_times = []
    flow_checked = []

    progress = st.progress(0)
    total_rows = len(first_calls)

    for i, (_, row) in enumerate(first_calls.iterrows(), start=1):
        created_adjusted = row["created_adjusted"]
        first_call_time = row["first_call_time"]
        delta_sec_base = row["delta_sec_base"]
        deal_id = int(row[COL_DEAL_ID])
        call_owner = row["call_owner"]

        start_time_real = created_adjusted
        start_source = "created_adjusted"
        reassignment_time = pd.NaT
        checked_flow = False

        if pd.notna(delta_sec_base) and delta_sec_base > FLOW_THRESHOLD_SECONDS and api_token and company_domain:
            checked_flow = True
            try:
                flow_json = fetch_deal_flow(api_token, company_domain, deal_id)
                start_time_real, reassignment_time, start_source = get_start_time_real_from_flow(
                    flow_json=flow_json,
                    created_adjusted=created_adjusted,
                    call_owner=call_owner,
                    call_time=first_call_time
                )
            except Exception:
                pass

        real_start_times.append(start_time_real)
        start_sources.append(start_source)
        reassignment_times.append(reassignment_time)
        flow_checked.append(checked_flow)

        progress.progress(i / total_rows if total_rows else 1)

    first_calls["reassignment_time"] = reassignment_times
    first_calls["start_time_real"] = real_start_times
    first_calls["start_source"] = start_sources
    first_calls["flow_checked"] = flow_checked

    # Tiempo final en horas hábiles del agente
    first_calls["delta_sec"] = first_calls.apply(
        lambda row: business_seconds_between(
            row["start_time_real"],
            row["first_call_time"],
            row["call_owner"]
        ),
        axis=1
    )

    first_calls = first_calls[first_calls["delta_sec"] >= 0].copy()

    if apply_filter_1day:
        first_calls = first_calls[first_calls["delta_sec"] < ONE_DAY_SECONDS].copy()

    keep_cols = [
        COL_DEAL_ID,
        COL_CREATED,
        "created_adjusted",
        "reassignment_time",
        "start_time_real",
        "start_source",
        "first_call_time",
        "first_call_subject",
        "delta_sec_base",
        "delta_sec",
        "flow_checked",
        "call_owner",
    ]

    res = first_calls[keep_cols].copy()
    res["tiempo_base_desde_creacion"] = res["delta_sec_base"].apply(format_duration_exact)
    res["tiempo_hasta_primera_llamada"] = res["delta_sec"].apply(format_duration_exact)
    res = res.sort_values(COL_CREATED).reset_index(drop=True)

    if len(res) > 0:
        agent_stats = (
            res.groupby("call_owner", dropna=False)
            .agg(
                leads_unicos=(COL_DEAL_ID, "count"),
                media_seg=("delta_sec", "mean"),
                mediana_seg=("delta_sec", "median"),
            )
            .reset_index()
        )
        agent_stats["media"] = agent_stats["media_seg"].apply(format_duration_exact)
        agent_stats["mediana"] = agent_stats["mediana_seg"].apply(format_duration_exact)
        agent_stats = agent_stats.sort_values("media_seg", na_position="last")
    else:
        agent_stats = pd.DataFrame()

    media_total = format_duration_exact(res["delta_sec"].mean()) if len(res) > 0 else ""
    mediana_total = format_duration_exact(res["delta_sec"].median()) if len(res) > 0 else ""

    return res, agent_stats, media_total, mediana_total, df


def to_excel_bytes(res: pd.DataFrame, agent_stats: pd.DataFrame, debug_calls: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        res.to_excel(writer, index=False, sheet_name="primera_llamada_por_lead")
        if len(agent_stats) > 0:
            agent_stats.to_excel(writer, index=False, sheet_name="resumen_por_agente")
        debug_calls.to_excel(writer, index=False, sheet_name="debug_llamadas_filtradas")
    return output.getvalue()


if uploaded:
    try:
        df = pd.read_excel(uploaded)
    except Exception as e:
        st.error(f"No he podido leer el Excel: {e}")
        st.stop()

    required_cols = [COL_DEAL_ID, COL_CREATED, COL_DUE_DATE, COL_SUBJECT, COL_OWNER]
    missing = [c for c in required_cols if c not in df.columns]

    if missing:
        st.error("Faltan columnas necesarias: " + ", ".join(missing))
        st.write("Columnas detectadas:", list(df.columns))
        st.stop()

    res, agent_stats, media_total, mediana_total, debug_calls = compute_first_outbound_call(df, apply_filter_1day)

    col1, col2, col3 = st.columns(3)
    col1.metric("Leads únicos con 1ª llamada", f"{len(res):,}".replace(",", "."))
    col2.metric("Media total", media_total)
    col3.metric("Mediana total", mediana_total)

    st.subheader("✅ Primera llamada saliente por lead único")
    st.dataframe(res, use_container_width=True)

    if len(agent_stats) > 0:
        st.subheader("👤 Resumen por agente (sobre leads únicos)")
        st.dataframe(
            agent_stats[["call_owner", "leads_unicos", "media", "mediana"]],
            use_container_width=True
        )

    with st.expander("🔎 Debug: llamadas salientes filtradas y ordenadas"):
        debug_cols = [
            COL_DEAL_ID,
            COL_CREATED,
            "call_owner",
            "created_adjusted",
            COL_DUE_DATE,
            COL_SUBJECT,
            "delta_sec_base",
        ]
        st.dataframe(debug_calls[debug_cols], use_container_width=True)

    xlsx_bytes = to_excel_bytes(res, agent_stats, debug_calls)
    st.download_button(
        "⬇️ Descargar Excel con resultados",
        data=xlsx_bytes,
        file_name="primera_llamada_saliente_por_lead_unico.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
else:
    st.info("Sube un Excel para calcular la primera llamada saliente por lead único.")
