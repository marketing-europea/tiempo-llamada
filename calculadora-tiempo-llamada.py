import io
import requests
import pandas as pd
import streamlit as st
from datetime import time

st.set_page_config(page_title="Tiempo hasta el primer contacto por lead (Pipedrive)", layout="wide")

st.title("📞 Tiempo hasta el primer contacto por lead")
st.write(
    "Sube un Excel exportado de Pipedrive (Actividades). "
    "La app calcula, para cada negocio, la PRIMERA actividad de contacto y mide el tiempo hasta ese primer contacto. "
    "Puedes elegir entre medir solo la primera llamada saliente o el primer contacto (llamada + WhatsApp). "
    "Solo se incluyen leads que tengan al menos un registro de llamada. "
    "Si el tiempo base supera 30 minutos, consulta el flow del deal y solo usa la reasignación "
    "si el primer evento relevante tras la creación es un cambio de propietario al agente que hace ese primer contacto."
)

uploaded = st.file_uploader("Sube tu Excel (.xlsx)", type=["xlsx"])
apply_filter_1day = st.checkbox("Excluir primeros contactos con 1 día o más de diferencia", value=False)

contact_mode = st.radio(
    "Qué quieres medir",
    ["Primera llamada saliente", "Primer contacto (llamada + WhatsApp)"],
    horizontal=True,
)

st.subheader("🔐 Configuración API Pipedrive")
api_token = st.text_input("API token", type="password")
company_domain = st.text_input("Subdominio de Pipedrive", placeholder="tuempresa")

COL_DEAL_ID = "Negocio - ID"
COL_CREATED = "Negocio - Negocio creado el"
COL_DUE_DATE = "Actividad - Fecha de vencimiento"
COL_SUBJECT = "Actividad - Asunto"
COL_OWNER = "Negocio - Propietario"
COL_ACTIVITY_OWNER = "Actividad - Asignada al usuario"

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


# Horario general del equipo
# Lunes a viernes: 09:00 - 20:00
# Sábados: 12:30 - 20:00
# Domingos: sin servicio
TEAM_SCHEDULE = {
    0: [(time(9, 0), time(20, 0))],    # lunes
    1: [(time(9, 0), time(20, 0))],    # martes
    2: [(time(9, 0), time(20, 0))],    # miércoles
    3: [(time(9, 0), time(20, 0))],    # jueves
    4: [(time(9, 0), time(20, 0))],    # viernes
    5: [(time(12, 30), time(20, 0))],  # sábado
}


def clean_text(value) -> str:
    if pd.isna(value) or value is None:
        return ""
    return str(value).strip()


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


def adjust_creation_time_for_agent(ts: pd.Timestamp, agent_name: str = "") -> pd.Timestamp:
    if pd.isna(ts):
        return ts
    return move_to_next_work_moment(ts)


def business_seconds_between(start_ts: pd.Timestamp, end_ts: pd.Timestamp, agent_name: str = "") -> float:
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


def extract_relevant_flow_events(flow_json: dict, contact_time: pd.Timestamp) -> pd.DataFrame:
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

        if pd.notna(event_time) and event_time <= contact_time and event_type is not None:
            rows.append({
                "event_time": event_time,
                "event_type": event_type,
                "owner_to": owner_to,
            })

    if not rows:
        return pd.DataFrame(columns=["event_time", "event_type", "owner_to"])

    events = pd.DataFrame(rows).sort_values("event_time").reset_index(drop=True)
    return events


def get_start_time_real_from_flow(
    flow_json: dict,
    created_adjusted: pd.Timestamp,
    contact_owner: str,
    contact_time: pd.Timestamp
):
    """
    Solo reasigna si el primer evento relevante tras la creación ajustada
    es un cambio de propietario al agente que hace el primer contacto.
    Si el primer evento relevante es una actividad, mantiene created_adjusted.
    """
    contact_owner_norm = normalize_name(contact_owner)
    events = extract_relevant_flow_events(flow_json, contact_time)

    if len(events) == 0:
        return created_adjusted, pd.NaT, "created_adjusted"

    events = events[events["event_time"] >= created_adjusted].copy()
    if len(events) == 0:
        return created_adjusted, pd.NaT, "created_adjusted"

    first_event = events.iloc[0]

    if first_event["event_type"] == "owner_change" and first_event["owner_to"] == contact_owner_norm:
        return first_event["event_time"], first_event["event_time"], "owner_reassignment_immediate"

    return created_adjusted, pd.NaT, "created_adjusted"


def get_activity_filter_pattern(selected_mode: str) -> str:
    if selected_mode == "Primera llamada saliente":
        return r"llamada saliente"
    return r"llamada saliente|whatsapp chat"


def get_result_labels(selected_mode: str):
    if selected_mode == "Primera llamada saliente":
        return {
            "title": "✅ Primera llamada saliente por lead único",
            "metric_count": "Leads únicos con 1ª llamada",
            "time_col": "tiempo_hasta_primera_llamada",
            "base_col": "tiempo_base_desde_creacion",
            "subject_col": "first_contact_subject",
            "time_display": "Media total",
            "median_display": "Mediana total",
            "download_name": "primera_llamada_saliente_por_lead_unico.xlsx",
        }
    return {
        "title": "✅ Primer contacto por lead único",
        "metric_count": "Leads únicos con 1er contacto",
        "time_col": "tiempo_hasta_primer_contacto",
        "base_col": "tiempo_base_desde_creacion",
        "subject_col": "first_contact_subject",
        "time_display": "Media total",
        "median_display": "Mediana total",
        "download_name": "primer_contacto_por_lead_unico.xlsx",
    }


def compute_first_contact(df: pd.DataFrame, apply_filter_1day: bool, selected_mode: str):
    df = df.copy()

    df[COL_DEAL_ID] = pd.to_numeric(df[COL_DEAL_ID], errors="coerce").astype("Int64")
    df[COL_CREATED] = pd.to_datetime(df[COL_CREATED], errors="coerce")
    df[COL_DUE_DATE] = pd.to_datetime(df[COL_DUE_DATE], errors="coerce")
    df[COL_SUBJECT] = df[COL_SUBJECT].astype(str).str.strip()
    df[COL_OWNER] = df[COL_OWNER].apply(clean_text)
    df[COL_ACTIVITY_OWNER] = df[COL_ACTIVITY_OWNER].apply(clean_text)

    df = df.dropna(subset=[COL_DEAL_ID, COL_CREATED, COL_DUE_DATE, COL_SUBJECT]).copy()

    # Excluir leads que no tengan ninguna llamada en absoluto
    df["has_any_call"] = df[COL_SUBJECT].str.contains(
        r"llamada saliente|llamada entrante",
        case=False,
        na=False
    )
    deals_with_call = df.loc[df["has_any_call"], COL_DEAL_ID].dropna().unique()
    df = df[df[COL_DEAL_ID].isin(deals_with_call)].copy()

    # Filtrar solo las actividades válidas para el modo elegido
    pattern = get_activity_filter_pattern(selected_mode)
    df = df[df[COL_SUBJECT].str.contains(pattern, case=False, na=False)].copy()

    # Owner del deal vs owner real de la actividad
    df["deal_owner"] = df[COL_OWNER]
    df["contact_owner"] = df[COL_ACTIVITY_OWNER]

    # Si alguna actividad viniera sin usuario asignado, evitamos vacíos visibles
    df["contact_owner"] = df["contact_owner"].replace("", "Sin asignar")

    df["created_adjusted"] = df.apply(
        lambda row: adjust_creation_time_for_agent(row[COL_CREATED], row["contact_owner"]),
        axis=1
    )

    df["delta_sec_base"] = df.apply(
        lambda row: business_seconds_between(
            row["created_adjusted"],
            row[COL_DUE_DATE],
            row["contact_owner"]
        ),
        axis=1
    )

    df = df[df["delta_sec_base"] >= 0].copy()
    df = df.sort_values([COL_DEAL_ID, COL_DUE_DATE, COL_SUBJECT]).copy()

    # Primera actividad de contacto por lead
    first_contacts = df.drop_duplicates(subset=[COL_DEAL_ID], keep="first").copy()

    first_contacts = first_contacts.rename(columns={
        COL_DUE_DATE: "first_contact_time",
        COL_SUBJECT: "first_contact_subject",
    })

    real_start_times = []
    start_sources = []
    reassignment_times = []
    flow_checked = []

    progress = st.progress(0)
    total_rows = len(first_contacts)

    for i, (_, row) in enumerate(first_contacts.iterrows(), start=1):
        created_adjusted = row["created_adjusted"]
        first_contact_time = row["first_contact_time"]
        delta_sec_base = row["delta_sec_base"]
        deal_id = int(row[COL_DEAL_ID])
        contact_owner = row["contact_owner"]

        start_time_real = created_adjusted
        start_source = "created_adjusted"
        reassignment_time = pd.NaT
        checked_flow = False

        if (
            pd.notna(delta_sec_base)
            and delta_sec_base > FLOW_THRESHOLD_SECONDS
            and api_token
            and company_domain
        ):
            checked_flow = True
            try:
                flow_json = fetch_deal_flow(api_token, company_domain, deal_id)
                start_time_real, reassignment_time, start_source = get_start_time_real_from_flow(
                    flow_json=flow_json,
                    created_adjusted=created_adjusted,
                    contact_owner=contact_owner,
                    contact_time=first_contact_time
                )
            except Exception:
                pass

        real_start_times.append(start_time_real)
        start_sources.append(start_source)
        reassignment_times.append(reassignment_time)
        flow_checked.append(checked_flow)

        progress.progress(i / total_rows if total_rows else 1)

    first_contacts["reassignment_time"] = reassignment_times
    first_contacts["start_time_real"] = real_start_times
    first_contacts["start_source"] = start_sources
    first_contacts["flow_checked"] = flow_checked

    first_contacts["delta_sec"] = first_contacts.apply(
        lambda row: business_seconds_between(
            row["start_time_real"],
            row["first_contact_time"],
            row["contact_owner"]
        ),
        axis=1
    )

    first_contacts = first_contacts[first_contacts["delta_sec"] >= 0].copy()

    if apply_filter_1day:
        first_contacts = first_contacts[first_contacts["delta_sec"] < ONE_DAY_SECONDS].copy()

    keep_cols = [
        COL_DEAL_ID,
        COL_CREATED,
        "deal_owner",
        "contact_owner",
        "created_adjusted",
        "reassignment_time",
        "start_time_real",
        "start_source",
        "first_contact_time",
        "first_contact_subject",
        "delta_sec_base",
        "delta_sec",
        "flow_checked",
    ]

    res = first_contacts[keep_cols].copy()
    res["tiempo_base_desde_creacion"] = res["delta_sec_base"].apply(format_duration_exact)

    if selected_mode == "Primera llamada saliente":
        res["tiempo_hasta_primera_llamada"] = res["delta_sec"].apply(format_duration_exact)
    else:
        res["tiempo_hasta_primer_contacto"] = res["delta_sec"].apply(format_duration_exact)

    res = res.sort_values(COL_CREATED).reset_index(drop=True)

    if len(res) > 0:
        agent_stats = (
            res.groupby("contact_owner", dropna=False)
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
        res.to_excel(writer, index=False, sheet_name="primer_contacto_por_lead")
        if len(agent_stats) > 0:
            agent_stats.to_excel(writer, index=False, sheet_name="resumen_por_agente")
        debug_calls.to_excel(writer, index=False, sheet_name="debug_contactos_filtrados")
    return output.getvalue()


if uploaded:
    try:
        df = pd.read_excel(uploaded)
    except Exception as e:
        st.error(f"No he podido leer el Excel: {e}")
        st.stop()

    required_cols = [
        COL_DEAL_ID,
        COL_CREATED,
        COL_DUE_DATE,
        COL_SUBJECT,
        COL_OWNER,
        COL_ACTIVITY_OWNER,
    ]
    missing = [c for c in required_cols if c not in df.columns]

    if missing:
        st.error("Faltan columnas necesarias: " + ", ".join(missing))
        st.write("Columnas detectadas:", list(df.columns))
        st.stop()

    labels = get_result_labels(contact_mode)
    res, agent_stats, media_total, mediana_total, debug_calls = compute_first_contact(
        df,
        apply_filter_1day,
        contact_mode
    )

    col1, col2, col3 = st.columns(3)
    col1.metric(labels["metric_count"], f"{len(res):,}".replace(",", "."))
    col2.metric(labels["time_display"], media_total)
    col3.metric(labels["median_display"], mediana_total)

    st.subheader(labels["title"])
    st.dataframe(res, use_container_width=True)

    if len(agent_stats) > 0:
        st.subheader("👤 Resumen por agente (sobre leads únicos)")
        st.dataframe(
            agent_stats[["contact_owner", "leads_unicos", "media", "mediana"]],
            use_container_width=True
        )

    with st.expander("🔎 Debug: contactos filtrados y ordenados"):
        debug_cols = [
            COL_DEAL_ID,
            COL_CREATED,
            "deal_owner",
            "contact_owner",
            "created_adjusted",
            COL_DUE_DATE,
            COL_SUBJECT,
            "delta_sec_base",
        ]
        debug_view = debug_calls[debug_cols].sort_values([COL_DEAL_ID, COL_DUE_DATE, COL_SUBJECT])
        st.dataframe(debug_view, use_container_width=True)

    xlsx_bytes = to_excel_bytes(res, agent_stats, debug_calls)
    st.download_button(
        "⬇️ Descargar Excel con resultados",
        data=xlsx_bytes,
        file_name=labels["download_name"],
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
else:
    st.info("Sube un Excel para calcular el primer contacto por lead único.")
