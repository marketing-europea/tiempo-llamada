import io
import pandas as pd
import streamlit as st
from datetime import datetime, time, timedelta

st.set_page_config(page_title="Primera llamada por lead (Pipedrive)", layout="wide")

st.title("📞 Tiempo hasta la primera llamada saliente por lead")
st.write(
    "Sube un Excel exportado de Pipedrive (Actividades). "
    "La app calcula, para cada negocio, la PRIMERA actividad cuyo asunto contiene "
    "'Llamada saliente', y mide el tiempo desde la creación del negocio hasta esa primera llamada. "
    "La media y la mediana se calculan sobre leads únicos."
)

uploaded = st.file_uploader("Sube tu Excel (.xlsx)", type=["xlsx"])

COL_DEAL_ID = "Negocio - ID"
COL_CREATED = "Negocio - Negocio creado el"
COL_DUE_DATE = "Actividad - Fecha de vencimiento"
COL_SUBJECT = "Actividad - Asunto"
COL_OWNER = "Negocio - Propietario"

# Horario call center
WORK_START = time(9, 0, 0)
WORK_END_WEEKDAY = time(23, 59, 59)   # lunes a viernes, cambia esto si quieres otra hora
WORK_END_SATURDAY = time(18, 0, 0)    # sábado hasta las 18:00

# Filtro de outliers
MAX_HOURS = 24
APPLY_MAX_FILTER = True


def format_duration_exact(seconds: float) -> str:
    """Formatea segundos como HH:MM:SS o Xd HH:MM:SS."""
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


def is_working_day(dt: datetime) -> bool:
    # lunes=0 ... sábado=5, domingo=6
    return dt.weekday() <= 5


def get_day_bounds(dt: datetime):
    """Devuelve inicio y fin de jornada para el día de dt."""
    start_dt = datetime.combine(dt.date(), WORK_START)

    if dt.weekday() == 5:  # sábado
        end_dt = datetime.combine(dt.date(), WORK_END_SATURDAY)
    elif dt.weekday() <= 4:  # lunes-viernes
        end_dt = datetime.combine(dt.date(), WORK_END_WEEKDAY)
    else:
        return None, None

    return start_dt, end_dt


def next_work_start(dt: datetime) -> datetime:
    """Mueve un datetime al siguiente momento hábil."""
    cur = dt

    while True:
        if not is_working_day(cur):
            cur = datetime.combine(cur.date() + timedelta(days=1), WORK_START)
            continue

        day_start, day_end = get_day_bounds(cur)

        if cur < day_start:
            return day_start

        if cur > day_end:
            cur = datetime.combine(cur.date() + timedelta(days=1), WORK_START)
            continue

        return cur


def business_seconds_between(start: datetime, end: datetime) -> float:
    """
    Calcula segundos hábiles entre start y end.
    Horario:
      - lunes a viernes: desde WORK_START hasta WORK_END_WEEKDAY
      - sábado: desde WORK_START hasta 18:00
      - domingo: no cuenta
    """
    if pd.isna(start) or pd.isna(end):
        return float("nan")

    if end < start:
        return float("nan")

    current = next_work_start(start)
    end = end.to_pydatetime() if isinstance(end, pd.Timestamp) else end
    current = current.to_pydatetime() if isinstance(current, pd.Timestamp) else current

    total_seconds = 0.0

    while current < end:
        if not is_working_day(current):
            current = next_work_start(current)
            continue

        day_start, day_end = get_day_bounds(current)
        if day_start is None:
            current = next_work_start(current + timedelta(days=1))
            continue

        window_start = max(current, day_start)
        window_end = min(end, day_end)

        if window_end > window_start:
            total_seconds += (window_end - window_start).total_seconds()

        if end <= day_end:
            break

        current = datetime.combine(current.date() + timedelta(days=1), WORK_START)
        current = next_work_start(current)

    return total_seconds


def compute_first_outbound_call(df: pd.DataFrame):
    df = df.copy()

    # Normalizar tipos
    df[COL_DEAL_ID] = pd.to_numeric(df[COL_DEAL_ID], errors="coerce").astype("Int64")
    df[COL_CREATED] = pd.to_datetime(df[COL_CREATED], errors="coerce")
    df[COL_DUE_DATE] = pd.to_datetime(df[COL_DUE_DATE], errors="coerce")
    df[COL_SUBJECT] = df[COL_SUBJECT].astype(str).str.strip()

    # Filas válidas
    df = df.dropna(subset=[COL_DEAL_ID, COL_CREATED, COL_DUE_DATE, COL_SUBJECT]).copy()

    # Solo llamadas salientes
    df = df[df[COL_SUBJECT].str.contains("llamada saliente", case=False, na=False)].copy()

    # Solo posteriores o iguales a creación
    df = df[df[COL_DUE_DATE] >= df[COL_CREATED]].copy()

    # Delta real en segundos naturales
    df["delta_sec_natural"] = (df[COL_DUE_DATE] - df[COL_CREATED]).dt.total_seconds()

    # Delta en segundos hábiles
    df["delta_sec"] = df.apply(
        lambda row: business_seconds_between(row[COL_CREATED], row[COL_DUE_DATE]),
        axis=1
    )

    # Filtrar nulos
    df = df.dropna(subset=["delta_sec"]).copy()

    # Filtro opcional: descartar > 1 día hábil
    if APPLY_MAX_FILTER:
        max_seconds = MAX_HOURS * 3600
        df = df[df["delta_sec"] <= max_seconds].copy()

    # Orden cronológico por lead
    df = df.sort_values([COL_DEAL_ID, COL_DUE_DATE, COL_SUBJECT]).copy()

    # Primera llamada por lead único
    first_calls = df.drop_duplicates(subset=[COL_DEAL_ID], keep="first").copy()

    # Renombrar
    first_calls = first_calls.rename(columns={
        COL_DUE_DATE: "first_call_time",
        COL_SUBJECT: "first_call_subject"
    })

    keep_cols = [
        COL_DEAL_ID,
        COL_CREATED,
        "first_call_time",
        "first_call_subject",
        "delta_sec_natural",
        "delta_sec"
    ]

    if COL_OWNER in first_calls.columns:
        keep_cols.append(COL_OWNER)

    res = first_calls[keep_cols].copy()
    res["tiempo_hasta_primera_llamada"] = res["delta_sec"].apply(format_duration_exact)
    res["tiempo_natural"] = res["delta_sec_natural"].apply(format_duration_exact)
    res = res.sort_values(COL_CREATED).reset_index(drop=True)

    # Resumen por agente sobre leads únicos
    if COL_OWNER in res.columns:
        agent_stats = (
            res.groupby(COL_OWNER, dropna=False)
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

    # Media y mediana total sobre leads únicos
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

    required_cols = [COL_DEAL_ID, COL_CREATED, COL_DUE_DATE, COL_SUBJECT]
    missing = [c for c in required_cols if c not in df.columns]

    if missing:
        st.error("Faltan columnas necesarias: " + ", ".join(missing))
        st.write("Columnas detectadas:", list(df.columns))
        st.stop()

    res, agent_stats, media_total, mediana_total, debug_calls = compute_first_outbound_call(df)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Leads únicos con 1ª llamada", f"{len(res):,}".replace(",", "."))
    col2.metric("Media total", media_total)
    col3.metric("Mediana total", mediana_total)
    col4.metric("Filtro máximo", f"{MAX_HOURS} h" if APPLY_MAX_FILTER else "Sin filtro")

    st.subheader("✅ Primera llamada saliente por lead único")
    st.dataframe(res, use_container_width=True)

    if len(agent_stats) > 0:
        st.subheader("👤 Resumen por agente (sobre leads únicos)")
        st.dataframe(
            agent_stats[[COL_OWNER, "leads_unicos", "media", "mediana"]],
            use_container_width=True
        )

    with st.expander("🔎 Debug: llamadas salientes filtradas y ordenadas"):
        debug_cols = [
            COL_DEAL_ID,
            COL_CREATED,
            COL_DUE_DATE,
            COL_SUBJECT,
            "delta_sec_natural",
            "delta_sec"
        ]
        if COL_OWNER in debug_calls.columns:
            debug_cols.append(COL_OWNER)
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
