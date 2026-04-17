import io
import json
import time
from pathlib import Path
from typing import Optional, Tuple, List, Dict

import pandas as pd
import requests

# =========================
# CONFIG
# =========================

API_TOKEN = "TU_API_TOKEN"
COMPANY_DOMAIN = "asociacioneuropeacompaniadesegurossa"  # sin .pipedrive.com
INPUT_XLSX = "actividades_pipedrive.xlsx"
OUTPUT_XLSX = "sla_primera_llamada_con_reasignaciones.xlsx"

# Columnas del Excel de actividades
COL_DEAL_ID = "Negocio - ID"
COL_CREATED = "Negocio - Negocio creado el"
COL_DUE_DATE = "Actividad - Fecha de vencimiento"
COL_SUBJECT = "Actividad - Asunto"
COL_OWNER = "Negocio - Propietario"

# Ajustes de horario
WORK_START_HOUR = 9
SATURDAY_END_HOUR = 18

# Festivos indicados por ti (2026)
HOLIDAYS_2026 = {
    pd.Timestamp("2026-01-01").date(),  # Año Nuevo
    pd.Timestamp("2026-01-06").date(),  # Epifanía del Señor
    pd.Timestamp("2026-04-03").date(),  # Viernes Santo
    pd.Timestamp("2026-05-01").date(),  # Fiesta del Trabajo
    pd.Timestamp("2026-08-15").date(),  # Asunción de la Virgen
    pd.Timestamp("2026-10-12").date(),  # Fiesta Nacional de España
    pd.Timestamp("2026-11-01").date(),  # Todos los Santos
    pd.Timestamp("2026-12-08").date(),  # Inmaculada Concepción
    pd.Timestamp("2026-12-25").date(),  # Navidad
}

# Opcional: filtrar SLAs de 1 día o más
APPLY_FILTER_GE_1DAY = False
ONE_DAY_SECONDS = 86400

# Cache local de flows para no rehacer llamadas si reejecutas
CACHE_DIR = Path("pipedrive_flow_cache")
CACHE_DIR.mkdir(exist_ok=True)

# =========================
# HELPERS
# =========================

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


def is_holiday(ts: pd.Timestamp) -> bool:
    return ts.date() in HOLIDAYS_2026


def next_valid_workday_start(ts: pd.Timestamp) -> pd.Timestamp:
    """
    Mueve ts al siguiente día laborable a las 09:00.
    Considera domingos y festivos.
    """
    ts = ts.replace(hour=WORK_START_HOUR, minute=0, second=0, microsecond=0)

    while ts.weekday() == 6 or is_holiday(ts):
        ts = ts + pd.Timedelta(days=1)
        ts = ts.replace(hour=WORK_START_HOUR, minute=0, second=0, microsecond=0)

    return ts


def adjust_creation_time(ts: pd.Timestamp) -> pd.Timestamp:
    """
    Ajusta la creación al horario operativo:
    - lunes a viernes: desde las 09:00
    - sábado: desde las 09:00 hasta las 18:00
    - domingo: pasa al siguiente laborable a las 09:00
    - festivo: pasa al siguiente laborable a las 09:00
    """
    if pd.isna(ts):
        return ts

    # Domingo o festivo -> siguiente laborable 09:00
    if ts.weekday() == 6 or is_holiday(ts):
        next_day = ts + pd.Timedelta(days=1)
        return next_valid_workday_start(next_day)

    # Sábado
    if ts.weekday() == 5:
        if ts.hour < WORK_START_HOUR:
            return ts.replace(hour=WORK_START_HOUR, minute=0, second=0, microsecond=0)
        if ts.hour >= SATURDAY_END_HOUR:
            next_day = ts + pd.Timedelta(days=1)
            return next_valid_workday_start(next_day)
        return ts

    # Lunes a viernes
    if ts.hour < WORK_START_HOUR:
        return ts.replace(hour=WORK_START_HOUR, minute=0, second=0, microsecond=0)

    return ts


def normalize_owner_name(value: Optional[str]) -> str:
    """
    Normaliza nombres para cruzar el propietario del Excel con new_value_formatted del flow.
    """
    if value is None or pd.isna(value):
        return ""
    return " ".join(str(value).strip().lower().split())


def request_with_retry(url: str, max_retries: int = 3, sleep_seconds: float = 0.6) -> dict:
    last_error = None
    for attempt in range(max_retries):
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            data = r.json()
            if not data.get("success", False):
                raise RuntimeError(f"API success=false: {data}")
            return data
        except Exception as e:
            last_error = e
            time.sleep(sleep_seconds * (attempt + 1))
    raise RuntimeError(f"Error llamando a {url}: {last_error}")


def get_flow_cache_path(deal_id: int) -> Path:
    return CACHE_DIR / f"deal_{deal_id}.json"


def fetch_deal_flow(deal_id: int, force_refresh: bool = False) -> dict:
    """
    Descarga y cachea el flow del deal.
    """
    cache_path = get_flow_cache_path(deal_id)

    if cache_path.exists() and not force_refresh:
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)

    url = f"https://{COMPANY_DOMAIN}.pipedrive.com/api/v1/deals/{deal_id}/flow?api_token={API_TOKEN}"
    data = request_with_retry(url)

    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

    return data


def extract_owner_changes_from_flow(flow_json: dict, deal_id: int) -> List[Dict]:
    """
    Extrae cambios de propietario del flow.
    Busca object=dealChange y field_key=user_id.
    """
    rows = []

    for item in flow_json.get("data", []) or []:
        if item.get("object") != "dealChange":
            continue

        data = item.get("data", {}) or {}
        if data.get("field_key") != "user_id":
            continue

        additional = data.get("additional_data", {}) or {}

        assigned_at = pd.to_datetime(data.get("log_time"), errors="coerce")
        assigned_to = additional.get("new_value_formatted")
        assigned_from = additional.get("old_value_formatted")

        rows.append({
            "deal_id": deal_id,
            "assigned_at": assigned_at,
            "assigned_to": assigned_to,
            "assigned_to_norm": normalize_owner_name(assigned_to),
            "assigned_from": assigned_from,
            "raw_change_id": data.get("id"),
            "timestamp": item.get("timestamp"),
        })

    return rows


def build_owner_changes_table(deal_ids: List[int], force_refresh: bool = False) -> pd.DataFrame:
    all_rows = []
    total = len(deal_ids)

    for i, deal_id in enumerate(deal_ids, start=1):
        if i % 100 == 0 or i == total:
            print(f"[{i}/{total}] Descargando flow deal {deal_id}")

        flow_json = fetch_deal_flow(deal_id, force_refresh=force_refresh)
        rows = extract_owner_changes_from_flow(flow_json, deal_id)
        all_rows.extend(rows)

    owner_changes = pd.DataFrame(all_rows)

    if len(owner_changes) == 0:
        owner_changes = pd.DataFrame(columns=[
            "deal_id", "assigned_at", "assigned_to", "assigned_to_norm",
            "assigned_from", "raw_change_id", "timestamp"
        ])

    return owner_changes


def compute_first_outbound_call(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Devuelve:
    - first_calls: una fila por deal con la primera llamada saliente
    - debug_calls: llamadas filtradas y ordenadas
    """
    df = df.copy()

    df[COL_DEAL_ID] = pd.to_numeric(df[COL_DEAL_ID], errors="coerce").astype("Int64")
    df[COL_CREATED] = pd.to_datetime(df[COL_CREATED], errors="coerce")
    df[COL_DUE_DATE] = pd.to_datetime(df[COL_DUE_DATE], errors="coerce")
    df[COL_SUBJECT] = df[COL_SUBJECT].astype(str).str.strip()

    df = df.dropna(subset=[COL_DEAL_ID, COL_CREATED, COL_DUE_DATE, COL_SUBJECT]).copy()

    # Solo llamadas salientes
    df = df[df[COL_SUBJECT].str.contains("llamada saliente", case=False, na=False)].copy()

    # Ajuste de creación
    df["created_adjusted"] = df[COL_CREATED].apply(adjust_creation_time)

    # Solo llamadas posteriores a creación ajustada
    df["delta_from_created_sec"] = (df[COL_DUE_DATE] - df["created_adjusted"]).dt.total_seconds()
    df = df[df["delta_from_created_sec"] >= 0].copy()

    # Orden cronológico
    df = df.sort_values([COL_DEAL_ID, COL_DUE_DATE, COL_SUBJECT]).copy()

    # Primera llamada por deal
    first_calls = df.drop_duplicates(subset=[COL_DEAL_ID], keep="first").copy()

    first_calls = first_calls.rename(columns={
        COL_DUE_DATE: "first_call_time",
        COL_SUBJECT: "first_call_subject",
        COL_OWNER: "call_owner"
    })

    # Mantener columnas relevantes
    keep_cols = [
        COL_DEAL_ID,
        COL_CREATED,
        "created_adjusted",
        "first_call_time",
        "first_call_subject",
        "delta_from_created_sec",
    ]

    if "call_owner" in first_calls.columns:
        keep_cols.append("call_owner")

    first_calls = first_calls[keep_cols].copy()
    first_calls["call_owner_norm"] = first_calls["call_owner"].apply(normalize_owner_name)

    return first_calls, df


def attach_real_start_time(first_calls: pd.DataFrame, owner_changes: pd.DataFrame) -> pd.DataFrame:
    """
    Para cada primera llamada:
    - busca la última asignación al mismo agente que llama, anterior o igual a la llamada
    - si existe, usa esa como start_time_real
    - si no, usa created_adjusted
    """
    res = first_calls.copy()

    if len(owner_changes) == 0:
        res["assigned_at_for_call_owner"] = pd.NaT
        res["start_time_real"] = res["created_adjusted"]
        res["start_source"] = "created_adjusted"
    else:
        owner_changes = owner_changes.copy()
        owner_changes["assigned_at"] = pd.to_datetime(owner_changes["assigned_at"], errors="coerce")
        owner_changes = owner_changes.dropna(subset=["deal_id", "assigned_at", "assigned_to_norm"]).copy()

        assigned_at_list = []
        start_source_list = []

        # Agrupación para acelerar búsquedas
        grouped = {
            deal_id: chunk.sort_values("assigned_at")
            for deal_id, chunk in owner_changes.groupby("deal_id")
        }

        for _, row in res.iterrows():
            deal_id = int(row[COL_DEAL_ID])
            call_time = row["first_call_time"]
            call_owner_norm = row["call_owner_norm"]

            chunk = grouped.get(deal_id)

            if chunk is None or len(chunk) == 0:
                assigned_at_list.append(pd.NaT)
                start_source_list.append("created_adjusted")
                continue

            matches = chunk[
                (chunk["assigned_to_norm"] == call_owner_norm) &
                (chunk["assigned_at"] <= call_time)
            ].copy()

            if len(matches) == 0:
                assigned_at_list.append(pd.NaT)
                start_source_list.append("created_adjusted")
            else:
                assigned_at = matches["assigned_at"].max()
                assigned_at_list.append(assigned_at)
                start_source_list.append("owner_reassignment")

        res["assigned_at_for_call_owner"] = assigned_at_list
        res["start_source"] = start_source_list
        res["start_time_real"] = res["assigned_at_for_call_owner"].fillna(res["created_adjusted"])

    # SLA real
    res["delta_sec"] = (res["first_call_time"] - res["start_time_real"]).dt.total_seconds()
    res = res[res["delta_sec"] >= 0].copy()

    if APPLY_FILTER_GE_1DAY:
        res = res[res["delta_sec"] < ONE_DAY_SECONDS].copy()

    res["tiempo_hasta_primera_llamada"] = res["delta_sec"].apply(format_duration_exact)
    res["tiempo_desde_creacion_ajustada"] = res["delta_from_created_sec"].apply(format_duration_exact)

    return res


def build_agent_stats(res: pd.DataFrame) -> pd.DataFrame:
    if "call_owner" not in res.columns or len(res) == 0:
        return pd.DataFrame()

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

    return agent_stats


def to_excel_bytes(
    res: pd.DataFrame,
    agent_stats: pd.DataFrame,
    debug_calls: pd.DataFrame,
    owner_changes: pd.DataFrame
) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        res.to_excel(writer, index=False, sheet_name="sla_real_primera_llamada")
        if len(agent_stats) > 0:
            agent_stats.to_excel(writer, index=False, sheet_name="resumen_por_agente")
        debug_calls.to_excel(writer, index=False, sheet_name="debug_llamadas_filtradas")
        owner_changes.to_excel(writer, index=False, sheet_name="cambios_propietario")
    return output.getvalue()


def main():
    print("Leyendo Excel de actividades...")
    df = pd.read_excel(INPUT_XLSX)

    required_cols = [COL_DEAL_ID, COL_CREATED, COL_DUE_DATE, COL_SUBJECT]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas necesarias en el Excel: {missing}")

    print("Calculando primera llamada saliente por deal...")
    first_calls, debug_calls = compute_first_outbound_call(df)

    deal_ids = (
        first_calls[COL_DEAL_ID]
        .dropna()
        .astype(int)
        .drop_duplicates()
        .tolist()
    )

    print(f"Deals únicos con primera llamada: {len(deal_ids)}")

    print("Descargando historial de cambios de propietario...")
    owner_changes = build_owner_changes_table(deal_ids, force_refresh=False)

    print("Uniendo primera llamada con última reasignación válida...")
    res = attach_real_start_time(first_calls, owner_changes)

    print("Calculando resumen por agente...")
    agent_stats = build_agent_stats(res)

    media_total = format_duration_exact(res["delta_sec"].mean()) if len(res) > 0 else ""
    mediana_total = format_duration_exact(res["delta_sec"].median()) if len(res) > 0 else ""

    print("\n===== RESULTADOS =====")
    print(f"Leads únicos con 1ª llamada: {len(res)}")
    print(f"Media total SLA real: {media_total}")
    print(f"Mediana total SLA real: {mediana_total}")

    print(f"Guardando Excel en: {OUTPUT_XLSX}")
    xlsx_bytes = to_excel_bytes(res, agent_stats, debug_calls, owner_changes)
    with open(OUTPUT_XLSX, "wb") as f:
        f.write(xlsx_bytes)

    print("Hecho.")


if __name__ == "__main__":
    main()
