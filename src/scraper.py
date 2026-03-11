"""
Scraper de tarifas GDMTH de CFE.

Extrae tarifas de Gran Demanda en Media Tensión Horaria para las 16 divisiones
de CFE, desde 2018 hasta el presente. Genera un CSV compatible con Power BI.

Uso:
    python src/scraper.py                          # Completo 2018-presente
    python src/scraper.py --year 2026              # Solo un año
    python src/scraper.py --year 2026 --month 1    # Solo un mes
    python src/scraper.py --workers 1 --delay 2    # Modo conservador
    python src/scraper.py --validate                # Solo validar CSV existente
"""

import argparse
import json
import logging
import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# Añadir directorio raíz al path para imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.parser import get_asp_fields, find_option_value, find_tariff_table, parse_tariff_table, get_available_options
from src.csv_manager import load_existing_keys, build_rows, append_rows, validate_csv

# --- Constantes ---
URL = "https://app.cfe.mx/Aplicaciones/CCFE/Tarifas/TarifasCRENegocio/Tarifas/GranDemandaMTH.aspx"
DD_ANIO = "ctl00$ContentPlaceHolder1$Fecha$ddAnio"
DD_MES = "ctl00$ContentPlaceHolder1$Fecha2$ddMes"
DD_MES_ALT = "ctl00$ContentPlaceHolder1$MesVerano3$ddMesConsulta"
DD_ESTADO = "ctl00$ContentPlaceHolder1$EdoMpoDiv$ddEstado"
DD_MUNICIPIO = "ctl00$ContentPlaceHolder1$EdoMpoDiv$ddMunicipio"
DD_DIVISION = "ctl00$ContentPlaceHolder1$EdoMpoDiv$ddDivision"

CSV_PATH = PROJECT_ROOT / "data" / "tarifas_gdmth.csv"
DIVISIONES_PATH = PROJECT_ROOT / "config" / "divisiones.json"

# Saturación global: si un worker detecta que CFE está caído, los demás esperan
server_ok = threading.Event()
server_ok.set()  # Inicialmente OK

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("cfe-scraper")


# --- HTTP helpers ---

def new_session():
    """Crear una nueva sesión HTTP con headers completos de browser."""
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        "Origin": "https://app.cfe.mx",
        "Referer": "https://app.cfe.mx/Aplicaciones/CCFE/Tarifas/TarifasCRENegocio/Tarifas/GranDemandaMTH.aspx",
        "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"macOS"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    })
    return s


def detect_mes_field(soup):
    """Detectar qué campo de mes está activo en la página.

    CFE usa 'Fecha2$ddMes' para el año por defecto (2026) y
    'MesVerano3$ddMesConsulta' después de cambiar el año via postback.
    """
    if soup.find("select", {"name": DD_MES}):
        return DD_MES
    if soup.find("select", {"name": DD_MES_ALT}):
        return DD_MES_ALT
    return DD_MES  # fallback


def do_postback(session, soup, event_target, select_values, retries=5, delay=5):
    """Ejecutar postback ASP.NET con retry y backoff exponencial."""
    asp = get_asp_fields(soup)
    data = {
        "__EVENTTARGET": event_target,
        "__EVENTARGUMENT": "",
        **asp,
        **select_values,
    }
    for attempt in range(retries):
        server_ok.wait()  # Esperar si otro worker detectó saturación
        try:
            resp = session.post(URL, data=data, timeout=30)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")
        except (requests.RequestException, Exception) as e:
            if attempt < retries - 1:
                base_wait = delay * (2 ** attempt)
                jitter = base_wait * random.uniform(-0.3, 0.3)
                wait = base_wait + jitter
                log.warning(f"Retry {attempt + 1}/{retries} tras error: {e}. Esperando {wait:.1f}s...")
                time.sleep(wait)
            else:
                _signal_server_down()
                raise


def _signal_server_down(cooldown=60):
    """Señalar que CFE está saturado. Todos los workers pausan."""
    if server_ok.is_set():
        log.warning(f"Servidor CFE saturado. Pausando todos los workers por {cooldown}s...")
        server_ok.clear()
        time.sleep(cooldown)
        server_ok.set()
        log.info("Reanudando workers tras cooldown de saturación.")


def initial_get(session, retries=5, delay=5):
    """GET inicial con retry."""
    for attempt in range(retries):
        server_ok.wait()  # Esperar si otro worker detectó saturación
        try:
            resp = session.get(URL, timeout=30)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")
        except (requests.RequestException, Exception) as e:
            if attempt < retries - 1:
                base_wait = delay * (2 ** attempt)
                jitter = base_wait * random.uniform(-0.3, 0.3)
                wait = base_wait + jitter
                log.warning(f"Retry GET {attempt + 1}/{retries}: {e}. Esperando {wait:.1f}s...")
                time.sleep(wait)
            else:
                _signal_server_down()
                raise


def health_check():
    """Verificar que CFE responde antes de iniciar el scrape completo.

    Hace GET + 1 POST de prueba. Si falla, retorna False.
    """
    log.info("Health check: verificando conectividad con CFE...")
    session = new_session()
    try:
        soup = initial_get(session, retries=2, delay=3)
    except Exception as e:
        log.error(f"Health check FALLÓ en GET: {e}")
        return False

    # POST de prueba: seleccionar año
    try:
        asp = get_asp_fields(soup)
        data = {
            "__EVENTTARGET": DD_ANIO,
            "__EVENTARGUMENT": "",
            **asp,
            DD_ANIO: "2026",
            DD_MES: "0",
            DD_ESTADO: "0",
            DD_MUNICIPIO: "0",
            DD_DIVISION: "0",
        }
        resp = session.post(URL, data=data, timeout=30)
        resp.raise_for_status()
        log.info("Health check OK: CFE responde correctamente")
        return True
    except Exception as e:
        log.error(f"Health check FALLÓ en POST: {e}")
        return False


# --- Scraping de un mes completo (16 divisiones) ---

def scrape_month(anio, mes, divisiones, existing_keys, csv_path, lock, request_delay=1.0):
    """Scrape de las 16 divisiones para un mes dado.

    Flujo:
    - GET inicial
    - POST año (si no es el año por defecto) → detectar campo de mes correcto
    - POST mes → POST estado₁ → POST municipio₁ → POST división₁ → EXTRAER
    - POST estado₂ (reusa ViewState) → POST municipio₂ → POST división₂ → EXTRAER
    - ... repetir

    Para ESTADO DE MÉXICO (2 divisiones): tras la primera, cambiamos solo municipio + división.
    """
    session = new_session()
    extracted = 0
    skipped = 0
    consecutive_errors = 0
    MAX_CONSECUTIVE_ERRORS = 3

    # Agrupar por estado para optimizar (ESTADO DE MÉXICO tiene 2 divisiones)
    by_estado = {}
    for div in divisiones:
        by_estado.setdefault(div["estado"], []).append(div)

    # GET inicial
    try:
        soup = initial_get(session)
    except Exception as e:
        log.error(f"[{anio}-{mes:02d}] GET inicial falló: {e}")
        return extracted, skipped

    # Detectar qué campo de mes usa la página por defecto
    dd_mes = detect_mes_field(soup)

    select_values = {
        DD_ANIO: str(anio),
        dd_mes: "0",
        DD_ESTADO: "0",
        DD_MUNICIPIO: "0",
        DD_DIVISION: "0",
    }

    # Postback de año si no es el año por defecto de la página
    default_year_sel = soup.find("select", {"name": DD_ANIO})
    default_year = None
    if default_year_sel:
        selected_opt = default_year_sel.find("option", selected=True)
        if selected_opt:
            default_year = selected_opt.get("value")

    if default_year and str(anio) != default_year:
        try:
            time.sleep(request_delay)
            soup = do_postback(session, soup, DD_ANIO, select_values)
            # Después del postback de año, el campo de mes puede cambiar
            dd_mes = detect_mes_field(soup)
            # Reconstruir select_values con el campo de mes correcto
            select_values = {
                DD_ANIO: str(anio),
                dd_mes: "0",
                DD_ESTADO: "0",
                DD_MUNICIPIO: "0",
                DD_DIVISION: "0",
            }
            log.info(f"[{anio}-{mes:02d}] Año seleccionado, campo mes: {dd_mes.split('$')[-1]}")
        except Exception as e:
            log.error(f"[{anio}-{mes:02d}] Postback de año falló: {e}")
            return extracted, skipped

    first_estado = True

    for estado_nombre, divs_in_estado in by_estado.items():
        # Circuit breaker: abortar mes si hay demasiados errores consecutivos
        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
            log.error(f"[{anio}-{mes:02d}] {consecutive_errors} errores consecutivos. Abortando mes.")
            break
        # Verificar si TODAS las divisiones de este estado ya están scrapeadas
        all_done = all(
            (d["division"], anio, mes) in existing_keys for d in divs_in_estado
        )
        if all_done:
            skipped += len(divs_in_estado)
            continue

        try:
            # Seleccionar estado (y mes en el primer POST)
            time.sleep(request_delay)

            if first_estado:
                # Enviar mes + estado juntos en 1 POST
                select_values[dd_mes] = str(mes)
                estado_value = find_option_value(soup, DD_ESTADO, estado_nombre)
                if not estado_value:
                    # Fallback: primero hacer postback de mes, luego buscar estado
                    soup = do_postback(session, soup, dd_mes, select_values)
                    time.sleep(request_delay)
                    estado_value = find_option_value(soup, DD_ESTADO, estado_nombre)
                    if not estado_value:
                        log.error(f"[{anio}-{mes:02d}] Estado '{estado_nombre}' no encontrado")
                        skipped += len(divs_in_estado)
                        continue

                select_values[DD_ESTADO] = estado_value
                select_values[DD_MUNICIPIO] = "0"
                select_values[DD_DIVISION] = "0"
                soup = do_postback(session, soup, DD_ESTADO, select_values)
                first_estado = False
            else:
                # Reusar ViewState, cambiar solo estado
                estado_value = find_option_value(soup, DD_ESTADO, estado_nombre)
                if not estado_value:
                    log.error(f"[{anio}-{mes:02d}] Estado '{estado_nombre}' no encontrado")
                    skipped += len(divs_in_estado)
                    continue
                select_values[DD_ESTADO] = estado_value
                select_values[DD_MUNICIPIO] = "0"
                select_values[DD_DIVISION] = "0"
                soup = do_postback(session, soup, DD_ESTADO, select_values)

            # Para cada división en este estado
            first_mun_in_estado = True
            for div_info in divs_in_estado:
                div_name = div_info["division"]
                mun_name = div_info["municipio"]

                if (div_name, anio, mes) in existing_keys:
                    skipped += 1
                    continue

                # Seleccionar municipio
                time.sleep(request_delay)
                mun_value = find_option_value(soup, DD_MUNICIPIO, mun_name)
                if not mun_value:
                    log.warning(f"[{anio}-{mes:02d}] Municipio '{mun_name}' no encontrado para {div_name}")
                    skipped += 1
                    continue

                select_values[DD_MUNICIPIO] = mun_value
                select_values[DD_DIVISION] = "0"
                soup = do_postback(session, soup, DD_MUNICIPIO, select_values)

                # Seleccionar división
                time.sleep(request_delay)
                div_value = find_option_value(soup, DD_DIVISION, div_name)
                if not div_value:
                    log.warning(f"[{anio}-{mes:02d}] División '{div_name}' no encontrada")
                    skipped += 1
                    continue

                select_values[DD_DIVISION] = div_value
                soup = do_postback(session, soup, DD_DIVISION, select_values)

                # Parsear tabla
                table = find_tariff_table(soup)
                if not table:
                    log.warning(f"[{anio}-{mes:02d}] {div_name}: tabla no encontrada (mes no publicado?)")
                    skipped += 1
                    continue

                cargos = parse_tariff_table(table)
                if not cargos:
                    log.warning(f"[{anio}-{mes:02d}] {div_name}: tabla sin datos parseables")
                    skipped += 1
                    continue

                # Guardar al CSV
                rows = build_rows(
                    div_info["division"], div_info["estado"], div_info["municipio"],
                    anio, mes, cargos,
                )
                append_rows(str(csv_path), rows, lock)
                existing_keys.add((div_name, anio, mes))
                extracted += 1
                consecutive_errors = 0  # Reset en éxito
                log.info(f"[{anio}-{mes:02d}] {div_name}: {len(cargos)} cargos extraídos")

        except Exception as e:
            consecutive_errors += 1
            log.error(f"[{anio}-{mes:02d}] Error en estado '{estado_nombre}': {e} (consecutivos: {consecutive_errors}/{MAX_CONSECUTIVE_ERRORS})")
            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                log.error(f"[{anio}-{mes:02d}] Circuit breaker activado. Abortando mes.")
                return extracted, skipped
            # Cooldown antes de reintentar para no saturar CFE
            log.info(f"[{anio}-{mes:02d}] Cooldown 30s antes de reiniciar sesión...")
            time.sleep(30)
            session = new_session()
            try:
                soup = initial_get(session)
                dd_mes = detect_mes_field(soup)
                select_values = {
                    DD_ANIO: str(anio),
                    dd_mes: "0",
                    DD_ESTADO: "0",
                    DD_MUNICIPIO: "0",
                    DD_DIVISION: "0",
                }
                # Re-do year postback if needed
                default_year_sel = soup.find("select", {"name": DD_ANIO})
                if default_year_sel:
                    selected_opt = default_year_sel.find("option", selected=True)
                    if selected_opt and str(anio) != selected_opt.get("value"):
                        soup = do_postback(session, soup, DD_ANIO, select_values)
                        dd_mes = detect_mes_field(soup)
                        select_values = {
                            DD_ANIO: str(anio),
                            dd_mes: "0",
                            DD_ESTADO: "0",
                            DD_MUNICIPIO: "0",
                            DD_DIVISION: "0",
                        }
                first_estado = True
            except Exception as e2:
                log.error(f"[{anio}-{mes:02d}] Reinicio falló: {e2}. Abortando mes.")
                return extracted, skipped

    return extracted, skipped


# --- Worker para un rango de años ---

def worker_year_range(years, months, divisiones, existing_keys, csv_path, lock, request_delay):
    """Worker que procesa un rango de años secuencialmente."""
    total_extracted = 0
    total_skipped = 0
    consecutive_failed_months = 0
    MAX_FAILED_MONTHS = 3

    for anio in years:
        for mes in months:
            server_ok.wait()  # Esperar si CFE está saturado
            ext, skip = scrape_month(
                anio, mes, divisiones, existing_keys, csv_path, lock, request_delay,
            )
            total_extracted += ext
            total_skipped += skip
            if ext > 0:
                consecutive_failed_months = 0
                log.info(f"[{anio}-{mes:02d}] Completado: {ext} divisiones extraídas, {skip} saltadas")
            else:
                consecutive_failed_months += 1
                log.warning(f"[{anio}-{mes:02d}] 0 extracciones (fallos consecutivos: {consecutive_failed_months}/{MAX_FAILED_MONTHS})")
                if consecutive_failed_months >= MAX_FAILED_MONTHS:
                    log.error(f"Worker abortado: {MAX_FAILED_MONTHS} meses consecutivos sin extracciones. Servidor posiblemente caído.")
                    return total_extracted, total_skipped
            # Pausa entre meses para no saturar CFE
            time.sleep(3)

    return total_extracted, total_skipped


# --- CLI ---

def parse_args():
    parser = argparse.ArgumentParser(description="Scraper de tarifas GDMTH de CFE")
    parser.add_argument("--year", type=int, help="Año específico (ej: 2026)")
    parser.add_argument("--month", type=int, help="Mes específico (1-12, requiere --year)")
    parser.add_argument("--workers", type=int, default=1, help="Workers concurrentes (default: 1)")
    parser.add_argument("--delay", type=float, default=1.5, help="Delay entre requests en segundos (default: 1.5)")
    parser.add_argument("--validate", action="store_true", help="Solo validar CSV existente")
    parser.add_argument("--csv", type=str, default=None, help="Ruta alternativa para el CSV")
    return parser.parse_args()


def main():
    args = parse_args()
    csv_path = Path(args.csv) if args.csv else CSV_PATH

    if args.validate:
        validate_csv(str(csv_path))
        return

    # Health check antes de iniciar
    if not health_check():
        log.error("Health check falló. CFE no responde. Abortando.")
        log.error("Verifica que no haya procesos scraper zombie corriendo (ps aux | grep scraper)")
        sys.exit(1)

    # Cargar divisiones
    with open(DIVISIONES_PATH, "r", encoding="utf-8") as f:
        divisiones = json.load(f)
    log.info(f"Cargadas {len(divisiones)} divisiones desde {DIVISIONES_PATH}")

    # Cargar checkpoint (datos existentes)
    existing_keys = load_existing_keys(str(csv_path))
    if existing_keys:
        log.info(f"Checkpoint: {len(existing_keys)} combos (division, año, mes) ya en CSV")

    # Asegurar directorio data/
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    # Determinar rangos
    current_year = datetime.now().year
    current_month = datetime.now().month

    if args.year and args.month:
        years_months = [(args.year, [args.month])]
    elif args.year:
        max_month = current_month if args.year == current_year else 12
        years_months = [(args.year, list(range(1, max_month + 1)))]
    else:
        years_months = []
        for y in range(2018, current_year + 1):
            max_m = current_month if y == current_year else 12
            years_months.append((y, list(range(1, max_m + 1))))

    # Aplanar a lista de (año, meses) para distribución entre workers
    all_years = [ym[0] for ym in years_months]
    year_to_months = {ym[0]: ym[1] for ym in years_months}

    lock = threading.Lock()
    # existing_keys es compartido pero solo se añaden elementos (set.add es thread-safe en CPython)

    num_workers = min(args.workers, len(all_years))
    if num_workers <= 1:
        # Single worker
        log.info(f"Iniciando scrape secuencial: {len(all_years)} años")
        total_ext, total_skip = 0, 0
        for y in all_years:
            ext, skip = worker_year_range(
                [y], year_to_months[y], divisiones, existing_keys, csv_path, lock, args.delay,
            )
            total_ext += ext
            total_skip += skip
    else:
        # Distribuir años entre workers
        chunks = [[] for _ in range(num_workers)]
        for i, y in enumerate(all_years):
            chunks[i % num_workers].append(y)

        log.info(f"Iniciando scrape con {num_workers} workers")
        for i, chunk in enumerate(chunks):
            log.info(f"  Worker {i + 1}: años {chunk}")

        total_ext, total_skip = 0, 0
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = []
            for chunk in chunks:
                if not chunk:
                    continue
                future = executor.submit(
                    _worker_wrapper,
                    chunk,
                    year_to_months,
                    divisiones,
                    existing_keys,
                    csv_path,
                    lock,
                    args.delay,
                )
                futures.append(future)

            for future in as_completed(futures):
                try:
                    ext, skip = future.result()
                    total_ext += ext
                    total_skip += skip
                except Exception as e:
                    log.error(f"Worker falló: {e}")

    log.info(f"Scrape completado: {total_ext} divisiones extraídas, {total_skip} saltadas")

    # Validar
    if total_ext > 0:
        log.info("Validando CSV...")
        validate_csv(str(csv_path))


def _worker_wrapper(years, year_to_months, divisiones, existing_keys, csv_path, lock, delay):
    """Wrapper que llama worker_year_range con los meses correctos por año."""
    total_ext, total_skip = 0, 0
    for y in years:
        months = year_to_months.get(y, [])
        ext, skip = worker_year_range(
            [y], months, divisiones, existing_keys, csv_path, lock, delay,
        )
        total_ext += ext
        total_skip += skip
    return total_ext, total_skip


if __name__ == "__main__":
    main()
