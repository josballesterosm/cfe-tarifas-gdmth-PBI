"""Lectura, escritura y validación del CSV de tarifas GDMTH."""

import csv
import os
import threading

HEADERS = [
    "division", "estado", "municipio", "anio", "mes",
    "intervalo_horario", "cargo", "valor", "Fecha",
]


def load_existing_keys(csv_path):
    """Leer CSV existente y devolver set de (division, anio, mes) ya scrapeados."""
    keys = set()
    if not os.path.exists(csv_path):
        return keys
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                keys.add((row["division"], int(row["anio"]), int(row["mes"])))
            except (KeyError, ValueError):
                continue
    return keys


def build_rows(division, estado, municipio, anio, mes, cargos):
    """Construir filas CSV a partir de los cargos extraídos.

    Args:
        cargos: list of dict con keys intervalo_horario, cargo, valor
    Returns:
        list of list: filas listas para csv.writer
    """
    fecha = f"{anio}-{mes:02d}"
    rows = []
    for c in cargos:
        rows.append([
            division,
            estado,
            municipio,
            anio,
            mes,
            c["intervalo_horario"],
            c["cargo"],
            c["valor"],
            fecha,
        ])
    return rows


def append_rows(csv_path, rows, lock=None):
    """Appendear filas al CSV (thread-safe). Crea el archivo con headers si no existe."""
    if lock:
        lock.acquire()
    try:
        file_exists = os.path.exists(csv_path) and os.path.getsize(csv_path) > 0
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(HEADERS)
            writer.writerows(rows)
    finally:
        if lock:
            lock.release()


def validate_csv(csv_path):
    """Validar integridad del CSV: sin duplicados, valores numéricos, 6 filas por combo."""
    if not os.path.exists(csv_path):
        print("CSV no existe.")
        return False

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        print("CSV vacío.")
        return False

    # Verificar headers
    expected = set(HEADERS)
    actual = set(rows[0].keys())
    if expected != actual:
        print(f"Headers incorrectos. Esperados: {expected}, Encontrados: {actual}")
        return False

    # Contar filas por (division, anio, mes)
    combos = {}
    errors = []
    for i, row in enumerate(rows):
        key = (row["division"], row["anio"], row["mes"])
        combos.setdefault(key, []).append(i + 2)  # +2 por header y 0-index

        # Validar valor numérico
        try:
            float(row["valor"])
        except ValueError:
            errors.append(f"Fila {i + 2}: valor no numérico '{row['valor']}'")

    # Detectar duplicados (más de 6 filas por combo)
    for key, line_nums in combos.items():
        if len(line_nums) > 6:
            errors.append(f"{key}: {len(line_nums)} filas (esperadas 6)")

    # Resumen
    divisiones = {row["division"] for row in rows}
    total_combos = len(combos)

    print(f"Total filas: {len(rows)}")
    print(f"Combos (division, anio, mes): {total_combos}")
    print(f"Divisiones: {len(divisiones)}/16")
    print(f"Errores: {len(errors)}")
    for e in errors[:10]:
        print(f"  {e}")

    return len(errors) == 0
