"""Parsing de páginas ASP.NET y tablas de tarifas GDMTH de CFE."""

from bs4 import BeautifulSoup

ASP_FIELDS = ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION")


def get_asp_fields(soup):
    """Extraer __VIEWSTATE, __VIEWSTATEGENERATOR, __EVENTVALIDATION del HTML."""
    fields = {}
    for name in ASP_FIELDS:
        tag = soup.find("input", {"name": name})
        if tag:
            fields[name] = tag.get("value", "")
    return fields


def find_option_value(soup, select_name, target_text):
    """Buscar el value de una opción en un dropdown por texto (case-insensitive).

    Returns None si no se encuentra.
    """
    sel = soup.find("select", {"name": select_name})
    if not sel:
        return None
    target_upper = target_text.upper()
    for opt in sel.find_all("option"):
        if opt.text.strip().upper() == target_upper:
            return opt["value"]
    return None


def get_available_options(soup, select_name):
    """Listar opciones disponibles en un dropdown (excluyendo default value='0')."""
    sel = soup.find("select", {"name": select_name})
    if not sel:
        return []
    return [
        {"text": opt.text.strip(), "value": opt["value"]}
        for opt in sel.find_all("option")
        if opt.get("value", "0") != "0"
    ]


def find_tariff_table(soup):
    """Encontrar la tabla de tarifas GDMTH en el HTML.

    La tabla tiene headers 'Tarifa' y 'Cargo' con 7+ filas (header + 6 datos).
    """
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) >= 7:
            first_row_cells = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])]
            if "Tarifa" in first_row_cells and "Cargo" in first_row_cells:
                return table
    return None


def parse_tariff_table(table):
    """Parsear la tabla de tarifas y devolver lista de cargos.

    La tabla tiene rowspan=6 en las primeras 2 columnas. La primera fila de datos
    tiene 6 celdas, las siguientes tienen 4 (sin las columnas con rowspan).

    Returns:
        list of dict: [{intervalo_horario, cargo, valor}, ...]
    """
    rows = table.find_all("tr")
    results = []

    for row in rows[1:]:  # Skip header
        cells = [c.get_text(strip=True) for c in row.find_all(["th", "td"])]

        if len(cells) == 6:
            # Fila completa con rowspan: Tarifa | Descripción | Int.Horario | Cargo | Unidades | Valor
            intervalo = cells[2]
            cargo = cells[3]
            valor_str = cells[5]
        elif len(cells) == 4:
            # Fila parcial: Int.Horario | Cargo | Unidades | Valor
            intervalo = cells[0]
            cargo = cells[1]
            valor_str = cells[3]
        elif len(cells) == 3:
            # Fila sin unidades: Int.Horario | Cargo | Valor
            intervalo = cells[0]
            cargo = cells[1]
            valor_str = cells[2]
        else:
            continue

        try:
            valor = float(valor_str.replace(",", ""))
        except ValueError:
            continue

        results.append({
            "intervalo_horario": intervalo,
            "cargo": cargo,
            "valor": valor,
        })

    return results
