# CFE Tarifas GDMTH — Scraper & Automatización para Power BI

## Objetivo del Proyecto

Automatizar la extracción mensual de tarifas eléctricas GDMTH (Gran Demanda en Media Tensión Horaria) desde el portal de CFE, generando un CSV que alimente directamente un dashboard en Power BI. Actualmente este proceso se hace manualmente cada mes.

## Fuente de Datos

- **URL**: `https://app.cfe.mx/Aplicaciones/CCFE/Tarifas/TarifasCRENegocio/Tarifas/GranDemandaMTH.aspx`
- **Tecnología del sitio**: ASP.NET Web Forms con ViewState. Los datos se cargan dinámicamente al seleccionar año → mes → estado → municipio → división.
- **Frecuencia de actualización**: CFE publica tarifas nuevas cada mes (generalmente los primeros días del mes).
- **Solo GDMTH** — no capturamos GDMTO ni otras tarifas.

## Estructura de Datos de Salida (CSV)

El CSV debe tener exactamente estas columnas, en este orden:

```
division,estado,municipio,anio,mes,intervalo_horario,cargo,valor,Fecha
```

### Columnas

| Columna | Tipo | Ejemplo | Descripción |
|---------|------|---------|-------------|
| `division` | string | `CENTRO SUR` | División tarifaria de CFE (16 divisiones) |
| `estado` | string | `MORELOS` | Estado de la república |
| `municipio` | string | `CUERNAVACA` | Municipio sede de la división |
| `anio` | int | `2026` | Año |
| `mes` | int | `3` | Mes (1-12, sin cero inicial) |
| `intervalo_horario` | string | `Base` | Periodo horario. Ver valores abajo |
| `cargo` | string | `Variable (Energía)` | Tipo de cargo. Ver valores abajo |
| `valor` | float | `1.539` | Valor numérico de la tarifa |
| `Fecha` | string | `2026-03` | Formato YYYY-MM para filtros en Power BI |

### Valores de `intervalo_horario`

- `-` (guión) → Para cargos que NO dependen del horario (Fijo, Distribución, Capacidad)
- `Base` → Periodo base
- `Intermedia` → Periodo intermedio
- `Punta` → Periodo punta

### Valores de `cargo`

- `Fijo` → Cargo fijo mensual ($/mes). intervalo_horario = `-`
- `Variable (Energía)` → Cargo por energía consumida ($/kWh). Tiene variantes por intervalo_horario (Base, Intermedia, Punta)
- `Distribución` → Cargo por distribución ($/kW). intervalo_horario = `-`
- `Capacidad` → Cargo por capacidad ($/kW). intervalo_horario = `-`

### Las 16 Divisiones de CFE

Cada división tiene un estado y municipio de referencia:

| División | Estado | Municipio |
|----------|--------|-----------|
| BAJA CALIFORNIA | BAJA CALIFORNIA | TIJUANA |
| BAJA CALIFORNIA SUR | BAJA CALIFORNIA SUR | LA PAZ |
| BAJÍO | AGUASCALIENTES | AGUASCALIENTES |
| CENTRO OCCIDENTE | MICHOACÁN | MORELIA |
| CENTRO ORIENTE | PUEBLA | PUEBLA |
| CENTRO SUR | MORELOS | CUERNAVACA |
| GOLFO CENTRO | TAMAULIPAS | TAMPICO |
| GOLFO NORTE | NUEVO LEÓN | MONTERREY |
| JALISCO | JALISCO | GUADALAJARA |
| NOROESTE | SONORA | HERMOSILLO |
| ORIENTE | VERACRUZ | VERACRUZ |
| PENINSULAR | YUCATÁN | MERIDA |
| SURESTE | TABASCO | BALANCAN |
| VALLE DE MÉXICO CENTRO | CIUDAD DE MÉXICO | CUAUHTEMOC |
| VALLE DE MÉXICO NORTE | ESTADO DE MÉXICO | ECATEPEC DE MORELOS |
| VALLE DE MÉXICO SUR | ESTADO DE MÉXICO | CHALCO |

## Lógica del Scraper

### Flujo principal

1. Para cada combinación (año, mes, estado, municipio):
   - Hacer request a la página de CFE
   - Manejar el ViewState de ASP.NET (POST con __VIEWSTATE, __EVENTVALIDATION, etc.)
   - Seleccionar año → mes → estado → municipio secuencialmente
   - Parsear la tabla HTML de resultados
   - Extraer los 4 cargos con sus valores
   - Para Variable (Energía): extraer los 3 valores por intervalo horario (Base, Intermedia, Punta)
2. Generar las filas en el formato del CSV
3. Appendear al CSV existente (no sobrescribir datos históricos)

### Manejo de errores

- Si CFE aún no publica el mes actual, loguear y no crashear
- Retry con backoff exponencial si hay timeout
- Validar que los valores extraídos son numéricos
- Detectar si ya existen datos para ese mes/división y no duplicar

## Archivos del Proyecto

```
cfe-tarifas-automation/
├── CLAUDE.md              # Este archivo
├── README.md              # Documentación del proyecto
├── src/
│   ├── scraper.py         # Scraper principal de CFE
│   ├── parser.py          # Parsing de tablas HTML de CFE
│   └── csv_manager.py     # Lectura/escritura/validación del CSV
├── data/
│   └── tarifas_gdmth.csv  # CSV de salida (fuente para Power BI)
├── config/
│   └── divisiones.json    # Mapping divisiones → estado → municipio
├── tests/
│   └── test_scraper.py    # Tests
├── requirements.txt       # Dependencias Python
└── .gitignore
```

## Stack Técnico

- **Python 3.11+**
- **requests** + **BeautifulSoup4** para scraping (intentar primero sin Selenium)
- **Selenium** como fallback si ASP.NET ViewState es demasiado complejo
- **pandas** para manejo de CSV
- **schedule** o **cron** para automatización

## Integración con Power BI

### Migración de tabla manual a CSV externo (una sola vez)

El objetivo es reemplazar la fuente de datos de la tabla de tarifas SIN romper relaciones, medidas DAX ni visuales existentes. Esto funciona porque Power BI conecta los visuales al output de Power Query, no directamente al archivo fuente. Mientras las columnas y tipos de datos sean idénticos, todo se mantiene.

**Pasos:**

1. En Power BI Desktop → **Transform Data** (Power Query Editor)
2. Seleccionar la tabla de tarifas actual
3. Click en **Advanced Editor**
4. Reemplazar todo el M code con:

```m
let
    Source = Csv.Document(File.Contents("C:\ruta\al\proyecto\cfe-tarifas-automation\data\tarifas_gdmth.csv"), [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.None]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers", {
        {"division", type text},
        {"estado", type text},
        {"municipio", type text},
        {"anio", Int64.Type},
        {"mes", Int64.Type},
        {"intervalo_horario", type text},
        {"cargo", type text},
        {"valor", type number},
        {"Fecha", type text}
    })
in
    #"Changed Type"
```

5. Click **Done** → **Close & Apply**
6. Verificar que relaciones y visuales siguen funcionando

**IMPORTANTE:** La ruta del CSV debe ser absoluta. Ajustar `C:\ruta\al\proyecto\` a la ubicación real en la máquina donde corre Power BI.

### Actualización mensual

Después de la migración, el flujo es:
1. El scraper corre y actualiza `data/tarifas_gdmth.csv`
2. En Power BI → click **Refresh**
3. Los datos nuevos se cargan automáticamente

## Automatización (Fase 2)

Opciones para correr el scraper automáticamente cada mes:

- **Claude Code /loop**: `/loop 30d python src/scraper.py --month current`
- **Cowork scheduled task**: Tarea mensual el día 5 de cada mes
- **Cron local**: `0 9 5 * * cd /path/to/project && python src/scraper.py`
- **GitHub Actions**: Schedule trigger mensual

## Notas Importantes

- Las tarifas de CFE son públicas y de acceso libre
- El scraper debe respetar rate limits razonables (1-2 segundos entre requests)
- Los datos históricos empiezan desde enero 2018
- Power BI actualmente tiene los datos cargados como tabla manual — migrar a CSV externo es un cambio de una sola vez
