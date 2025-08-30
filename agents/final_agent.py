from agents import Agent
from openai import OpenAI
import requests
import json
import tarfile
import csv
import re
import os
from pathlib import Path
from collections import Counter, defaultdict
from utils_ecuador import fetch_all_search

client = OpenAI()

# ========================
# Config / Paths
# ========================
MAX_EC_ROWS = 200
DATA_DIR = Path("./data")
DATA_DIR.mkdir(exist_ok=True)

SCHEMA = "id, entidad, objeto, presupuesto, moneda, lugar, fecha_conv, fecha_adj, oferentes, proveedor, valor_adj, justificacion"

# ========================
# Utils de parsing / limpieza
# ========================

def strip_code_fences(text: str) -> str:
    if text is None:
        return ""
    text = text.strip()
    text = re.sub(r"^```[a-zA-Z0-9]*\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    return text.strip()

def to_number(val) -> float:
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)

    s = str(val).strip()
    if s == "":
        return 0.0

    repl = ["USD", "US$", "$", "CLP", "COP", "UF", "CLF", "FET"]
    s_up = s.upper()
    for token in repl:
        s_up = s_up.replace(token, "")
    s = s_up

    s = s.replace("\xa0", " ").strip()

    if s.count(".") > 0 and s.count(",") > 0:
        s = s.replace(".", "")
        s = s.replace(",", ".")
    else:
        if s.count(",") == 1 and len(s.split(",")[-1]) in (1, 2):
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")

    s = s.replace(" ", "")

    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]

    s = re.sub(r"[^0-9\.\-]", "", s)

    try:
        return float(s) if s not in ("", ".", "-", "-.") else 0.0
    except Exception:
        return 0.0

def convertir_a_usd(valor, moneda):
    """
    Variables de entorno para ajustar tasas:
      FX_COP_PER_USD (default 4000)
      FX_CLP_PER_USD (default 850)
      FX_UF_USD      (default 42)
    """
    v = to_number(valor)
    m = (moneda or "USD").strip().upper()

    cop_per_usd = to_number(os.getenv("FX_COP_PER_USD", "4000"))
    clp_per_usd = to_number(os.getenv("FX_CLP_PER_USD", "850"))
    uf_usd      = to_number(os.getenv("FX_UF_USD", "42"))

    rates = {
        "USD": 1.0,
        "COP": (1.0 / cop_per_usd) if cop_per_usd > 0 else 0.00025,
        "CLP": (1.0 / clp_per_usd) if clp_per_usd > 0 else 1/850.0,
        "UF": uf_usd if uf_usd > 0 else 42.0,
        "CLF": uf_usd if uf_usd > 0 else 42.0,
        "FET": uf_usd if uf_usd > 0 else 42.0,
        "": 1.0,
        None: 1.0,
    }
    return float(v) * rates.get(m, 1.0)

# ===== Nuevo clasificador (usa objeto + entidad + justificación, con reglas CHI) =====
def clasificar_categoria_avanzado(objeto: str, entidad: str, justificacion: str = "") -> str:
    t = f"{(objeto or '')} {(entidad or '')} {(justificacion or '')}".lower()

    # Salud (palabras en objeto/justificación o por tipo de entidad)
    salud_kw = [
        "salud", "hospital", "clínic", "clinic", "médic", "medic", "quirúrg", "quirurg",
        "insumo", "medicament", "laboratorio", "ambulancia", "esteriliz", "equipos médicos",
        "servicio de salud", "cesfam", "sar ", "sapu"
    ]
    # Educación
    educ_kw = [
        "educación", "educacion", "escuela", "colegio", "universidad", "liceo", "docente",
        "estudiante", "alumno", "junji", "jardín infantil", "jardin infantil", "daem",
        "institución educativa", "institucion educativa"
    ]
    # Infraestructura / obras / TI/telecom/seguro/equipamiento general
    infra_kw = [
        "infraestructura", "obra", "obras", "vía", "via", "carretera", "puente", "calzada",
        "rehabilitación", "rehabilitacion", "mantenimiento vial", "alcantarillado", "agua potable",
        "paviment", "vial", "edificaci", "servidor", "impresor", "telefon", "hpe", "seguro",
        "mantención", "mantencion", "licitación obra", "construcción", "conservación"
    ]

    # Heurísticas por entidad CHI
    entidad_health = any(x in (entidad or "").lower() for x in [
        "servicio de salud", "hospital", "cesfam", "sapu", "sar ", "posta", "ss metropolitan"
    ])
    entidad_educ = any(x in (entidad or "").lower() for x in [
        "junji", "jardín infantil", "jardin infantil", "colegio", "liceo", "daem",
        "ministerio de educación", "universidad", "municipalidad de"  # muchas muni compran p/escuelas
    ])
    entidad_infra = any(x in (entidad or "").lower() for x in [
        "mop", "obras públicas", "obras publicas", "dirección de vialidad", "direccion de vialidad",
        "serviu", "ministerio de obras", "municipalidad de", "dom"
    ])

    if any(k in t for k in salud_kw) or entidad_health:
        return "Salud"
    if any(k in t for k in educ_kw) or entidad_educ:
        return "Educación"
    if any(k in t for k in infra_kw) or entidad_infra:
        return "Infraestructura"
    return "Otras"

# ========================
# Chile (descarga, extracción, normalización por chunks, lectura)
# ========================

def download_and_extract_chile(url: str) -> Path:
    tar_path = DATA_DIR / "chile.tar.gz"
    print(f"[Chile] Descargando desde {url} ...")
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    with open(tar_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)

    print(f"[Chile] Archivo comprimido guardado en {tar_path}")

    csv_output = None
    with tarfile.open(tar_path, "r:gz") as tar:
        for member in tar.getmembers():
            if member.name.endswith("contracts.csv"):
                print(f"[Chile] Extrayendo {member.name} ...")
                try:
                    tar.extract(member, DATA_DIR, filter="data")  # Python 3.14+
                except TypeError:
                    tar.extract(member, DATA_DIR)
                csv_output = DATA_DIR / member.name
                break

    if csv_output is None:
        raise FileNotFoundError("[Chile] No se encontró un contracts.csv en el tar.gz")

    print(f"[Chile] CSV extraído en {csv_output}")
    return csv_output

def normalize_chile_chunked(file_path: Path, lines_per_chunk: int = 1800, max_chunks: int = 6) -> Path:
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        all_lines = f.readlines()

    if not all_lines:
        out_file = DATA_DIR / "chile_normalized.csv"
        with open(out_file, "w", encoding="utf-8") as fout:
            fout.write(SCHEMA + "\n")
        print(f"[Chile] Archivo vacío, creado CSV normalizado vacío → {out_file}")
        return out_file

    header = all_lines[0]
    body = all_lines[1:]

    chunks = []
    for i in range(0, len(body), lines_per_chunk):
        if len(chunks) >= max_chunks:
            break
        chunk_lines = [header] + body[i:i + lines_per_chunk]
        chunks.append("".join(chunk_lines))

    out_file = DATA_DIR / "chile_normalized.csv"
    wrote_header = False

    for idx, chunk_text in enumerate(chunks, start=1):
        prompt = f"""
Eres un agente especializado en compras públicas.
País: Chile
Este es un fragmento del archivo CSV oficial (incluye cabecera).
Convierte EXCLUSIVAMENTE las filas de este fragmento al siguiente esquema CSV EXACTO:

{SCHEMA}

Reglas estrictas:
- Usa coma (,) como separador (no uses ';').
- No repitas ni inventes filas.
- No agregues texto fuera del CSV.
- No envuelvas en bloques de código.
- Si un valor no existe, deja la celda vacía.
- En el PRIMER chunk incluye la cabecera; en los siguientes, NO incluyas cabecera.

Fragmento CSV:
{chunk_text[:180000]}
""".strip()

        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
        )
        csv_out = strip_code_fences(resp.choices[0].message.content or "")
        lines = [ln for ln in csv_out.splitlines() if ln.strip() != ""]

        if not wrote_header:
            first = (lines[0].strip().lower() if lines else "")
            if not first.startswith("id,"):
                lines.insert(0, SCHEMA)
            wrote_header = True
            mode = "w"
        else:
            if lines and lines[0].strip().lower().startswith("id,"):
                lines = lines[1:]
            mode = "a"

        with open(out_file, mode, encoding="utf-8") as fout:
            fout.write("\n".join(lines) + "\n")

    print(f"[Chile] Normalización por chunks completada → {out_file}")
    return out_file

def _dictreader_autodelim(fp):
    sample = fp.read(4096)
    fp.seek(0)
    dialect = None
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";"])
    except Exception:
        pass
    if dialect is None:
        first_try = csv.DictReader(fp, delimiter=";")
        rows = []
        try:
            for i, r in enumerate(first_try):
                rows.append(r)
                if i > 3:
                    break
        except Exception:
            rows = []
        fp.seek(0)
        if rows and len(rows[0].keys()) > 1:
            return csv.DictReader(fp, delimiter=";")
        else:
            fp.seek(0)
            return csv.DictReader(fp, delimiter=",")
    else:
        return csv.DictReader(fp, dialect=dialect)

def query_chile_data():
    try:
        chile_url = "https://data.open-contracting.org/es/publication/144/download?name=2023.csv.tar.gz"
        extracted_path = download_and_extract_chile(chile_url)
        normalized_path = normalize_chile_chunked(extracted_path, lines_per_chunk=1800, max_chunks=6)

        datos = []
        with open(normalized_path, "r", encoding="utf-8", errors="ignore", newline="") as f:
            reader = _dictreader_autodelim(f)
            for row in reader:
                if not row:
                    continue
                rid = (row.get("id") or "").strip()
                if not rid or rid.lower() == "id":
                    continue
                datos.append({
                    "id": rid,
                    "entidad": (row.get("entidad") or "").strip(),
                    "objeto": (row.get("objeto") or "").strip(),
                    "presupuesto": (row.get("presupuesto") or "").strip(),
                    "moneda": ((row.get("moneda") or "CLP").strip()).upper(),
                    "lugar": (row.get("lugar") or "").strip(),
                    "fecha_conv": (row.get("fecha_conv") or "").strip(),
                    "fecha_adj": (row.get("fecha_adj") or "").strip(),
                    "oferentes": (row.get("oferentes") or "").strip(),
                    "proveedor": (row.get("proveedor") or "").strip(),
                    "valor_adj": (row.get("valor_adj") or "").strip(),
                    "justificacion": (row.get("justificacion") or "").strip(),
                    "pais": "Chile",
                })

        print(f"✅ Chile: {len(datos)} registros normalizados por chunks.")
        return datos
    except Exception as e:
        print("❌ Error Chile:", e)
        return []

# ========================
# Ecuador / Colombia
# ========================

def query_ecuador_api(year, method):
    print("📡 Consultando datos de Ecuador...")
    try:
        data = fetch_all_search(year=year, search=method, max_rows=MAX_EC_ROWS)
        print(f"✅ {len(data)} registros obtenidos de Ecuador.")
        for d in data:
            d.setdefault("pais", "Ecuador")
        return data
    except Exception as e:
        print("❌ Error Ecuador:", e)
        return []

def query_colombia_api(year, method, api_url):
    print("📡 Consultando datos de Colombia...")
    r = requests.get("https://www.datos.gov.co/resource/p6dx-8zbt.json", params={
        "$limit": 10000,
        "$where": "modalidad_de_contratacion like '%subasta inversa%' AND fecha_de_publicacion_del >= '2023-01-01T00:00:00'"
    })
    if r.status_code != 200:
        print("❌ Error Colombia:", r.status_code)
        return []
    data = r.json()
    out = [
        {
            "pais": "Colombia",
            "id": d.get("id_del_proceso"),
            "entidad": d.get("entidad"),
            "objeto": d.get("descripci_n_del_procedimiento"),
            "presupuesto": d.get("precio_base", 0),
            "moneda": "COP",
            "lugar": d.get("ciudad_entidad"),
            "fecha_conv": d.get("fecha_de_publicacion_del"),
            "fecha_adj": d.get("fecha_adjudicacion"),
            "oferentes": d.get("proveedores_invitados"),
            "proveedor": d.get("nombre_del_proveedor"),
            "valor_adj": d.get("valor_total_adjudicacion", 0),
            "justificacion": d.get("justificaci_n_modalidad_de")
        }
        for d in data
    ]
    print(f"✅ {len(out)} registros obtenidos de Colombia.")
    return out

# ========================
# Router + normalización final
# ========================

def ejecutar_router():
    datos = []
    datos += query_ecuador_api(2023, "subasta inversa")
    datos += query_colombia_api(2023, "subasta inversa", "")
    datos += query_chile_data()

    for d in datos:
        # Moneda por defecto si faltara
        if d.get("pais") == "Ecuador" and not d.get("moneda"):
            d["moneda"] = "USD"
        if d.get("pais") == "Chile" and not d.get("moneda"):
            d["moneda"] = "CLP"

        # Fallback de presupuesto
        presupuesto = to_number(d.get("presupuesto"))
        if presupuesto <= 0:
            presupuesto = to_number(d.get("valor_adj"))
        d["presupuesto"] = presupuesto or 0.0

        # Clasificación (usa objeto + entidad + justificación)
        d["categoria"] = clasificar_categoria_avanzado(
            d.get("objeto", ""),
            d.get("entidad", ""),
            d.get("justificacion", "")
        )

        # Conversión a USD
        d["presupuesto_usd"] = convertir_a_usd(d.get("presupuesto", 0), d.get("moneda", "USD"))
        d["valor_adj_usd"] = convertir_a_usd(d.get("valor_adj", 0), d.get("moneda", "USD"))

        # Si presupuesto_usd quedó 0 pero valor_adj_usd > 0, usarlo como aproximación
        if (not d["presupuesto_usd"]) and d.get("valor_adj_usd"):
            d["presupuesto_usd"] = d["valor_adj_usd"]

    return datos

# ========================
# Agentes / Runner
# ========================

def run_agent(agent, user_input):
    prompt = f"{agent.instructions}\n\nInput: {user_input}"
    response = client.chat.completions.create(
        model=agent.model,
        messages=[{"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content

# Agentes
ecuador_agent = Agent(name="Ecuador Agent", instructions="...", model="gpt-4o-mini", tools=[])
colombia_agent = Agent(name="Colombia Agent", instructions="...", model="gpt-4o-mini", tools=[])
chile_agent = Agent(name="Chile Agent", instructions="...", model="gpt-4o-mini", tools=[])

router_agent = Agent(
    name="Router Agent",
    instructions="Recoge y normaliza los datos de Ecuador, Colombia y Chile.",
    model="gpt-4o-mini"
)

analysis_agent = Agent(
    name="Analysis Agent",
    instructions=(
        "Recibes una lista de contratos de subasta inversa normalizados (con país, categoría y presupuesto_usd). "
        "Calcula el total de presupuesto en USD para las categorías: Salud, Educación e Infraestructura por país. "
        "Haz un análisis comparativo en forma de informe."
    ),
    model="gpt-4o"
)

# ========================
# Main
# ========================
if __name__ == "__main__":
    print("📦 Ejecutando Router Agent (recolección de datos)...")
    normalized_data = ejecutar_router()

    with open("datos_normalizados.json", "w", encoding="utf-8") as f:
        json.dump(normalized_data, f, indent=2, ensure_ascii=False)
    print("✅ Datos guardados en datos_normalizados.json")

    print("\n✔️ Países detectados:")
    conteo_paises = Counter(d["pais"] for d in normalized_data)
    for pais, cantidad in conteo_paises.items():
        print(f" - {pais}: {cantidad} registros")

    # --- 1) Totales locales (USD) por país y categoría ---
    def compute_totals(data):
        objetivos = {"Salud", "Educación", "Infraestructura"}
        totals = defaultdict(lambda: defaultdict(float))
        for d in data:
            pais = d.get("pais", "Desconocido")
            cat = d.get("categoria", "Otras")
            amt = float(d.get("presupuesto_usd") or 0)
            if amt == 0:
                amt = float(d.get("valor_adj_usd") or 0)  # safety net
            if cat in objetivos:
                totals[pais][cat] += amt
        # Asegura países/categorías aunque estén en 0
        for p in ["Ecuador", "Colombia", "Chile"]:
            for c in ["Salud", "Educación", "Infraestructura"]:
                _ = totals[p][c]
        return totals

    totales = compute_totals(normalized_data)

    print("\n📊 Totales por país y categoría:")
    for pais in sorted(totales.keys()):
        print(f" - {pais}: ", {cat: round(val, 2) for cat, val in totales[pais].items()})

    # --- 2) Muestra estratificada para GPT ---
    def stratified_sample(data, k_per_country=120):
        by_country = defaultdict(list)
        for d in data:
            by_country[d.get("pais", "Desconocido")].append(d)
        sampled = []
        for pais, filas in by_country.items():
            sampled.extend(filas[:k_per_country])
        return sampled

    muestra_para_gpt = stratified_sample(normalized_data, k_per_country=120)

    payload_gpt = {
        "totales_usd": {
            pais: {cat: round(val, 2) for cat, val in cats.items()}
            for pais, cats in totales.items()
        },
        "muestra": muestra_para_gpt[:300]
    }

    print("\n📤 Enviando datos al Analysis Agent...")
    input_text = json.dumps(payload_gpt, ensure_ascii=False)

    final_report = run_agent(analysis_agent, input_text)

    print("\n📄 Informe generado por GPT:\n")
    print(final_report)

