from agents import Agent
from openai import OpenAI
import requests
import json
import time
from collections import Counter, defaultdict
from utils_ecuador import fetch_all_search

client = OpenAI()

# --------------------------
# Config
# --------------------------
MAX_EC_ROWS = 200  # límite de registros a traer de Ecuador (search_ocds)

# --------------------------
# Funciones reales
# --------------------------

def convertir_a_usd(valor, moneda):
    tasas = {"USD": 1, "COP": 0.00025, "CLP": 0.0012}
    try:
        return float(valor) * tasas.get((moneda or "USD"), 1)
    except:
        return 0.0

def clasificar_categoria(texto):
    """Clasificador con vocabulario ampliado (usa objeto + entidad)."""
    t = (texto or "").lower()

    salud_kw = [
        "salud","hospital","clínica","clinica","médic","medic","quirúrg","quirurg","insumo","medicament",
        "bioméd","biomed","odont","laboratorio","ambulancia","equipos médicos","suministro médico"
    ]
    educ_kw = [
        "educación","educacion","escuela","colegio","universidad","docente","estudiante","alumno",
        "sena","institución educativa","institucion educativa","mobiliario escolar","textos escolares","distrital"
    ]
    infra_kw = [
        "infraestructura","obra","obras","vía","via","carretera","puente","calzada",
        "rehabilitación","rehabilitacion","mantenimiento vial","alcantarillado","agua potable",
        "pavimentación","pavimentacion","vial","edificación","edificacion"
    ]

    if any(k in t for k in salud_kw): return "Salud"
    if any(k in t for k in educ_kw):  return "Educación"
    if any(k in t for k in infra_kw): return "Infraestructura"
    return "Otras"

def query_ecuador_api(year, method):
    print("📡 Consultando datos de Ecuador via search_ocds...")
    try:
        # 👇 pasamos el límite de 200
        data = fetch_all_search(year=year, search=method, max_rows=MAX_EC_ROWS)
        print(f"✅ {len(data)} registros obtenidos de Ecuador.")
        return data
    except Exception as e:
        print("❌ Error al consultar Ecuador:", e)
        return []

def query_colombia_api(year, method, api_url):
    print("📡 Consultando datos de Colombia...")
    r = requests.get("https://www.datos.gov.co/resource/p6dx-8zbt.json", params={
        "$limit": 10000,
        "$where": "modalidad_de_contratacion like '%subasta inversa%' AND fecha_de_publicacion_del >= '2023-01-01T00:00:00'"
    })
    if r.status_code != 200:
        return []
    data = r.json()
    return [
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

def query_chile_api(year, method, api_url):
    print("🧪 Simulando datos de Chile con 2 registros de prueba...")
    return [
        {
            "pais": "Chile",
            "id": "ocds-chile-001",
            "entidad": "Ministerio de Salud",
            "objeto": "Compra de equipos médicos para hospitales regionales",
            "presupuesto": 100000000,
            "moneda": "CLP",
            "lugar": "Región Metropolitana",
            "fecha_conv": "2023-05-10",
            "fecha_adj": "2023-06-15",
            "oferentes": 5,
            "proveedor": "MedTec S.A.",
            "valor_adj": 95000000,
            "justificacion": "Mejor oferta técnica y económica"
        },
        {
            "pais": "Chile",
            "id": "ocds-chile-002",
            "entidad": "Ministerio de Obras Públicas",
            "objeto": "Rehabilitación de puente en ruta nacional",
            "presupuesto": 250000000,
            "moneda": "CLP",
            "lugar": "Valparaíso",
            "fecha_conv": "2023-08-12",
            "fecha_adj": "2023-09-18",
            "oferentes": 3,
            "proveedor": "Constructora Andes Ltda.",
            "valor_adj": 240000000,
            "justificacion": "Subasta inversa exitosa"
        }
    ]

# --------------------------
# Ejecutar Router
# --------------------------
def ejecutar_router():
    datos = []
    datos += query_ecuador_api(2023, "subasta inversa")
    datos += query_colombia_api(2023, "subasta inversa", "")
    datos += query_chile_api(2023, "subasta inversa", "")

    # Normalización final + conversión + clasificado + fallbacks
    for d in datos:
        # Moneda por defecto para Ecuador si faltara
        if d.get("pais") == "Ecuador" and not d.get("moneda"):
            d["moneda"] = "USD"

        # Fallback de presupuesto: si no hay presupuesto pero sí hay valor adjudicado, úsalo
        presupuesto = d.get("presupuesto")
        if not presupuesto or float(presupuesto or 0) == 0:
            if d.get("valor_adj"):
                presupuesto = d.get("valor_adj")
        d["presupuesto"] = presupuesto or 0

        # Clasificación usando objeto + entidad
        texto_clasif = f"{d.get('objeto','')} {d.get('entidad','')}"
        d["categoria"] = clasificar_categoria(texto_clasif)

        # Conversión a USD
        d["presupuesto_usd"] = convertir_a_usd(d.get("presupuesto", 0), d.get("moneda", "USD"))
        d["valor_adj_usd"] = convertir_a_usd(d.get("valor_adj", 0), d.get("moneda", "USD"))

        # Si presupuesto_usd quedó 0 pero valor_adj_usd > 0, usarlo como aproximación
        if (d["presupuesto_usd"] == 0 or d["presupuesto_usd"] is None) and d.get("valor_adj_usd"):
            d["presupuesto_usd"] = d["valor_adj_usd"]

    return datos

# --------------------------
# Ejecutar agente
# --------------------------
def run_agent(agent, user_input):
    prompt = f"{agent.instructions}\n\nInput: {user_input}"
    response = client.chat.completions.create(
        model=agent.model,
        messages=[{"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content

# --------------------------
# Agentes
# --------------------------
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

# --------------------------
# Flujo principal
# --------------------------
if __name__ == "__main__":
    print("📦 Ejecutando Router Agent (recolección de datos)...")
    normalized_data = ejecutar_router()

    with open("datos_normalizados.json", "w", encoding="utf-8") as f:
        json.dump(normalized_data, f, indent=2, ensure_ascii=False)

    print("✅ Datos guardados en datos_normalizados.json")

    print("\n✔️ Países detectados en los datos:")
    conteo_paises = Counter(d["pais"] for d in normalized_data)
    for pais, cantidad in conteo_paises.items():
        print(f"   - {pais}: {cantidad} registros")

    # --- 1) Totales locales (USD) por país y categoría ---
    def compute_totals(data):
        objetivos = {"Salud", "Educación", "Infraestructura"}
        totals = defaultdict(lambda: defaultdict(float))
        for d in data:
            pais = d.get("pais", "Desconocido")
            cat = d.get("categoria", "Otras")
            if cat in objetivos:
                totals[pais][cat] += float(d.get("presupuesto_usd") or 0)
        return totals

    totales = compute_totals(normalized_data)

    print("\n📊 Totales locales (USD) por país y categoría:")
    for pais in sorted(totales.keys()):
        print(f" - {pais}: ", {cat: round(val, 2) for cat, val in totales[pais].items()})

    # --- 2) Muestra estratificada para GPT (incluye todos los países) ---
    def stratified_sample(data, k_per_country=120):
        by_country = defaultdict(list)
        for d in data:
            by_country[d.get("pais", "Desconocido")].append(d)
        sampled = []
        for pais, filas in by_country.items():
            sampled.extend(filas[:k_per_country])  # hasta k por país
        return sampled

    muestra_para_gpt = stratified_sample(normalized_data, k_per_country=120)

    payload_gpt = {
        "totales_usd": {
            pais: {cat: round(val, 2) for cat, val in cats.items()}
            for pais, cats in totales.items()
        },
        "muestra": muestra_para_gpt[:300]  # seguridad: límite 300 items
    }

    print("\n📤 Enviando resumen + muestra estratificada al Analysis Agent...")
    input_text = json.dumps(payload_gpt, ensure_ascii=False)

    # Ajusta las instrucciones del Analysis Agent para que use 'totales_usd' como fuente de verdad
    analysis_agent.instructions = (
        "Recibes un JSON con 'totales_usd' (totales de presupuesto en USD por país y por categoría: "
        "Salud, Educación, Infraestructura) y una 'muestra' representativa de contratos normalizados. "
        "Usa 'totales_usd' como fuente de verdad para los números y redacta un informe comparativo claro "
        "entre Ecuador, Colombia y Chile. Señala brevemente patrones y diferencias. "
        "Si falta una categoría en algún país, menciónalo como 0."
    )

    final_report = run_agent(analysis_agent, input_text)

    print("\n📄 Informe generado por GPT:\n")
    print(final_report)
