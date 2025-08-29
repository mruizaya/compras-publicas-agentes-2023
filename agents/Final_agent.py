from agents import Agent
from openai import OpenAI

client = OpenAI()  # no need to pass api_key explicitly

def run_agent(agent, user_input):
    """
    Executes a local Agent by sending instructions + input to the model.
    """
    prompt = f"{agent.instructions}\n\nInput: {user_input}"
    response = client.chat.completions.create(
        model=agent.model,
        messages=[{"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content

# -------------------------
# 1️⃣ Ecuador Agent
# -------------------------
ecuador_agent = Agent(
    name="Ecuador Agent",
    instructions=(
        "Consulta la API de Ecuador y devuelve únicamente los procesos de 2023 "
        "de modalidad 'subasta inversa'. Normaliza los resultados en un JSON con los campos: "
        "id, entidad, objeto, presupuesto, moneda, lugar, fecha_conv, fecha_adj, "
        "oferentes, proveedor, valor_adj, justificacion."
    ),
    model="gpt-4o-mini",
    tools=[
        {
            "type": "function",
            "function": {
                "name": "query_ecuador_api",
                "description": "Consulta la API de Ecuador para obtener procesos de contratación.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "year": {"type": "integer", "default": 2023},
                        "method": {"type": "string", "default": "subasta inversa"},
                        "api_url": {"type": "string", "default": "<https://datosabiertos.compraspublicas.gob.ec/PLATAFORMA/api/search_ocds>"}
                    },
                    "required": ["year", "method", "api_url"]
                }
            }
        }
    ]
)

# -------------------------
# 2️⃣ Colombia Agent
# -------------------------
colombia_agent = Agent(
    name="Colombia Agent",
    instructions=(
        "Consulta la API de Colombia y devuelve únicamente los procesos de 2023 "
        "de modalidad 'subasta inversa'. Normaliza los resultados en un JSON con los campos: "
        "id, entidad, objeto, presupuesto, moneda, lugar, fecha_conv, fecha_adj, "
        "oferentes, proveedor, valor_adj, justificacion."
    ),
    model="gpt-4o-mini",
    tools=[
        {
            "type": "function",
            "function": {
                "name": "query_colombia_api",
                "description": "Consulta la API de Colombia para obtener procesos de contratación.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "year": {"type": "integer", "default": 2023},
                        "method": {"type": "string", "default": "subasta inversa"},
                        "api_url": {"type": "string", "default": "<https://www.datos.gov.co/api/v3/views/p6dx-8zbt/query.json?query=SELECT%0A%20%20%60entidad%60%2C%0A%20%20%60nit_entidad%60%2C%0A%20%20%60departamento_entidad%60%2C%0A%20%20%60ciudad_entidad%60%2C%0A%20%20%60ordenentidad%60%2C%0A%20%20%60codigo_pci%60%2C%0A%20%20%60id_del_proceso%60%2C%0A%20%20%60referencia_del_proceso%60%2C%0A%20%20%60ppi%60%2C%0A%20%20%60id_del_portafolio%60%2C%0A%20%20%60nombre_del_procedimiento%60%2C%0A%20%20%60descripci_n_del_procedimiento%60%2C%0A%20%20%60fase%60%2C%0A%20%20%60fecha_de_publicacion_del%60%2C%0A%20%20%60fecha_de_ultima_publicaci%60%2C%0A%20%20%60fecha_de_publicacion_fase%60%2C%0A%20%20%60fecha_de_publicacion_fase_1%60%2C%0A%20%20%60fecha_de_publicacion%60%2C%0A%20%20%60fecha_de_publicacion_fase_2%60%2C%0A%20%20%60fecha_de_publicacion_fase_3%60%2C%0A%20%20%60precio_base%60%2C%0A%20%20%60modalidad_de_contratacion%60%2C%0A%20%20%60justificaci_n_modalidad_de%60%2C%0A%20%20%60duracion%60%2C%0A%20%20%60unidad_de_duracion%60%2C%0A%20%20%60fecha_de_recepcion_de%60%2C%0A%20%20%60fecha_de_apertura_de_respuesta%60%2C%0A%20%20%60fecha_de_apertura_efectiva%60%2C%0A%20%20%60ciudad_de_la_unidad_de%60%2C%0A%20%20%60nombre_de_la_unidad_de%60%2C%0A%20%20%60proveedores_invitados%60%2C%0A%20%20%60proveedores_con_invitacion%60%2C%0A%20%20%60visualizaciones_del%60%2C%0A%20%20%60proveedores_que_manifestaron%60%2C%0A%20%20%60respuestas_al_procedimiento%60%2C%0A%20%20%60respuestas_externas%60%2C%0A%20%20%60conteo_de_respuestas_a_ofertas%60%2C%0A%20%20%60proveedores_unicos_con%60%2C%0A%20%20%60numero_de_lotes%60%2C%0A%20%20%60estado_del_procedimiento%60%2C%0A%20%20%60id_estado_del_procedimiento%60%2C%0A%20%20%60adjudicado%60%2C%0A%20%20%60id_adjudicacion%60%2C%0A%20%20%60codigoproveedor%60%2C%0A%20%20%60departamento_proveedor%60%2C%0A%20%20%60ciudad_proveedor%60%2C%0A%20%20%60fecha_adjudicacion%60%2C%0A%20%20%60valor_total_adjudicacion%60%2C%0A%20%20%60nombre_del_adjudicador%60%2C%0A%20%20%60nombre_del_proveedor%60%2C%0A%20%20%60nit_del_proveedor_adjudicado%60%2C%0A%20%20%60codigo_principal_de_categoria%60%2C%0A%20%20%60estado_de_apertura_del_proceso%60%2C%0A%20%20%60tipo_de_contrato%60%2C%0A%20%20%60subtipo_de_contrato%60%2C%0A%20%20%60categorias_adicionales%60%2C%0A%20%20%60urlproceso%60%2C%0A%20%20%60codigo_entidad%60%2C%0A%20%20%60estado_resumen%60%0AWHERE%0A%20%20%60fecha_de_publicacion_del%60%0A%20%20%20%20BETWEEN%20%222023-01-01T02%3A58%3A19%22%20%3A%3A%20floating_timestamp%0A%20%20%20%20AND%20%222025-12-31T02%3A58%3A19%22%20%3A%3A%20floating_timestamp%0ASEARCH%20%22subasta%20inversa%22>"}
                    },
                    "required": ["year", "method", "api_url"]
                }
            }
        }
    ]
)

# -------------------------
# 3️⃣ Chile Agent
# -------------------------
chile_agent = Agent(
    name="Chile Agent",
    instructions=(
        "Consulta la API de Chile y devuelve únicamente los procesos de 2023 "
        "de modalidad 'subasta inversa'. Normaliza los resultados en un JSON con los campos: "
        "id, entidad, objeto, presupuesto, moneda, lugar, fecha_conv, fecha_adj, "
        "oferentes, proveedor, valor_adj, justificacion."
    ),
    model="gpt-4o-mini",
    tools=[
        {
            "type": "function",
            "function": {
                "name": "query_chile_api",
                "description": "Consulta la API de Chile (Mercado Público) para obtener procesos de contratación.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "year": {"type": "integer", "default": 2023},
                        "method": {"type": "string", "default": "subasta inversa"},
                        "api_url": {"type": "string", "default": "<https://api.mercadopublico.cl/APISOCDS/Utilidades/Politicas/Actualizacion>"}
                    },
                    "required": ["year", "method", "api_url"]
                }
            }
        }
    ]
)

# -------------------------
# 4️⃣ Router Agent
# -------------------------
router_agent = Agent(
    name="Router Agent",
    instructions=(
        "Recibe una consulta del usuario sobre contrataciones públicas. "
        "Decide a qué país enviar la consulta (Ecuador, Colombia, Chile) "
        "y hace handoff al agente correspondiente. "
        "Recopila los resultados normalizados de los agentes de país y devuelve JSON combinado."
    ),
    model="gpt-4o-mini",
    tools=[
        # In your implementation, you can call the country agents inside this router
    ]
)

# -------------------------
# 5️⃣ Analysis Agent
# -------------------------
analysis_agent = Agent(
    name="Analysis Agent",
    instructions=(
        "Recibe los datos normalizados de Ecuador, Colombia y Chile. "
        "Cree un informe elemental del total en dólares del presupuesto en las categorías amplias: "
        "Salud, Educación, Infraestructura. "
        "Además, haga un comparativo entre el gasto de los tres países."
    ),
    model="gpt-4o-mini"
)

# -------------------------
# 6️⃣ Example workflow
# -------------------------
user_query = "Genera el informe de subastas inversas de 2023 para los tres países"

# Step 1: router decides which agents to call and gets normalized data
normalized_data = run_agent(router_agent, user_query)

# Step 2: analysis agent produces final report
final_report = run_agent(analysis_agent, normalized_data)

print(final_report)