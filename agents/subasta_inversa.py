
from __future__ import annotations
import asyncio, os, requests
from typing import Dict, Any, List, Optional

from agents import Agent, Runner, function_tool
from agents.extensions.models.litellm_model import LitellmModel

SEARCH_URL = "https://datosabiertos.compraspublicas.gob.ec/PLATAFORMA/api/search_ocds"

# ---- helpers planos (sin anidación) ----
def get_multi(d: Dict[str, Any], keys: List[str], default=None):
    for k in keys:
        if k in d and d[k] not in (None, "", [], {}):
            return d[k]
    return default

def normalize_from_search_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normaliza una fila del endpoint /api/search_ocds (campo 'data') al esquema pedido.
    OJO: este endpoint es resumido; algunos campos no existirán y quedarán como None.
    """
    return {
        # básicos
        "id": get_multi(row, ["ocid", "id"]),
        "entidad": get_multi(row, ["buyerName", "buyer", "buyer_name", "entidad"]),
        "objeto": get_multi(row, ["title", "objeto", "descripcion", "description"]),
        # presupuesto/moneda: se completan solo si el backend los incluye en 'data'
        "presupuesto": get_multi(row, ["budgetAmount", "tenderValueAmount", "presupuesto", "amount", "valueAmount"]),
        "moneda": get_multi(row, ["currency", "budgetCurrency", "tenderValueCurrency", "moneda"]),
        # lugar: si el backend trae provincia/cantón del comprador en 'data'
        "lugar": get_multi(row, [
            "buyerProvince", "buyer_province", "province", "provincia",
            "region", "buyerRegion", "buyer_region", "jurisdiction",
        ]),
        # fechas: 'date' suele ser la de publicación en el buscador
        "fecha_conv": get_multi(row, ["date", "publicationDate", "tenderStartDate"]),
        "fecha_adj": get_multi(row, ["awardDate", "adjudicationDate"]),
        # oferentes / proveedor / valor adjudicado (si el backend los publica en la búsqueda)
        "oferentes": get_multi(row, ["tenderersCount", "numberOfTenderers", "oferentes"]),
        "proveedor": get_multi(row, ["supplierName", "supplier", "adjudicatario", "awardedSupplier"]),
        "valor_adj": get_multi(row, ["awardValueAmount", "awardedAmount", "amountAwarded", "valorAdjudicado"]),
        "justificacion": get_multi(row, ["procurementMethodRationale", "justification", "rationale", "justificacion"]),
    }

def fetch_all_search(year: int, search: str, buyer: Optional[str], supplier: Optional[str]) -> List[Dict[str, Any]]:
    """
    Recorre TODAS las páginas del buscador oficial y devuelve el 'data' concatenado.
    """
    page = 1
    all_rows: List[Dict[str, Any]] = []

    while True:
        params: Dict[str, Any] = {"year": year, "search": search, "page": page}
        if buyer:
            params["buyer"] = buyer
        if supplier:
            params["supplier"] = supplier

        r = requests.get(SEARCH_URL, params=params, timeout=60)
        r.raise_for_status()
        payload = r.json()

        data = payload.get("data") or []
        if not isinstance(data, list):
            break

        all_rows.extend(data)

        pages = int(payload.get("pages") or 1)
        cur = int(payload.get("page") or page)
        if cur >= pages:
            break
        page = cur + 1

    return all_rows

# ---- TOOL (Ecuador) SOLO con search_ocds, sin límite de 5 y paginando todo ----
@function_tool
def ec_fetch_search_only(year: int = 2023,
                         search: str = "subasta inversa",
                         buyer: Optional[str] = None,
                         supplier: Optional[str] = None) -> Dict[str, Any]:
    """
    Descarga TODO lo que devuelva /api/search_ocds para los parámetros indicados,
    y normaliza fila por fila. SIN límite artificial.
    """
    raw_rows = fetch_all_search(year, search, buyer, supplier)
    normalized = [normalize_from_search_row(r) for r in raw_rows]

    return {
        "used_params": {"year": year, "search": search, "buyer": buyer, "supplier": supplier},
        "count": len(normalized),
        "rows_head": normalized[:5],   # solo mostramos 5 en pantalla, pero
        "all_rows": normalized,        # aquí viene TODO el contenido
    }

# ---- stubs (igual que antes) ----
@function_tool
def co_fetch_stub() -> Dict[str, Any]:
    return {"country": "Colombia", "count": 2, "rows": [{"id":"co-1"},{"id":"co-2"}]}

@function_tool
def cl_fetch_stub() -> Dict[str, Any]:
    return {"country": "Chile", "count": 1, "rows": [{"id":"cl-1"}]}

# ---- agentes ----
ecuador_agent = Agent(
    name="EcuadorAgent",
    instructions=(
        "Usa ec_fetch_search_only con year=2023 y search='subasta inversa' (sin límite), "
        "y devuelve: parámetros usados, cantidad total, las primeras 5 filas normalizadas y guarda la lista completa en all_rows."
    ),
    tools=[ec_fetch_search_only],
)

colombia_agent = Agent(
    name="ColombiaAgent",
    instructions="Usa co_fetch_stub y devuelve su salida sin adornos.",
    tools=[co_fetch_stub],
)

chile_agent = Agent(
    name="ChileAgent",
    instructions="Usa cl_fetch_stub y devuelve su salida sin adornos.",
    tools=[cl_fetch_stub],
)

# ---- manager ----
manager = Agent(
    name="Manager",
    instructions=(
        "Orquesta a los agentes. Para Ecuador, llama a la herramienta y reporta: "
        "parámetros usados, conteo total y muestra (5). Para Colombia y Chile (stubs), "
        "solo conteo y sample."
    ),
    model=LitellmModel(model=os.getenv("OPENAI_MODEL", "gpt-5"), api_key=os.getenv("OPENAI_API_KEY")),
    tools=[
        ecuador_agent.as_tool(tool_name="ecuador_tool", tool_description="Ecuador vía buscador OCDS (paginado total)"),
        colombia_agent.as_tool(tool_name="colombia_tool", tool_description="Stub Colombia"),
        chile_agent.as_tool(tool_name="chile_tool", tool_description="Stub Chile"),
    ],
)

async def main():
    # Prompt simple: Manager llama a las tools y compone el resumen
    prompt = (
        "Ecuador: ejecuta ec_fetch_search_only(year=2023, search='subasta inversa'). "
        "Muestra used_params, count y las primeras 5 filas normalizadas (rows_head). "
        "Colombia y Chile: usa stubs."
    )
    result = await Runner.run(manager, prompt)
    print(result.final_output)

if __name__ == "__main__":
    asyncio.run(main())
