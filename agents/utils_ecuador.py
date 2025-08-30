# utils_ecuador.py
import requests
import time
from typing import Dict, Any, List, Optional

SEARCH_URL = "https://datosabiertos.compraspublicas.gob.ec/PLATAFORMA/api/search_ocds"

def get_multi(d: Dict[str, Any], keys: List[str], default=None):
    for k in keys:
        if k in d and d[k] not in (None, "", [], {}):
            return d[k]
    return default

# utils_ecuador.py (solo este bloque)
def normalize_from_search_row(row: Dict[str, Any]) -> Dict[str, Any]:
    def get_multi(d, keys, default=None):
        for k in keys:
            if k in d and d[k] not in (None, "", [], {}):
                return d[k]
        return default

    out = {
        "pais": "Ecuador",
        "id": get_multi(row, ["ocid", "id"]),
        "entidad": get_multi(row, ["buyerName", "buyer", "buyer_name", "entidad"]),
        "objeto": get_multi(row, ["title", "objeto", "descripcion", "description"]),
        "presupuesto": get_multi(row, ["budgetAmount", "tenderValueAmount", "presupuesto", "amount", "valueAmount"]),
        "moneda": get_multi(row, ["currency", "budgetCurrency", "tenderValueCurrency", "moneda"]) or "USD",
        "lugar": get_multi(row, [
            "buyerProvince","buyer_province","province","provincia",
            "region","buyerRegion","buyer_region","jurisdiction"
        ]),
        "fecha_conv": get_multi(row, ["date", "publicationDate", "tenderStartDate"]),
        "fecha_adj": get_multi(row, ["awardDate", "adjudicationDate"]),
        "oferentes": get_multi(row, ["tenderersCount", "numberOfTenderers", "oferentes"]),
        "proveedor": get_multi(row, ["supplierName", "supplier", "adjudicatario", "awardedSupplier"]),
        "valor_adj": get_multi(row, ["awardValueAmount", "awardedAmount", "amountAwarded", "valorAdjudicado"]),
        "justificacion": get_multi(row, ["procurementMethodRationale", "justification", "rationale", "justificacion"]),
    }
    return out


def fetch_all_search(year=2023, search="subasta inversa", buyer=None, supplier=None, max_rows=500):
    page, all_rows, backoff = 1, [], 2.0
    while True:
        params = {"year": year, "search": search, "page": page}
        if buyer: params["buyer"] = buyer
        if supplier: params["supplier"] = supplier

        try:
            r = requests.get(
                "https://datosabiertos.compraspublicas.gob.ec/PLATAFORMA/api/search_ocds",
                params=params, timeout=60, headers={"Accept-Encoding": "gzip, deflate"}
            )
            if r.status_code == 429:
                print(f"⏳ 429 recibido, reintentando en {backoff:.1f}s...")
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            r.raise_for_status()
        except requests.RequestException as e:
            print("❌ Error Ecuador:", e)
            break

        payload = r.json()
        data = payload.get("data") or []
        if not isinstance(data, list) or not data:
            break

        for row in data:
            all_rows.append(row)
            if len(all_rows) >= max_rows:
                return [normalize_from_search_row(r) for r in all_rows]

        pages = int(payload.get("pages") or 1)
        cur = int(payload.get("page") or page)
        if cur >= pages:
            break
        page, backoff = cur + 1, 2.0  # reset backoff tras éxito

    return [normalize_from_search_row(r) for r in all_rows]