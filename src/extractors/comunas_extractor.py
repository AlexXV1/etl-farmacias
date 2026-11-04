"""Extractor DPA SUBDERE desde un servicio ArcGIS REST (capa de comunas).

PAGINACIÓN POR OBJECTID. El servidor del MOP no declara `supportsPagination` (paso 1), así que
no se puede confiar en resultOffset. Se usa la técnica estándar para estos servidores:
  1) pedir solo la lista de OBJECTID           (returnIdsOnly=true)
  2) pedir los registros por lotes de page_size (objectIds=1,2,3,...)
Es exacta (cada id se pide una vez), no depende del orden del servidor y tiene un tope de páginas.
También funciona con el servicio de respaldo (Subpesca), cuyos campos vienen en minúscula.
"""
from __future__ import annotations

import math
import time
from typing import Any

import requests

from src.extractors.base import Extraccion, Pagina
from src.extractors.http_client import ExtractionError, InvalidResponseError, http_get, parse_json
from src.utils.logging_config import get_logger

log = get_logger(__name__)

CAMPOS = ("CUT_REG", "CUT_PROV", "CUT_COM", "REGION", "PROVINCIA", "COMUNA")
# Protección: con 345 comunas y page_size=100 son 4 páginas. Si salieran más de 50, algo
# está mal (page_size erróneo o el servicio devuelve otra capa) y es mejor detenerse.
MAX_PAGINAS = 50


def _query(layer_url: str, params: dict[str, Any], timeout_s: float, session) -> tuple[Any, Any]:
    full = {"f": "json", **params}
    resp = http_get(f"{layer_url}/query", params=full, timeout_s=timeout_s, session=session)
    data = parse_json(resp)
    # ArcGIS informa errores con HTTP 200 y un objeto {"error": {...}}: hay que revisar el cuerpo.
    if isinstance(data, dict) and "error" in data:
        raise InvalidResponseError(f"ArcGIS devolvió error en {layer_url}: {data['error']}")
    return resp, data


def validar_disponibilidad(layer_url: str, timeout_s: float, session: requests.Session | None = None) -> dict:
    """Validación previa: la capa responde y tiene comunas (returnCountOnly, consulta liviana)."""
    resp, data = _query(layer_url, {"where": "1=1", "returnCountOnly": "true"}, timeout_s, session)
    count = data.get("count") if isinstance(data, dict) else None
    if not isinstance(count, int) or count <= 0:
        raise InvalidResponseError(f"Conteo de comunas inválido en {layer_url}: {count!r}")
    return {"url": layer_url, "http_status": resp.status, "registros": count, "elapsed_s": resp.elapsed_s}


def normalizar_atributos(attrs: dict) -> dict:
    """Deja solo los campos necesarios, con nombre en mayúsculas (MOP: COMUNA, Subpesca: comuna)."""
    upper = {k.upper(): v for k, v in attrs.items()}
    return {c: upper.get(c) for c in CAMPOS}


def extraer(layer_url: str, page_size: int, timeout_s: float,
            session: requests.Session | None = None) -> Extraccion:
    inicio = time.monotonic()

    # 1) Todos los OBJECTID.
    resp_ids, ids_data = _query(layer_url, {"where": "1=1", "returnIdsOnly": "true"}, timeout_s, session)
    object_ids = sorted(ids_data.get("objectIds") or []) if isinstance(ids_data, dict) else []
    if not object_ids:
        raise InvalidResponseError(f"El servicio no devolvió objectIds en {layer_url}")
    total_paginas = math.ceil(len(object_ids) / page_size)
    if total_paginas > MAX_PAGINAS:
        raise ExtractionError(f"{total_paginas} páginas superan el tope de {MAX_PAGINAS}; revisar page_size")

    # 2) Un lote de ids por página.
    paginas: list[Pagina] = []
    registros: list[dict] = []
    intentos = resp_ids.attempts
    for n in range(total_paginas):
        lote = object_ids[n * page_size:(n + 1) * page_size]
        params = {"objectIds": ",".join(map(str, lote)), "outFields": "*", "returnGeometry": "false"}
        resp, data = _query(layer_url, params, timeout_s, session)
        intentos += resp.attempts
        features = data.get("features") if isinstance(data, dict) else None
        if not isinstance(features, list):
            raise InvalidResponseError(f"Página {n + 1} sin 'features' en {layer_url}")
        # Cada página debe traer exactamente los ids pedidos: si no, faltarían comunas en silencio.
        if len(features) != len(lote):
            raise InvalidResponseError(f"Página {n + 1}: se pidieron {len(lote)} ids y llegaron {len(features)}")
        paginas.append(Pagina(n + 1, f"{layer_url}/query", params, resp.status, resp.content_type,
                              data, resp.sha256, len(features)))
        registros.extend(normalizar_atributos(f.get("attributes") or {}) for f in features)

    log.info("DPA: %d comunas en %d páginas de %d (%s)", len(registros), total_paginas, page_size, layer_url)
    return Extraccion(fuente="dpa_comunas", url_base=layer_url, extraido_en=resp_ids.requested_at,
                      paginas=paginas, registros=registros, intentos_http=intentos,
                      duracion_s=time.monotonic() - inicio)


def extraer_con_respaldo(urls: list[str], page_size: int, timeout_s: float,
                         session: requests.Session | None = None) -> Extraccion:
    """Intenta cada URL en orden (principal, respaldo). Si se usó el respaldo, lo deja en advertencias."""
    errores = []
    for i, url in enumerate(urls):
        try:
            extraccion = extraer(url, page_size, timeout_s, session)
        except ExtractionError as exc:
            log.error("Falló la extracción DPA desde %s: %s", url, exc)
            errores.append(f"{url}: {exc}")
            continue
        if i > 0:
            extraccion.advertencias.append(f"se_uso_respaldo: principal falló ({errores[0]})")
        return extraccion
    raise ExtractionError("Todas las URLs DPA fallaron: " + " | ".join(errores))
