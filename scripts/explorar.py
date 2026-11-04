"""Paso 1: explorar las APIs antes de diseñar nada.

Pide cada URL e imprime código HTTP, Content-Type, tipo de dato, cantidad de
registros, claves y un ejemplo. Luego responde con código las preguntas de la
guía sobre los datos de MINSAL.

Solo usa la biblioteca estándar:  py -3.13 scripts\\explorar.py
"""
from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from collections import Counter

USER_AGENT = "INF522-ETL-Farmacias/explorar (proyecto academico)"
TIMEOUT_S = 60

MOP = "https://rest-sit.mop.gob.cl/arcgis/rest/services/INTEROP/SERVICIO_DPA/MapServer/1"
URLS = {
    "minsal_locales": "https://midas.minsal.cl/farmacia_v2/WS/getLocales.php",
    "minsal_turnos": "https://midas.minsal.cl/farmacia_v2/WS/getLocalesTurnos.php",
    "dpa_metadatos": MOP + "?f=json",
    "dpa_query": MOP + "/query?where=1%3D1&outFields=*&returnGeometry=false&f=json",
    "dpa_modernizacion": "https://apis.modernizacion.cl/dpa/comunas",  # API original: debería fallar
}


def pedir(url: str) -> tuple[int | None, str | None, bytes, str | None]:
    """Devuelve (estado HTTP, Content-Type, cuerpo, error). Nunca lanza excepción."""
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_S) as resp:
            return resp.status, resp.headers.get("Content-Type"), resp.read(), None
    except urllib.error.HTTPError as exc:  # 4xx / 5xx: sí hubo respuesta
        return exc.code, exc.headers.get("Content-Type"), exc.read(), None
    except (urllib.error.URLError, TimeoutError, OSError) as exc:  # red, DNS, SSL, timeout
        return None, None, b"", repr(exc)


def a_json(cuerpo: bytes):
    # utf-8-sig quita un posible BOM. No confiamos en el Content-Type: MINSAL dice text/html.
    try:
        return json.loads(cuerpo.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None


def describir(nombre: str, url: str):
    estado, ctype, cuerpo, error = pedir(url)
    print(f"\n=== {nombre}\n{url}")
    print(f"HTTP={estado}  Content-Type={ctype}  bytes={len(cuerpo)}")
    if error:
        print("ERROR:", error)
        return None
    data = a_json(cuerpo)
    if isinstance(data, list):
        print(f"Tipo: lista con {len(data)} registros")
        if data:
            print("Claves:", list(data[0]))
            print("Ejemplo:", json.dumps(data[0], ensure_ascii=False))
    elif isinstance(data, dict):
        print("Tipo: objeto con claves", list(data)[:15])
        if "features" in data:
            feats = data["features"]
            print(f"features: {len(feats)}  exceededTransferLimit={data.get('exceededTransferLimit')}")
            if feats:
                print("Ejemplo:", json.dumps(feats[0]["attributes"], ensure_ascii=False))
    else:
        print("No es JSON. Inicio:", cuerpo[:200])
    return data


def es_decimal(texto: str) -> bool:
    try:
        float(texto)
        return True
    except ValueError:
        return False


def main() -> None:
    datos = {n: describir(n, u) for n, u in URLS.items()}

    meta = datos["dpa_metadatos"] or {}
    print("\n=== Metadatos de la capa DPA")
    print("maxRecordCount:", meta.get("maxRecordCount"))
    print("supportsPagination:", meta.get("advancedQueryCapabilities", {}).get("supportsPagination"))
    print("Campos:", [f["name"] for f in meta.get("fields", [])])

    locales = datos["minsal_locales"] or []
    turnos = datos["minsal_turnos"] or []
    print("\n=== Preguntas sobre MINSAL")

    for nombre, filas in (("locales", locales), ("turnos", turnos)):
        ids = Counter(r["local_id"] for r in filas)
        repetidos = {k: v for k, v in ids.items() if v > 1}
        print(f"[{nombre}] local_id repetidos: {len(repetidos)} {list(repetidos.items())[:5]}")

    print("fk_region (locales):", sorted({r["fk_region"] for r in locales}, key=int))
    print("fk_region (turnos):", sorted({r["fk_region"] for r in turnos}, key=int))

    comunas = {r["comuna_nombre"] for r in locales + turnos}
    print("Comunas distintas en MINSAL (por nombre):", len(comunas))

    ids_locales = {r["local_id"] for r in locales}
    solo_turno = [r for r in turnos if r["local_id"] not in ids_locales]
    print(f"Locales de turno que NO están en getLocales: {len(solo_turno)} de {len(turnos)}")

    def formatos(filas):
        patron = lambda f: re.sub(r"\d", "9", f or "")  # 01-11-26 -> 99-99-99
        return dict(Counter(patron(r.get("fecha")) for r in filas))

    print("Formatos de 'fecha' (locales):", formatos(locales))
    print("Formatos de 'fecha' (turnos):", formatos(turnos))

    todas = locales + turnos
    vacias = sum(1 for r in todas if not str(r.get("local_lat", "")).strip())
    raras = sorted({r["local_lat"] for r in todas
                    if str(r.get("local_lat", "")).strip() and not es_decimal(r["local_lat"].strip())})
    print(f"Latitudes vacías: {vacias}")
    print(f"Latitudes no numéricas ({len(raras)}):", raras[:10])

    dpa = datos["dpa_query"] or {}
    print("\nComunas DPA devueltas en una sola consulta:", len(dpa.get("features", [])),
          "(ojo: puede estar limitado por maxRecordCount)")


if __name__ == "__main__":
    main()
