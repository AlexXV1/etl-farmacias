"""Transformación MINSAL: limpieza, unión locales + turnos y cruce territorial con la DPA.

Entrada: registros de getLocales y getLocalesTurnos (tal como vienen de raw) + catálogo DPA de staging.
Salida:  filas para staging.minsal_establecimiento, rechazos con motivo y métricas de calidad.
Función pura: no usa red ni base de datos.
"""
from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal

from src.transformers.normalizacion import limpiar_texto, normalizar_nombre

# Variantes de escritura MINSAL -> nombre normalizado oficial DPA.
# Verificadas una a una contra el servicio DPA (los 9 primeros el 2026-11-01; MARCHIGUE el 2026-11-02):
# cada destino existe y está en la región esperada. El CUT siempre se toma del catálogo, nunca de esta
# tabla. NO se usan coincidencias aproximadas (fuzzy): un parecido de letras podría asignar una farmacia
# a la comuna equivocada. Un nombre nuevo queda 'sin_correspondencia' hasta verificarlo y agregarlo aquí.
ALIAS_COMUNAS = {
    "ALTO BIO BIO": "ALTO BIOBIO",
    "AYSEN": "AISEN",
    "COYHAIQUE": "COIHAIQUE",
    "LA CALERA": "CALERA",
    "LLAY LLAY": "LLAILLAY",
    "MARCHIGUE": "MARCHIHUE",          # CUT 06204, O'Higgins (fk_region MINSAL 8 -> 06)
    "PUERTO NATALES": "NATALES",
    "PUERTO SAAVEDRA": "SAAVEDRA",
    "SAN FRANCISCO DE MOSTAZAL": "MOSTAZAL",
    "TIL-TIL": "TILTIL",
}

# Rango geográfico de Chile (incluye Rapa Nui y Juan Fernández; excluye la Antártica).
LAT_MIN, LAT_MAX = Decimal("-56.0"), Decimal("-17.4")
LNG_MIN, LNG_MAX = Decimal("-109.5"), Decimal("-66.0")
_NUMERO = re.compile(r"^-?\d+(\.\d+)?$")
_RESIDUO_FINAL = re.compile(r"[^\d]+$")   # caracteres NO numéricos al final: ',', '.', '�'

# De mejor a peor: el estado de un par lat/lng es el peor de sus dos componentes.
_ORDEN_ESTADO = ["valida", "corregida", "fuera_de_rango", "formato_invalido", "vacia"]


@dataclass
class ResultadoMinsal:
    filas: list[dict] = field(default_factory=list)
    rechazos: list[dict] = field(default_factory=list)
    metricas: dict = field(default_factory=dict)


# ============================================================ 1) coordenadas
def parse_coordenada(valor) -> tuple[Decimal | None, str]:
    """Devuelve (valor, estado) con estado: valida | corregida | vacia | formato_invalido.

    - '-33.0469'              -> valida
    - '-33.361375857962635,'  -> corregida: se quita el residuo no numérico del final
    - '-71.5315875,17'        -> formato_invalido: el residuo tiene dígitos, no se sabe qué significa
    - '-33431624'             -> 'valida' aquí, pero queda fuera_de_rango en el par (no se adivina
                                 dónde iba el punto decimal)
    Se usa Decimal (no float) para no introducir errores de redondeo binario.
    """
    texto = limpiar_texto(valor)
    if texto is None:
        return None, "vacia"
    if _NUMERO.match(texto):
        return Decimal(texto), "valida"
    limpio = _RESIDUO_FINAL.sub("", texto)
    if limpio != texto and _NUMERO.match(limpio):
        return Decimal(limpio), "corregida"
    return None, "formato_invalido"


def parse_par_coordenadas(lat_raw, lng_raw) -> tuple[Decimal | None, Decimal | None, str]:
    """Evalúa latitud y longitud JUNTAS: el par es tan bueno como su peor componente.

    Una latitud sin longitud no sirve para ubicar el local; por eso o van las dos, o ninguna
    (lo mismo exige el CHECK ck_coordenadas de staging).
    """
    lat, e_lat = parse_coordenada(lat_raw)
    lng, e_lng = parse_coordenada(lng_raw)
    estado = max(e_lat, e_lng, key=_ORDEN_ESTADO.index)
    if estado in ("valida", "corregida"):
        if not (LAT_MIN <= lat <= LAT_MAX and LNG_MIN <= lng <= LNG_MAX):
            return None, None, "fuera_de_rango"
        # numeric(10,7) en la base: 7 decimales ≈ 1 cm de precisión.
        return lat.quantize(Decimal("0.0000001")), lng.quantize(Decimal("0.0000001")), estado
    return None, None, estado


# ============================================================ 2) fechas y enteros
def parse_fecha_fuente(valor) -> date | None:
    """Unifica los dos formatos observados: 'dd-mm-yy' (getLocales) y 'yyyy-mm-dd' (turnos)."""
    texto = limpiar_texto(valor)
    for fmt in ("%Y-%m-%d", "%d-%m-%y"):
        try:
            return datetime.strptime(texto or "", fmt).date()
        except ValueError:
            continue
    return None


def parse_entero(valor) -> int | None:
    """La API entrega todo como texto ('43'); None si no es un entero."""
    texto = limpiar_texto(valor)
    try:
        return int(texto) if texto is not None else None
    except ValueError:
        return None


# ============================================================ 3) tipo de establecimiento
def inferir_tipo(nombre: str) -> str:
    """Tipo de establecimiento inferido del nombre (campo calculado: la API no lo informa)."""
    n = normalizar_nombre(nombre) or ""
    if n.startswith("BOTIQUIN"):
        return "botiquin"
    if "VACUNATORIO" in n:
        return "vacunatorio"
    if "ALMACEN FARMACEUTICO" in n:
        return "almacen_farmaceutico"
    return "farmacia"


# ============================================================ 4) limpieza por endpoint
def _limpiar_endpoint(registros: list[dict], fuente: str) -> tuple[dict[int, dict], list[dict]]:
    """Valida campos obligatorios y elimina duplicados de local_id DENTRO de un endpoint.

    Devuelve {local_id: registro} con los válidos y la lista de rechazos (con motivo).
    """
    validos: dict[int, dict] = {}
    rechazos = []
    for reg in registros:
        clave = str(reg.get("local_id") or "")
        local_id = parse_entero(reg.get("local_id"))
        motivo = None
        if local_id is None or local_id <= 0:
            motivo = "local_id_invalido"
        elif limpiar_texto(reg.get("local_nombre")) is None:
            motivo = "nombre_vacio"
        elif normalizar_nombre(reg.get("comuna_nombre")) is None:
            motivo = "comuna_vacia"
        elif parse_entero(reg.get("fk_region")) is None:
            motivo = "fk_region_invalido"
        elif local_id in validos:
            motivo = "duplicado_en_endpoint"   # se conserva la primera aparición
        if motivo:
            rechazos.append({"fuente": fuente, "clave_origen": clave, "motivo": motivo, "registro": reg})
        else:
            validos[local_id] = reg
    return validos, rechazos


# ============================================================ 6) cruce territorial
def _candidato(comuna_norm: str, catalogo: dict[str, dict]) -> tuple[dict | None, str]:
    """Busca la comuna en la DPA: primero nombre exacto, después alias verificado."""
    if comuna_norm in catalogo:
        return catalogo[comuna_norm], "exacto"
    alias = ALIAS_COMUNAS.get(comuna_norm)
    if alias and alias in catalogo:
        return catalogo[alias], "alias"
    return None, "sin_correspondencia"


def inferir_mapa_regiones(pares: list[tuple[int, str]]) -> dict[int, str]:
    """Para cada fk_region de MINSAL, el CUT_REG mayoritario entre sus comunas encontradas.

    Se deriva de los datos (no se escribe a mano): si MINSAL renumera sus regiones, el mapa
    se ajusta solo. Sirve para detectar un cruce con una comuna homónima de otra región.
    """
    votos: dict[int, Counter] = defaultdict(Counter)
    for fk_region, cut_reg in pares:
        votos[fk_region][cut_reg] += 1
    return {fk: c.most_common(1)[0][0] for fk, c in votos.items()}


def asignar_territorio(filas: list[dict], catalogo_dpa: list[dict]) -> dict:
    """Asigna cut_com y estado_correspondencia a cada fila (modifica `filas`)."""
    if not catalogo_dpa:
        # Sin catálogo no se puede cruzar: se marca, no se inventa.
        for f in filas:
            f["cut_com"], f["estado_correspondencia"] = None, "catalogo_no_disponible"
        return {"mapa_regiones": {}}

    catalogo = {c["comuna_normalizada"]: c for c in catalogo_dpa}
    candidatos = [_candidato(f["comuna_normalizada"], catalogo) for f in filas]
    mapa = inferir_mapa_regiones([(f["fk_region_minsal"], c["cut_reg"])
                                  for f, (c, _) in zip(filas, candidatos) if c])
    for f, (cand, estado) in zip(filas, candidatos):
        # El nombre coincide, pero la región no es la que corresponde a su fk_region: se descarta.
        if cand and cand["cut_reg"] != mapa.get(f["fk_region_minsal"]):
            cand, estado = None, "region_inconsistente"
        f["cut_com"] = cand["cut_com"] if cand else None
        f["estado_correspondencia"] = estado
    return {"mapa_regiones": {str(k): v for k, v in sorted(mapa.items())}}


# ============================================================ 5 + 7) unión y transformación completa
def transformar(locales: list[dict], turnos: list[dict], catalogo_dpa: list[dict],
                fecha_observacion: date, extraccion_ids: list[str]) -> ResultadoMinsal:
    res = ResultadoMinsal()
    val_locales, rech_l = _limpiar_endpoint(locales, "minsal_locales")
    val_turnos, rech_t = _limpiar_endpoint(turnos, "minsal_turnos")
    res.rechazos = rech_l + rech_t

    conflictos = 0
    # UNIÓN por local_id: un establecimiento por cada id presente en cualquiera de los dos endpoints.
    for local_id in sorted(val_locales.keys() | val_turnos.keys()):
        en_general, en_turno = local_id in val_locales, local_id in val_turnos
        # Si aparece en ambos, los atributos se toman del listado general.
        reg = val_locales[local_id] if en_general else val_turnos[local_id]
        if en_general and en_turno and (normalizar_nombre(val_locales[local_id]["comuna_nombre"])
                                        != normalizar_nombre(val_turnos[local_id]["comuna_nombre"])):
            conflictos += 1
        lat, lng, estado_coord = parse_par_coordenadas(reg.get("local_lat"), reg.get("local_lng"))
        nombre = limpiar_texto(reg["local_nombre"])
        # El endpoint de turnos publica también turnos con fecha antigua (p. ej. 2026-07-01):
        # solo es vigente el turno cuya fecha coincide con la fecha de observación.
        turno_vigente = en_turno and parse_fecha_fuente(val_turnos[local_id].get("fecha")) == fecha_observacion
        res.filas.append({
            "fecha_observacion": fecha_observacion,
            "local_id": local_id,
            "nombre": nombre,
            "tipo_establecimiento": inferir_tipo(nombre),
            "direccion": limpiar_texto(reg.get("local_direccion")),
            "telefono": limpiar_texto(reg.get("local_telefono")),
            "localidad": limpiar_texto(reg.get("localidad_nombre")),
            "comuna_origen": limpiar_texto(reg["comuna_nombre"]),
            "comuna_normalizada": normalizar_nombre(reg["comuna_nombre"]),
            "fk_region_minsal": parse_entero(reg["fk_region"]),
            "fk_comuna_minsal": parse_entero(reg.get("fk_comuna")),
            "latitud": lat,
            "longitud": lng,
            "estado_coordenada": estado_coord,
            "fecha_fuente": parse_fecha_fuente(reg.get("fecha")),
            "en_listado_general": en_general,
            "en_turno": en_turno,
            "turno_vigente": turno_vigente,
            "extraccion_ids": extraccion_ids,
        })

    info_cruce = asignar_territorio(res.filas, catalogo_dpa)
    res.metricas = {
        "leidos_locales": len(locales),
        "leidos_turnos": len(turnos),
        "establecimientos": len(res.filas),
        "en_ambos_endpoints": len(val_locales.keys() & val_turnos.keys()),
        "turnos_publicados": sum(1 for f in res.filas if f["en_turno"]),
        "turnos_vigentes": sum(1 for f in res.filas if f["turno_vigente"]),
        "conflictos_comuna_entre_endpoints": conflictos,
        "rechazados": len(res.rechazos),
        "rechazos_por_motivo": dict(Counter(r["motivo"] for r in res.rechazos)),
        "estado_correspondencia": dict(Counter(f["estado_correspondencia"] for f in res.filas)),
        "estado_coordenada": dict(Counter(f["estado_coordenada"] for f in res.filas)),
        "tipo_establecimiento": dict(Counter(f["tipo_establecimiento"] for f in res.filas)),
        "comunas_sin_correspondencia": sorted({f["comuna_origen"] for f in res.filas
                                               if f["estado_correspondencia"] in ("sin_correspondencia", "region_inconsistente")}),
        "fecha_fuente_distinta_a_observacion": sum(1 for f in res.filas
                                                   if f["fecha_fuente"] and f["fecha_fuente"] != fecha_observacion),
        **info_cruce,
    }
    return res
