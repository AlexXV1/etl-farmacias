"""Transformación del catálogo DPA SUBDERE: validación de códigos CUT y normalización.

Entrada: registros de comunas_extractor (campos CUT_REG, CUT_PROV, CUT_COM, REGION, PROVINCIA, COMUNA).
Salida:  filas para staging.dpa_comuna + rechazos con motivo para staging.rechazo.
Es una función pura (no usa red ni base de datos): se prueba con datos sintéticos.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from src.transformers.normalizacion import limpiar_texto, normalizar_nombre


@dataclass
class ResultadoComunas:
    filas: list[dict] = field(default_factory=list)
    rechazos: list[dict] = field(default_factory=list)

    def metricas(self, leidos: int) -> dict:
        """Métricas de calidad para la auditoría: leídos = cargados + rechazados."""
        return {"leidos": leidos, "cargados": len(self.filas), "rechazados": len(self.rechazos),
                "regiones": len({f["cut_reg"] for f in self.filas}),
                "provincias": len({f["cut_prov"] for f in self.filas})}


def normalizar_codigo(valor, largo: int) -> str | None:
    """Recodifica a texto de largo fijo con ceros a la izquierda: '1101' o 1101 -> '01101'.

    El CUT es un código, no un número: como entero, la comuna 01101 perdería el 0 inicial y
    no se podría derivar su región con left(cut, 2). Devuelve None si no es numérico o es largo.
    """
    texto = limpiar_texto(valor)
    if texto is None or not texto.isdigit() or len(texto) > largo:
        return None
    return texto.zfill(largo)


def transformar(registros: list[dict]) -> ResultadoComunas:
    res = ResultadoComunas()
    vistos_cut: set[str] = set()
    vistos_nombre: dict[str, str] = {}   # nombre normalizado -> cut_com que lo usó primero

    def rechazar(reg, clave, motivo):
        res.rechazos.append({"clave_origen": clave or "", "motivo": motivo, "registro": reg})

    for reg in registros:
        cut_com = normalizar_codigo(reg.get("CUT_COM"), 5)
        cut_prov = normalizar_codigo(reg.get("CUT_PROV"), 3)
        cut_reg = normalizar_codigo(reg.get("CUT_REG"), 2)
        clave = str(reg.get("CUT_COM") or "")
        comuna, provincia, region = (limpiar_texto(reg.get(k)) for k in ("COMUNA", "PROVINCIA", "REGION"))

        # Las validaciones van en orden: cada registro se rechaza por el PRIMER problema encontrado.
        if cut_com is None:
            rechazar(reg, clave, "cut_com_invalido"); continue
        if cut_prov != cut_com[:3] or cut_reg != cut_com[:2]:
            # Jerarquía CUT: 05801 pertenece a la provincia 058 y a la región 05.
            rechazar(reg, clave, "jerarquia_cut_inconsistente"); continue
        if not (comuna and provincia and region):
            rechazar(reg, clave, "nombre_territorial_vacio"); continue
        if cut_com in vistos_cut:
            rechazar(reg, clave, "cut_com_duplicado"); continue
        nombre_norm = normalizar_nombre(comuna)
        if nombre_norm in vistos_nombre:
            # Dos comunas con el mismo nombre normalizado harían ambiguo el cruce con MINSAL.
            rechazar(reg, clave, f"nombre_normalizado_duplicado_con_{vistos_nombre[nombre_norm]}"); continue

        vistos_cut.add(cut_com)
        vistos_nombre[nombre_norm] = cut_com
        res.filas.append({"cut_com": cut_com, "cut_prov": cut_prov, "cut_reg": cut_reg, "comuna": comuna,
                          "provincia": provincia, "region": region, "comuna_normalizada": nombre_norm})
    return res
