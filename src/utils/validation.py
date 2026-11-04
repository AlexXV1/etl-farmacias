"""Métricas de calidad reutilizables (perfil de nulos)."""
from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence


def perfil_nulos(filas: Sequence[Mapping], campos: Iterable[str]) -> dict:
    """Perfil de nulos sobre `filas` para los `campos` indicados.

    DENOMINADOR de todos los porcentajes = len(filas): los registros de UNA tabla en UNA carga.
    - por_campo[c].pct:     % de filas con el campo c nulo.
    - pct_filas_con_nulos:  % de filas con al menos un campo relevante nulo (va a etl_audit.pct_nulos).
    """
    campos = list(campos)
    total = len(filas)
    por_campo = {}
    for c in campos:
        nulos = sum(1 for f in filas if f.get(c) is None)
        por_campo[c] = {"nulos": nulos, "total": total, "pct": round(100.0 * nulos / total, 2) if total else 0.0}
    con_nulos = sum(1 for f in filas if any(f.get(c) is None for c in campos))
    return {
        "campos_evaluados": campos,
        "total_filas": total,
        "filas_con_nulos": con_nulos,
        "pct_filas_con_nulos": round(100.0 * con_nulos / total, 2) if total else 0.0,
        "por_campo": por_campo,
    }
