"""Normalización de textos, compartida por las dos fuentes (DPA y MINSAL)."""
from __future__ import annotations

import unicodedata


def limpiar_texto(valor) -> str | None:
    """Recorta y colapsa espacios. Cadena vacía o solo espacios -> None (valor ausente).

    Así un "" de la API y un campo que no vino quedan representados igual: como NULL.
    """
    if valor is None:
        return None
    texto = " ".join(str(valor).split())
    return texto or None


def normalizar_nombre(valor) -> str | None:
    """Clave de comparación de nombres territoriales: sin tildes, mayúsculas, espacios simples.

    'Viña del Mar' -> 'VINA DEL MAR';  'Ñuñoa' -> 'NUNOA';  '  Alto  Biobío ' -> 'ALTO BIOBIO'.
    No se usa para mostrar (para eso se guarda el nombre original), solo para cruzar fuentes.
    NFKD separa cada letra de su tilde ('á' -> 'a' + '´'); luego se descartan las tildes.
    """
    texto = limpiar_texto(valor)
    if texto is None:
        return None
    sin_tildes = "".join(c for c in unicodedata.normalize("NFKD", texto) if not unicodedata.combining(c))
    return sin_tildes.upper()
