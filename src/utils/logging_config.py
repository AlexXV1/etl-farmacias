"""Logging del proyecto. Dentro de Airflow los mensajes llegan al log de cada tarea."""
import logging

_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    # Airflow ya configura el logging. Fuera de Airflow (scripts locales) no hay handler
    # raíz, así que se configura uno básico para que los mensajes se vean en consola.
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format=_FORMAT)
    return logger
