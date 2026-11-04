"""Crea .env a partir de .env.example, reemplazando cada 'cambiar_...' por un secreto aleatorio.

- No sobrescribe un .env existente (cambiar contraseñas con la base ya creada la deja inaccesible).
- No imprime ningún secreto.
Uso:  .\\.venv\\Scripts\\python.exe scripts\\generar_env.py
"""
import base64
import re
import secrets
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    destino = ROOT / ".env"
    if destino.exists():
        print(".env ya existe; no se modifica.")
        return

    texto = (ROOT / ".env.example").read_text(encoding="utf-8")
    valores = {
        # Fernet exige exactamente 32 bytes codificados en base64 url-safe.
        "cambiar_fernet_key": base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(),
        "cambiar_jwt_secret": secrets.token_urlsafe(32),
        "cambiar_password_admin": secrets.token_urlsafe(12),
        "cambiar_password_postgres": secrets.token_hex(16),
        # Esta contraseña va también dentro de la URI de la Connection: hex evita caracteres como @ / :
        "cambiar_password_etl": secrets.token_hex(16),
    }
    for marcador, valor in valores.items():
        texto = texto.replace(marcador, valor)

    if re.search(r"cambiar_\w+", texto):
        raise SystemExit("Quedaron valores 'cambiar_' sin reemplazar en .env.example")

    # newline="\n": docker compose lee el archivo; evitamos CRLF.
    destino.write_text(texto, encoding="utf-8", newline="\n")
    print(f".env creado en {destino}. Usuario de Airflow: ver AIRFLOW_ADMIN_USER/PASSWORD en .env")


if __name__ == "__main__":
    main()
