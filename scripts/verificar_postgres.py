"""Verifica la conexión a la base 'farmacias' desde el host, con el rol de aplicación.

Usa la misma función que el resto del proyecto (params_from_env), que lee .env sin imprimirlo.
Comprueba permisos de escritura con una tabla temporal (desaparece al cerrar la conexión).
Uso:  .\\.venv\\Scripts\\python.exe scripts\\verificar_postgres.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # para poder importar src/

from src.database.connection import params_from_env  # noqa: E402


def main() -> None:
    params = params_from_env()
    print("Conectando a", params)  # el __repr__ no muestra la contraseña
    with params.connect() as conn:
        version, db, user, superuser = conn.execute(
            "SELECT version(), current_database(), current_user, current_setting('is_superuser')"
        ).fetchone()
        conn.execute("CREATE TEMPORARY TABLE _verificacion (id int)")
        conn.execute("INSERT INTO _verificacion VALUES (1)")
        n = conn.execute("SELECT count(*) FROM _verificacion").fetchone()[0]
        print(f"OK db={db} user={user} superusuario={superuser} escritura_temporal={n == 1}")
        print(version)


if __name__ == "__main__":
    main()
