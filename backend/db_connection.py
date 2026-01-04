import os
import psycopg2
from urllib.parse import urlparse


def _get_env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except ValueError:
        return default


def get_connection():
    """
    Soporta dos modos:
    1) DATABASE_URL (recomendado en cloud: Streamlit, Render, Neon, Supabase, etc.)
       Ejemplo:
       postgres://user:password@host:5432/dbname

    2) Variables separadas (fallback, estilo Docker/local)
    """

    database_url = os.getenv("DATABASE_URL")

    if database_url:
        # Parsear DATABASE_URL
        url = urlparse(database_url)

        db_config = {
            "dbname": url.path.lstrip("/"),
            "user": url.username,
            "password": url.password,
            "host": url.hostname,
            "port": url.port or 5432,
            # En cloud casi siempre es necesario
            "sslmode": os.getenv("PGSSLMODE", "require"),
        }
    else:
        # Modo variables separadas (local / docker)
        db_config = {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": _get_env_int("DB_PORT", 5432),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", ""),
            "dbname": os.getenv("DB_NAME", "finanzas"),
            "sslmode": os.getenv("PGSSLMODE", "prefer"),
        }

    # Log útil (sin imprimir password)
    safe_config = {k: v for k, v in db_config.items() if k != "password"}
    print("Intentando conectar a Postgres con:", safe_config)

    return psycopg2.connect(**db_config)