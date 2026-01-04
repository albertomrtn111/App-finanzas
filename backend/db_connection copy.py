import os
import mysql.connector

def _get_env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except ValueError:
        return default

def get_connection():
    # Por defecto: modo Docker (host=mysql, port=3306)
    # En local puedes sobrescribir con .env o variables del sistema.
    db_config = {
        "host": os.getenv("DB_HOST", "mysql"),
        "port": _get_env_int("DB_PORT", 3306),
        "user": os.getenv("DB_USER", "appuser"),
        "password": os.getenv("DB_PASSWORD", "APPPASS01"),
        "database": os.getenv("DB_NAME", "finanzas"),
    }

    # Log útil (siempre)
    print("Intentando conectar con:", db_config)

    conn = mysql.connector.connect(**db_config)
    return conn