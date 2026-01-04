from datetime import datetime
import os
import psycopg2
from urllib.parse import urlparse


def _get_env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except ValueError:
        return default


def get_conn():
    """
    Soporta:
    1) DATABASE_URL (recomendado)
    2) Variables separadas (fallback)
    """
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        url = urlparse(database_url)
        return psycopg2.connect(
            dbname=url.path.lstrip("/"),
            user=url.username,
            password=url.password,
            host=url.hostname,
            port=url.port or 5432,
            sslmode=os.getenv("PGSSLMODE", "require"),
        )

    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=_get_env_int("DB_PORT", 5432),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
        dbname=os.getenv("DB_NAME", "finanzas"),
        sslmode=os.getenv("PGSSLMODE", "prefer"),
    )


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # 1) USERS
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        email VARCHAR(255) NOT NULL UNIQUE,
        password_hash VARCHAR(255) NULL,
        google_sub VARCHAR(255) NULL UNIQUE,
        created_at TIMESTAMP NOT NULL,
        last_login_at TIMESTAMP NULL
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);")

    # 2) CATEGORÍAS GASTO
    cur.execute("""
    CREATE TABLE IF NOT EXISTS expense_categories (
        id SERIAL PRIMARY KEY,
        user_id INT NOT NULL,
        name VARCHAR(100) NOT NULL,
        created_at TIMESTAMP NOT NULL,
        CONSTRAINT uq_exp_cat_user_name UNIQUE (user_id, name),
        CONSTRAINT fk_exp_cat_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_exp_cat_user ON expense_categories(user_id);")

    # 3) CATEGORÍAS INGRESO
    cur.execute("""
    CREATE TABLE IF NOT EXISTS income_categories (
        id SERIAL PRIMARY KEY,
        user_id INT NOT NULL,
        name VARCHAR(100) NOT NULL,
        created_at TIMESTAMP NOT NULL,
        CONSTRAINT uq_inc_cat_user_name UNIQUE (user_id, name),
        CONSTRAINT fk_inc_cat_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_inc_cat_user ON income_categories(user_id);")

    # 4) GASTOS
    cur.execute("""
    CREATE TABLE IF NOT EXISTS expenses (
        id SERIAL PRIMARY KEY,
        user_id INT NOT NULL,
        date DATE NOT NULL,
        amount DECIMAL(12,2) NOT NULL,
        category VARCHAR(100) NOT NULL,
        subcategory VARCHAR(100) NULL,
        payment_method VARCHAR(50) NULL,
        expense_type VARCHAR(50) NULL,
        notes TEXT NULL,
        created_at TIMESTAMP NOT NULL,
        CONSTRAINT fk_exp_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_exp_user_date ON expenses(user_id, date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_exp_user_cat  ON expenses(user_id, category);")

    # 5) INGRESOS
    cur.execute("""
    CREATE TABLE IF NOT EXISTS income (
        id SERIAL PRIMARY KEY,
        user_id INT NOT NULL,
        date DATE NOT NULL,
        amount DECIMAL(12,2) NOT NULL,
        source VARCHAR(150) NULL,
        category VARCHAR(100) NOT NULL,
        notes TEXT NULL,
        created_at TIMESTAMP NOT NULL,
        CONSTRAINT fk_inc_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_inc_user_date ON income(user_id, date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_inc_user_cat  ON income(user_id, category);")

    # 6) PRESUPUESTOS
    cur.execute("""
    CREATE TABLE IF NOT EXISTS budgets (
        id SERIAL PRIMARY KEY,
        user_id INT NOT NULL,
        category VARCHAR(100) NOT NULL,
        monthly_amount DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        created_at TIMESTAMP NOT NULL,
        updated_at TIMESTAMP NOT NULL,
        CONSTRAINT uq_budget_user_cat UNIQUE (user_id, category),
        CONSTRAINT fk_budget_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_budget_user ON budgets(user_id);")

    # 7) TIPOS DE ACTIVO
    cur.execute("""
    CREATE TABLE IF NOT EXISTS investment_asset_types (
        id SERIAL PRIMARY KEY,
        user_id INT NOT NULL,
        name VARCHAR(100) NOT NULL,
        created_at TIMESTAMP NOT NULL,
        CONSTRAINT uq_asset_type_user_name UNIQUE (user_id, name),
        CONSTRAINT fk_asset_type_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_asset_type_user ON investment_asset_types(user_id);")

    # 8) PRODUCTOS DE INVERSIÓN
    cur.execute("""
    CREATE TABLE IF NOT EXISTS investment_products (
        id SERIAL PRIMARY KEY,
        user_id INT NOT NULL,
        name VARCHAR(150) NOT NULL,
        asset_type VARCHAR(100) NOT NULL,
        created_at TIMESTAMP NOT NULL,
        CONSTRAINT uq_inv_prod_user_name UNIQUE (user_id, name),
        CONSTRAINT fk_inv_prod_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_inv_prod_user ON investment_products(user_id);")

    # 9) MOVIMIENTOS / SNAPSHOTS DE INVERSIÓN
    cur.execute("""
    CREATE TABLE IF NOT EXISTS investments (
        id SERIAL PRIMARY KEY,
        user_id INT NOT NULL,
        date DATE NOT NULL,
        account VARCHAR(150) NOT NULL,
        asset_type VARCHAR(100) NOT NULL,
        contribution DECIMAL(12,2) NOT NULL,
        current_value DECIMAL(12,2) NOT NULL,
        notes TEXT NULL,
        created_at TIMESTAMP NOT NULL,
        CONSTRAINT fk_inv_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_inv_user_date    ON investments(user_id, date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_inv_user_account ON investments(user_id, account);")

    # 10) SNAPSHOTS DE EFECTIVO
    cur.execute("""
    CREATE TABLE IF NOT EXISTS cash_snapshots (
        id SERIAL PRIMARY KEY,
        user_id INT NOT NULL,
        date DATE NOT NULL,
        account VARCHAR(150) NOT NULL,
        current_value DECIMAL(12,2) NOT NULL,
        notes TEXT NULL,
        created_at TIMESTAMP NOT NULL,
        CONSTRAINT uq_cash_user_date_account UNIQUE (user_id, date, account),
        CONSTRAINT fk_cash_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cash_user_date    ON cash_snapshots(user_id, date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cash_user_account ON cash_snapshots(user_id, account);")

    conn.commit()
    cur.close()
    conn.close()
    print("OK: esquema creado/actualizado (Postgres).")


if __name__ == "__main__":
    init_db()