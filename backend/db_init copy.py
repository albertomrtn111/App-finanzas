from datetime import datetime
import os
import mysql.connector

DB_HOST = os.getenv("DB_HOST", "mysql")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "appuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "APPPASS01")
DB_NAME = os.getenv("DB_NAME", "finanzas")

def get_conn():
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
    )

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # 1) USERS (compatible: password_hash puede ser NULL si el usuario venía de Google)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        email VARCHAR(255) NOT NULL UNIQUE,
        password_hash VARCHAR(255) NULL,
        google_sub VARCHAR(255) NULL UNIQUE,
        created_at DATETIME NOT NULL,
        last_login_at DATETIME NULL,
        INDEX idx_users_email (email)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    # 2) CATEGORÍAS GASTO
    cur.execute("""
    CREATE TABLE IF NOT EXISTS expense_categories (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL,
        name VARCHAR(100) NOT NULL,
        created_at DATETIME NOT NULL,
        UNIQUE KEY uq_exp_cat_user_name (user_id, name),
        CONSTRAINT fk_exp_cat_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        INDEX idx_exp_cat_user (user_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    # 3) CATEGORÍAS INGRESO
    cur.execute("""
    CREATE TABLE IF NOT EXISTS income_categories (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL,
        name VARCHAR(100) NOT NULL,
        created_at DATETIME NOT NULL,
        UNIQUE KEY uq_inc_cat_user_name (user_id, name),
        CONSTRAINT fk_inc_cat_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        INDEX idx_inc_cat_user (user_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    # 4) GASTOS
    cur.execute("""
    CREATE TABLE IF NOT EXISTS expenses (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL,
        date DATE NOT NULL,
        amount DECIMAL(12,2) NOT NULL,
        category VARCHAR(100) NOT NULL,
        subcategory VARCHAR(100) NULL,
        payment_method VARCHAR(50) NULL,
        expense_type VARCHAR(50) NULL,
        notes TEXT NULL,
        created_at DATETIME NOT NULL,
        INDEX idx_exp_user_date (user_id, date),
        INDEX idx_exp_user_cat (user_id, category),
        CONSTRAINT fk_exp_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    # 5) INGRESOS
    cur.execute("""
    CREATE TABLE IF NOT EXISTS income (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL,
        date DATE NOT NULL,
        amount DECIMAL(12,2) NOT NULL,
        source VARCHAR(150) NULL,
        category VARCHAR(100) NOT NULL,
        notes TEXT NULL,
        created_at DATETIME NOT NULL,
        INDEX idx_inc_user_date (user_id, date),
        INDEX idx_inc_user_cat (user_id, category),
        CONSTRAINT fk_inc_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    # 6) PRESUPUESTOS
    cur.execute("""
    CREATE TABLE IF NOT EXISTS budgets (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL,
        category VARCHAR(100) NOT NULL,
        monthly_amount DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        UNIQUE KEY uq_budget_user_cat (user_id, category),
        CONSTRAINT fk_budget_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        INDEX idx_budget_user (user_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    # 7) TIPOS DE ACTIVO
    cur.execute("""
    CREATE TABLE IF NOT EXISTS investment_asset_types (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL,
        name VARCHAR(100) NOT NULL,
        created_at DATETIME NOT NULL,
        UNIQUE KEY uq_asset_type_user_name (user_id, name),
        CONSTRAINT fk_asset_type_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        INDEX idx_asset_type_user (user_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    # 8) PRODUCTOS DE INVERSIÓN
    cur.execute("""
    CREATE TABLE IF NOT EXISTS investment_products (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL,
        name VARCHAR(150) NOT NULL,
        asset_type VARCHAR(100) NOT NULL,
        created_at DATETIME NOT NULL,
        UNIQUE KEY uq_inv_prod_user_name (user_id, name),
        CONSTRAINT fk_inv_prod_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        INDEX idx_inv_prod_user (user_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    # 9) MOVIMIENTOS / SNAPSHOTS DE INVERSIÓN
    cur.execute("""
    CREATE TABLE IF NOT EXISTS investments (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL,
        date DATE NOT NULL,
        account VARCHAR(150) NOT NULL,
        asset_type VARCHAR(100) NOT NULL,
        contribution DECIMAL(12,2) NOT NULL,
        current_value DECIMAL(12,2) NOT NULL,
        notes TEXT NULL,
        created_at DATETIME NOT NULL,
        INDEX idx_inv_user_date (user_id, date),
        INDEX idx_inv_user_account (user_id, account),
        CONSTRAINT fk_inv_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    # 10) SNAPSHOTS DE EFECTIVO (recupero UNIQUE por (user_id, date, account))
    cur.execute("""
    CREATE TABLE IF NOT EXISTS cash_snapshots (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL,
        date DATE NOT NULL,
        account VARCHAR(150) NOT NULL,
        current_value DECIMAL(12,2) NOT NULL,
        notes TEXT NULL,
        created_at DATETIME NOT NULL,
        UNIQUE KEY uq_cash_user_date_account (user_id, date, account),
        INDEX idx_cash_user_date (user_id, date),
        INDEX idx_cash_user_account (user_id, account),
        CONSTRAINT fk_cash_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("OK: esquema creado/actualizado (compatible).")

if __name__ == "__main__":
    init_db()