import streamlit as st
import pandas as pd
from datetime import date, datetime
import altair as alt
import bcrypt


from backend.db_connection import get_connection


# --------- FUNCIONES BBDD --------- #

def insert_income(user_id, date_val, amount, source, category, notes):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO income (user_id, date, amount, source, category, notes, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            int(user_id),
            date_val,
            float(amount),
            source if source else None,
            category,
            notes if notes else None,
            datetime.now(),
        ),
    )
    conn.commit()
    cursor.close()
    conn.close()


def update_income(user_id, income_id: int, date_val, amount, source, category, notes):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE income
        SET date = %s,
            amount = %s,
            source = %s,
            category = %s,
            notes = %s
        WHERE id = %s AND user_id = %s
        """,
        (
            date_val,
            float(amount),
            source if source else None,
            category,
            notes if notes else None,
            int(income_id),
            int(user_id),
        ),
    )
    conn.commit()
    cursor.close()
    conn.close()


def insert_expense(user_id, date_val, amount, category, subcategory, payment_method, expense_type, notes):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO expenses
        (user_id, date, amount, category, subcategory, payment_method, expense_type, notes, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            int(user_id),
            date_val,
            float(amount),
            category,
            subcategory if subcategory else None,
            payment_method,
            expense_type,
            notes if notes else None,
            datetime.now(),
        ),
    )
    conn.commit()
    cursor.close()
    conn.close()


def update_expense(
    user_id,
    expense_id: int,
    date_val,
    amount,
    category,
    subcategory,
    payment_method,
    expense_type,
    notes,
):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE expenses
        SET date = %s,
            amount = %s,
            category = %s,
            subcategory = %s,
            payment_method = %s,
            expense_type = %s,
            notes = %s
        WHERE id = %s AND user_id = %s
        """,
        (
            date_val,
            float(amount),
            category,
            subcategory if subcategory else None,
            payment_method if payment_method else None,
            expense_type if expense_type else None,
            notes if notes else None,
            int(expense_id),
            int(user_id),
        ),
    )
    conn.commit()
    cursor.close()
    conn.close()


def get_all_income(user_id):
    conn = get_connection()
    df = pd.read_sql(
        """
        SELECT id, date, amount, source, category, notes
        FROM income
        WHERE user_id = %(user_id)s
        ORDER BY date DESC, id DESC
        """,
        conn,
        params={"user_id": int(user_id)},
    )
    conn.close()
    return df


def get_all_expenses(user_id):
    conn = get_connection()
    df = pd.read_sql(
        """
        SELECT id, date, amount, category, subcategory, payment_method, expense_type, notes
        FROM expenses
        WHERE user_id = %(user_id)s
        ORDER BY date DESC, id DESC
        """,
        conn,
        params={"user_id": int(user_id)},
    )
    conn.close()
    return df


def delete_expense(user_id, expense_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM expenses WHERE id = %s AND user_id = %s",
        (int(expense_id), int(user_id)),
    )
    conn.commit()
    cursor.close()
    conn.close()


def delete_income(user_id, income_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM income WHERE id = %s AND user_id = %s",
        (int(income_id), int(user_id)),
    )
    conn.commit()
    cursor.close()
    conn.close()


def get_expense_categories(user_id):
    conn = get_connection()
    df = pd.read_sql(
        """
        SELECT id, name
        FROM expense_categories
        WHERE user_id = %(user_id)s
        ORDER BY name
        """,
        conn,
        params={"user_id": int(user_id)},
    )
    conn.close()
    return df


def add_expense_category(user_id, name: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO expense_categories (user_id, name, created_at)
        VALUES (%s, %s, %s)
        """,
        (int(user_id), name, datetime.now()),
    )
    conn.commit()
    cursor.close()
    conn.close()


def delete_expense_category(user_id, name: str):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT COUNT(*) FROM expenses WHERE user_id = %s AND category = %s",
        (int(user_id), name),
    )
    used_exp = cursor.fetchone()[0]

    cursor.execute(
        "SELECT COUNT(*) FROM budgets WHERE user_id = %s AND category = %s",
        (int(user_id), name),
    )
    used_bud = cursor.fetchone()[0]

    if used_exp > 0 or used_bud > 0:
        raise ValueError("No se puede eliminar la categoría porque está en uso.")

    cursor.execute(
        "DELETE FROM expense_categories WHERE user_id = %s AND name = %s",
        (int(user_id), name),
    )
    conn.commit()
    cursor.close()
    conn.close()

def update_expense_category(user_id: int, old_name: str, new_name: str):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        # 1) Renombrar en tabla de categorías
        cursor.execute(
            """
            UPDATE expense_categories
            SET name = %s
            WHERE user_id = %s AND name = %s
            """,
            (new_name, int(user_id), old_name),
        )

        # 2) Actualizar gastos existentes
        cursor.execute(
            """
            UPDATE expenses
            SET category = %s
            WHERE user_id = %s AND category = %s
            """,
            (new_name, int(user_id), old_name),
        )

        # 3) Actualizar presupuestos existentes
        cursor.execute(
            """
            UPDATE budgets
            SET category = %s
            WHERE user_id = %s AND category = %s
            """,
            (new_name, int(user_id), old_name),
        )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def get_income_categories(user_id):
    conn = get_connection()
    df = pd.read_sql(
        """
        SELECT id, name
        FROM income_categories
        WHERE user_id = %(user_id)s
        ORDER BY name
        """,
        conn,
        params={"user_id": int(user_id)},
    )
    conn.close()
    return df



def add_income_category(user_id, name: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO income_categories (user_id, name, created_at)
        VALUES (%s, %s, %s)
        """,
        (int(user_id), name, datetime.now()),
    )
    conn.commit()
    cursor.close()
    conn.close()


def delete_income_category(user_id, name: str):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT COUNT(*) FROM income WHERE user_id = %s AND category = %s",
        (int(user_id), name),
    )
    used_inc = cursor.fetchone()[0]

    if used_inc > 0:
        raise ValueError("No se puede eliminar la categoría porque está en uso.")

    cursor.execute(
        "DELETE FROM income_categories WHERE user_id = %s AND name = %s",
        (int(user_id), name),
    )
    conn.commit()
    cursor.close()
    conn.close()

def update_income_category(user_id: int, old_name: str, new_name: str):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        # 1) Renombrar en tabla de categorías
        cursor.execute(
            """
            UPDATE income_categories
            SET name = %s
            WHERE user_id = %s AND name = %s
            """,
            (new_name, int(user_id), old_name),
        )

        # 2) Actualizar ingresos existentes
        cursor.execute(
            """
            UPDATE income
            SET category = %s
            WHERE user_id = %s AND category = %s
            """,
            (new_name, int(user_id), old_name),
        )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def insert_investment(user_id, date_val, account, asset_type, contribution, current_value, notes):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO investments
        (user_id, date, account, asset_type, contribution, current_value, notes, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            int(user_id),
            date_val,
            account,
            asset_type,
            float(contribution),
            float(current_value),
            notes if notes else None,
            datetime.now(),
        ),
    )
    conn.commit()
    cursor.close()
    conn.close()


def get_investments_df(user_id):
    conn = get_connection()
    df = pd.read_sql(
        """
        SELECT *
        FROM investments
        WHERE user_id = %(user_id)s
        """,
        conn,
        params={"user_id": int(user_id)},
    )
    conn.close()
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df


def get_all_investments(user_id):
    conn = get_connection()
    df = pd.read_sql(
        """
        SELECT *
        FROM investments
        WHERE user_id = %(user_id)s
        ORDER BY date DESC, id DESC
        """,
        conn,
        params={"user_id": int(user_id)},
    )
    conn.close()
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df


def delete_investment(user_id, inv_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM investments WHERE id = %s AND user_id = %s",
        (int(inv_id), int(user_id)),
    )
    conn.commit()
    cursor.close()
    conn.close()

def update_investment(
    user_id: int,
    inv_id: int,
    date_val,
    account: str,
    asset_type: str,
    contribution: float,
    current_value: float,
    notes=None,
):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE investments
        SET date = %s,
            account = %s,
            asset_type = %s,
            contribution = %s,
            current_value = %s,
            notes = %s
        WHERE id = %s AND user_id = %s
        """,
        (
            date_val,
            account,
            asset_type,
            float(contribution),
            float(current_value),
            notes if notes else None,
            int(inv_id),
            int(user_id),
        ),
    )
    conn.commit()
    cursor.close()
    conn.close()


def get_investment_products(user_id):
    conn = get_connection()
    df = pd.read_sql(
        """
        SELECT *
        FROM investment_products
        WHERE user_id = %(user_id)s
        ORDER BY name
        """,
        conn,
        params={"user_id": int(user_id)},
    )
    conn.close()
    return df


def update_investment(
    user_id: int,
    inv_id: int,
    date_val,
    account: str,
    asset_type: str,
    contribution: float,
    current_value: float,
    notes=None,
):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE investments
        SET date = %s,
            account = %s,
            asset_type = %s,
            contribution = %s,
            current_value = %s,
            notes = %s
        WHERE id = %s AND user_id = %s
        """,
        (
            date_val,
            account,
            asset_type,
            float(contribution),
            float(current_value),
            notes if notes else None,
            int(inv_id),
            int(user_id),
        ),
    )
    conn.commit()
    cursor.close()
    conn.close()

def get_asset_types(user_id):
    conn = get_connection()
    df = pd.read_sql(
        """
        SELECT name
        FROM investment_asset_types
        WHERE user_id = %(user_id)s
        ORDER BY name
        """,
        conn,
        params={"user_id": int(user_id)},
    )
    conn.close()
    return df["name"].tolist() if not df.empty else []


def insert_investment_product(user_id, name, asset_type):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO investment_products (user_id, name, asset_type, created_at)
        VALUES (%s, %s, %s, %s)
        """,
        (int(user_id), name, asset_type, datetime.now()),
    )
    conn.commit()
    cursor.close()
    conn.close()


def delete_investment_product(user_id, prod_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM investment_products WHERE id = %s AND user_id = %s",
        (int(prod_id), int(user_id)),
    )
    conn.commit()
    cursor.close()
    conn.close()

def update_investment_product(user_id, prod_id: int, name: str, asset_type: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE investment_products
        SET name = %s,
            asset_type = %s
        WHERE id = %s AND user_id = %s
        """,
        (
            name,
            asset_type,
            int(prod_id),
            int(user_id),
        ),
    )
    conn.commit()
    cursor.close()
    conn.close()


def insert_cash_snapshot(user_id, date_val, account, current_value, notes=None):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO cash_snapshots
        (user_id, date, account, current_value, notes, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (int(user_id), date_val, account, float(current_value), notes, datetime.now()),
    )
    conn.commit()
    cursor.close()
    conn.close()


def get_cash_df(user_id):
    conn = get_connection()
    df = pd.read_sql(
        """
        SELECT *
        FROM cash_snapshots
        WHERE user_id = %(user_id)s
        """,
        conn,
        params={"user_id": int(user_id)},
    )
    conn.close()
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df


def get_all_cash(user_id):
    conn = get_connection()
    df = pd.read_sql(
        """
        SELECT *
        FROM cash_snapshots
        WHERE user_id = %(user_id)s
        ORDER BY date DESC, id DESC
        """,
        conn,
        params={"user_id": int(user_id)},
    )
    conn.close()
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df


def delete_cash_snapshot(user_id, cash_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM cash_snapshots WHERE id = %s AND user_id = %s",
        (int(cash_id), int(user_id)),
    )
    conn.commit()
    cursor.close()
    conn.close()

def update_cash_snapshot(
    user_id: int,
    cash_id: int,
    date_val,
    account: str,
    current_value: float,
    notes=None,
):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE cash_snapshots
        SET date = %s,
            account = %s,
            current_value = %s,
            notes = %s
        WHERE id = %s AND user_id = %s
        """,
        (
            date_val,
            account,
            float(current_value),
            notes if notes else None,
            int(cash_id),
            int(user_id),
        ),
    )
    conn.commit()
    cursor.close()
    conn.close()

# --------- USUARIOS --------- #

def get_user_by_email(email: str):
    conn = get_connection()
    df = pd.read_sql(
        """
        SELECT *
        FROM users
        WHERE email = %(email)s
        LIMIT 1
        """,
        conn,
        params={"email": email},
    )
    conn.close()
    return df.iloc[0].to_dict() if not df.empty else None


def get_users(limit: int = 200):
    conn = get_connection()
    df = pd.read_sql(
        """
        SELECT id, email, created_at, last_login_at
        FROM users
        ORDER BY created_at DESC, id DESC
        LIMIT %(limit)s
        """,
        conn,
        params={"limit": int(limit)},
    )
    conn.close()
    return df
            

def create_user_email_password(email: str, password: str):
    pw_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO users (email, password_hash, created_at)
        VALUES (%s, %s, %s)
        """,
        (email.strip().lower(), pw_hash, datetime.now()),
    )
    conn.commit()
    cursor.close()
    conn.close()


def verify_user_password(email: str, password: str):
    user = get_user_by_email(email.strip().lower())
    if not user:
        return None

    stored = user.get("password_hash")
    if not stored:
        return None

    ok = bcrypt.checkpw(password.encode("utf-8"), stored.encode("utf-8"))
    return user if ok else None


def set_last_login(user_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE users SET last_login_at = %s WHERE id = %s",
        (datetime.now(), int(user_id)),
    )
    conn.commit()
    cursor.close()
    conn.close()



# --------- CONFIGURACIÓN PÁGINA --------- #
st.set_page_config(page_title="Finanzas personales", layout="wide")

# Estado de sesión
if "user_id" not in st.session_state:
    st.session_state["user_id"] = None
if "user_email" not in st.session_state:
    st.session_state["user_email"] = None
if "menu" not in st.session_state:
    st.session_state["menu"] = "Inicio"


# --------- AUTH (LOGIN / SIGNUP) --------- #

def authenticate(email: str, password: str):
    user = verify_user_password(email, password)
    if not user:
        return None
    try:
        uid = int(user["id"])
    except Exception:
        return None
    set_last_login(uid)
    st.session_state["user_email"] = user.get("email")
    return uid


def require_login() -> int:
    if st.session_state.get("user_id") is None:
        st.title("Acceso")

        tab_login, tab_signup = st.tabs(["Entrar", "Crear cuenta"])

        with tab_login:
            with st.form("login_form"):
                email = st.text_input("Email")
                password = st.text_input("Contraseña", type="password")
                ok = st.form_submit_button("Entrar")

            if ok:
                uid = authenticate(email.strip().lower(), password)
                if uid is None:
                    st.error("Credenciales inválidas.")
                else:
                    st.session_state["user_id"] = uid
                    st.rerun()

        with tab_signup:
            with st.form("signup_form"):
                email2 = st.text_input("Email", key="signup_email")
                password2 = st.text_input("Contraseña", type="password", key="signup_password")
                password3 = st.text_input("Repite contraseña", type="password", key="signup_password2")
                ok2 = st.form_submit_button("Crear cuenta")

            if ok2:
                email2 = email2.strip().lower()
                if not email2:
                    st.error("El email es obligatorio.")
                elif password2 != password3:
                    st.error("Las contraseñas no coinciden.")
                else:
                    existing = get_user_by_email(email2)
                    if existing:
                        st.error("Ese email ya existe.")
                    else:
                        try:
                            create_user_email_password(email2, password2)
                            st.success("Cuenta creada. Ya puedes entrar en la pestaña 'Entrar'.")
                        except Exception as e:
                            st.error(f"No se pudo crear la cuenta: {e}")

        st.stop()

    return int(st.session_state["user_id"])


# --------- REQUIRE LOGIN (BLOQUEANTE) --------- #
user_id = require_login()

# --------- ESTADO INICIAL DEL MENÚ (evita KeyError) --------- #
st.session_state.setdefault("menu", "Inicio")

# ==============================================================================================================================
#       VISTA: MENU LATERAL (ROBUSTO + COMPATIBLE CON BOTONES DE INICIO)
# ==============================================================================================================================

# --------- ESTADO INICIAL DEL MENÚ --------- #
if "menu" not in st.session_state:
    st.session_state["menu"] = "Inicio"

# Páginas
pages = [
    ("🏠 Inicio", "Inicio"),
    ("➕ Registro", "Registro"),
    ("📊 Resumen", "Resumen"),
    ("📈 Registro inversiones", "Registro inversiones"),
    ("📊 Resumen inversiones", "Resumen inversiones"),
    ("💶 Registro efectivo", "Registro efectivo"),
    ("💼 Patrimonio", "Patrimonio"),
    ("💰 Presupuestos", "Presupuestos"),
    ("🏷️ Categorías", "Categorías"),
    ("💼 Productos de inversión", "Productos de inversión"),
    ("📥 Importar datos", "Importar datos"),
]

labels = [p[0] for p in pages]
values = [p[1] for p in pages]

label_to_value = {label: value for (label, value) in pages}
value_to_label = {value: label for (label, value) in pages}

# Sanear menu si viene corrupto
if st.session_state.get("menu") not in values:
    st.session_state["menu"] = "Inicio"

# Inicializar el estado del radio SIEMPRE antes de crearlo
st.session_state.setdefault(
    "sidebar_menu_radio",
    value_to_label[st.session_state["menu"]],
)

# -------------------------------------------------
#  APLICAR NAVEGACIÓN PENDIENTE (DESDE BOTONES)
#  IMPORTANTE: ANTES DE CREAR EL RADIO
# -------------------------------------------------
if st.session_state.get("pending_menu"):
    target = st.session_state.pop("pending_menu")
    if target in values:
        st.session_state["menu"] = target
        st.session_state["sidebar_menu_radio"] = value_to_label[target]

# Índice del radio según la página actual
try:
    current_index = labels.index(st.session_state["sidebar_menu_radio"])
except ValueError:
    current_index = 0
    st.session_state["sidebar_menu_radio"] = labels[0]
    st.session_state["menu"] = label_to_value[labels[0]]

# -----------------------
#   SIDEBAR
# -----------------------
st.sidebar.title("Menú")

choice_label = st.sidebar.radio(
    "",
    labels,
    index=current_index,
    label_visibility="collapsed",
    key="sidebar_menu_radio",
)

# Mapear selección del sidebar al menú activo
st.session_state["menu"] = label_to_value[choice_label]
menu = st.session_state["menu"]

st.title("App de finanzas personales")

# ========================================================================================================================================================================================
#       VISTA: VISTA DE INICIO
# ========================================================================================================================================================================================

if menu == "Inicio":
    st.markdown("Selecciona una opción en alguno de los bloques:")

    # Helper de navegación: sincroniza menu + radio lateral
    def go(page_value: str):
        st.session_state["pending_menu"] = page_value
        st.rerun()

    col_bal, col_inv, col_pat = st.columns(3)

    # -----------------------------
    #   COLUMNA 1: BALANCE (INGRESOS/GASTOS)
    # -----------------------------
    with col_bal:
        st.markdown("### 💶 Balance")

        b_reg_ig = st.button(
            "➕ Registrar gastos / ingresos",
            use_container_width=True,
            key="btn_reg_ig",
        )

        b_res_ig = st.button(
            "📊 Resumen de mis gastos",
            use_container_width=True,
            key="btn_res_ig",
        )

    # -----------------------------
    #   COLUMNA 2: INVERSIONES
    # -----------------------------
    with col_inv:
        st.markdown("### 📈 Inversiones")

        b_reg_inv = st.button(
            "📈 Registrar inversiones",
            use_container_width=True,
            key="btn_reg_inv",
        )

        b_res_inv = st.button(
            "📊 Resumen de mis inversiones",
            use_container_width=True,
            key="btn_res_inv",
        )

    # -----------------------------
    #   COLUMNA 3: PATRIMONIO
    # -----------------------------
    with col_pat:
        st.markdown("### 💼 Patrimonio")

        b_reg_cash = st.button(
            "💶 Registrar efectivo",
            use_container_width=True,
            key="btn_reg_cash",
        )

        b_pat = st.button(
            "💼 Ver patrimonio",
            use_container_width=True,
            key="btn_pat",
        )

    st.markdown("---")

    # -----------------------------
    #   CONFIGURACIÓN (CARDS)
    # -----------------------------
    st.markdown("### ⚙️ Configuración")
    st.caption("Ajusta presupuestos, categorías y configura datos base de la aplicación.")

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        with st.container(border=True):
            st.markdown("**💰 Presupuestos**")
            b_pres = st.button("Abrir", use_container_width=True, key="btn_pres")

    with c2:
        with st.container(border=True):
            st.markdown("**🏷️ Categorías**")
            b_cats = st.button("Abrir", use_container_width=True, key="btn_cats")

    with c3:
        with st.container(border=True):
            st.markdown("**📥 Importar datos**")
            b_imp = st.button("Abrir", use_container_width=True, key="btn_import")

    with c4:
        with st.container(border=True):
            st.markdown("**💼 Productos de inversión**")
            b_prod = st.button("Abrir", use_container_width=True, key="btn_prod")

    # -------- LÓGICA DE NAVEGACIÓN --------
    if b_reg_ig:
        go("Registro")

    if b_res_ig:
        go("Resumen")

    if b_reg_inv:
        go("Registro inversiones")

    if b_res_inv:
        go("Resumen inversiones")

    if b_reg_cash:
        go("Registro efectivo")

    if b_pat:
        go("Patrimonio")

    if b_pres:
        go("Presupuestos")

    if b_cats:
        go("Categorías")

    if b_prod:
        go("Productos de inversión")

    if b_imp:
        go("Importar datos")

# ======================================================================================================================
#       VISTA: REGISTRO
# ======================================================================================================================

elif menu == "Registro":
    st.subheader("Registro de movimientos")

    # Estado de edición
    if "edit_expense_id" not in st.session_state:
        st.session_state["edit_expense_id"] = None
    if "edit_income_id" not in st.session_state:
        st.session_state["edit_income_id"] = None

    modo = st.radio(
        "¿Qué quieres registrar / consultar?",
        ["Gastos", "Ingresos"],
        horizontal=True,
    )

    # ======================================================================
    #   GASTOS
    # ======================================================================
    if modo == "Gastos":
        st.header("Registro de gastos")

        df_cats = get_expense_categories(user_id)

        if df_cats.empty:
            st.warning(
                "No tienes categorías de gasto definidas. "
                "Ve a la pestaña 'Categorías' para crear al menos una."
            )
        else:
            # -------------------------
            # CREAR GASTO
            # -------------------------
            with st.form("form_gasto_create"):
                col1, col2 = st.columns(2)

                with col1:
                    fecha = st.date_input("Fecha", value=date.today())
                    importe = st.number_input("Importe (€)", min_value=0.0, step=1.0)
                    categoria = st.selectbox(
                        "Categoría",
                        df_cats["name"].tolist(),
                    )

                with col2:
                    subcategoria = st.text_input("Subcategoría (opcional)")
                    payment_method = st.selectbox(
                        "Método de pago",
                        ["Tarjeta", "Efectivo", "Bizum", "Transferencia", "Otro"],
                    )
                    expense_type = st.selectbox(
                        "Tipo de gasto",
                        ["Variable", "Fijo"],
                    )

                notas = st.text_area("Notas (opcional)", height=80)

                save_new = st.form_submit_button("Guardar gasto")

            if save_new:
                if importe <= 0:
                    st.error("El importe debe ser mayor que 0.")
                else:
                    insert_expense(
                        user_id,
                        fecha,
                        importe,
                        categoria,
                        subcategoria.strip() or None,
                        payment_method,
                        expense_type,
                        notas.strip() or None,
                    )
                    st.success("Gasto guardado correctamente.")
                    st.rerun()

        # -------------------------
        # LISTADO
        # -------------------------
        st.subheader("Últimos gastos")

        df_exp = get_all_expenses(user_id)

        if df_exp.empty:
            st.info("No hay gastos registrados.")
        else:
            st.dataframe(df_exp, use_container_width=True)

            # -------------------------
            # EDITAR GASTO
            # -------------------------
            with st.expander("Editar un gasto"):
                options = {
                    f"{r['id']} - {r['date']} - {r['amount']} € - {r['category']}": int(r["id"])
                    for _, r in df_exp.iterrows()
                }

                sel = st.selectbox(
                    "Selecciona un gasto",
                    list(options.keys()),
                )

                if st.button("Editar"):
                    st.session_state["edit_expense_id"] = options[sel]
                    st.rerun()

                edit_id = st.session_state["edit_expense_id"]

                if edit_id:
                    row = df_exp[df_exp["id"] == edit_id].iloc[0]

                    cat_list = df_cats["name"].tolist()

                    with st.form("form_edit_expense"):
                        col1, col2 = st.columns(2)

                        with col1:
                            fecha_e = st.date_input("Fecha", pd.to_datetime(row["date"]).date())
                            importe_e = st.number_input(
                                "Importe (€)", min_value=0.0, value=float(row["amount"])
                            )
                            categoria_e = st.selectbox(
                                "Categoría",
                                cat_list,
                                index=cat_list.index(row["category"])
                                if row["category"] in cat_list else 0,
                            )

                        with col2:
                            subcat_e = st.text_input("Subcategoría", value=row["subcategory"] or "")
                            pay_e = st.selectbox(
                                "Método de pago",
                                ["Tarjeta", "Efectivo", "Bizum", "Transferencia", "Otro"],
                                index=["Tarjeta", "Efectivo", "Bizum", "Transferencia", "Otro"]
                                .index(row["payment_method"])
                                if row["payment_method"] in ["Tarjeta", "Efectivo", "Bizum", "Transferencia", "Otro"]
                                else 0,
                            )
                            type_e = st.selectbox(
                                "Tipo de gasto",
                                ["Variable", "Fijo"],
                                index=["Variable", "Fijo"].index(row["expense_type"])
                                if row["expense_type"] in ["Variable", "Fijo"]
                                else 0,
                            )

                        notes_e = st.text_area("Notas", value=row["notes"] or "")

                        colb1, colb2 = st.columns(2)
                        save = st.form_submit_button("Guardar cambios")
                        cancel = st.form_submit_button("Cancelar")

                    if cancel:
                        st.session_state["edit_expense_id"] = None
                        st.rerun()

                    if save:
                        update_expense(
                            user_id,
                            edit_id,
                            fecha_e,
                            importe_e,
                            categoria_e,
                            subcat_e.strip() or None,
                            pay_e,
                            type_e,
                            notes_e.strip() or None,
                        )
                        st.success("Gasto actualizado.")
                        st.session_state["edit_expense_id"] = None
                        st.rerun()

            # -------------------------
            # ELIMINAR
            # -------------------------
            with st.expander("Eliminar un gasto"):
                sel_del = st.selectbox(
                    "Selecciona gasto a eliminar",
                    list(options.keys()),
                    key="del_exp",
                )
                if st.button("Eliminar"):
                    delete_expense(user_id, options[sel_del])
                    st.success("Gasto eliminado.")
                    st.rerun()

    # ======================================================================
    #   INGRESOS
    # ======================================================================
    else:
        st.header("Registro de ingresos")

        df_inc_cats = get_income_categories(user_id)

        if df_inc_cats.empty:
            st.warning(
                "No tienes categorías de ingreso definidas. "
                "Ve a la pestaña 'Categorías' para crear al menos una."
            )
        else:
            # -------------------------
            # CREAR INGRESO
            # -------------------------
            with st.form("form_ingreso_create"):
                col1, col2 = st.columns(2)

                with col1:
                    fecha = st.date_input("Fecha", value=date.today())
                    importe = st.number_input("Importe (€)", min_value=0.0, step=1.0)

                with col2:
                    source = st.text_input("Fuente")
                    category = st.selectbox(
                        "Categoría",
                        df_inc_cats["name"].tolist(),
                    )

                notas = st.text_area("Notas (opcional)", height=80)

                save_inc = st.form_submit_button("Guardar ingreso")

            if save_inc:
                if importe <= 0:
                    st.error("El importe debe ser mayor que 0.")
                else:
                    insert_income(
                        user_id,
                        fecha,
                        importe,
                        source.strip() or None,
                        category,
                        notas.strip() or None,
                    )
                    st.success("Ingreso guardado.")
                    st.rerun()

        # -------------------------
        # LISTADO
        # -------------------------
        st.subheader("Últimos ingresos")

        df_inc = get_all_income(user_id)

        if df_inc.empty:
            st.info("No hay ingresos registrados.")
        else:
            st.dataframe(df_inc, use_container_width=True)

            # -------------------------
            # EDITAR INGRESO
            # -------------------------
            with st.expander("Editar un ingreso"):
                options = {
                    f"{r['id']} - {r['date']} - {r['amount']} € - {r['category']}": int(r["id"])
                    for _, r in df_inc.iterrows()
                }

                sel = st.selectbox("Selecciona ingreso", list(options.keys()))

                if st.button("Editar ingreso"):
                    st.session_state["edit_income_id"] = options[sel]
                    st.rerun()

                edit_id = st.session_state["edit_income_id"]

                if edit_id:
                    row = df_inc[df_inc["id"] == edit_id].iloc[0]
                    cat_list = df_inc_cats["name"].tolist()

                    with st.form("form_edit_income"):
                        col1, col2 = st.columns(2)

                        with col1:
                            fecha_e = st.date_input("Fecha", pd.to_datetime(row["date"]).date())
                            importe_e = st.number_input(
                                "Importe (€)", min_value=0.0, value=float(row["amount"])
                            )

                        with col2:
                            source_e = st.text_input("Fuente", value=row["source"] or "")
                            category_e = st.selectbox(
                                "Categoría",
                                cat_list,
                                index=cat_list.index(row["category"])
                                if row["category"] in cat_list else 0,
                            )

                        notes_e = st.text_area("Notas", value=row["notes"] or "")

                        colb1, colb2 = st.columns(2)
                        save = st.form_submit_button("Guardar cambios")
                        cancel = st.form_submit_button("Cancelar")

                    if cancel:
                        st.session_state["edit_income_id"] = None
                        st.rerun()

                    if save:
                        update_income(
                            user_id,
                            edit_id,
                            fecha_e,
                            importe_e,
                            source_e.strip() or None,
                            category_e,
                            notes_e.strip() or None,
                        )
                        st.success("Ingreso actualizado.")
                        st.session_state["edit_income_id"] = None
                        st.rerun()

            # -------------------------
            # ELIMINAR
            # -------------------------
            with st.expander("Eliminar un ingreso"):
                sel_del = st.selectbox(
                    "Selecciona ingreso a eliminar",
                    list(options.keys()),
                    key="del_inc",
                )
                if st.button("Eliminar ingreso"):
                    delete_income(user_id, options[sel_del])
                    st.success("Ingreso eliminado.")
                    st.rerun()

# ========================================================================================================================================================================================
#       VISTA: RESUMEN (MULTIUSUARIO)
# ========================================================================================================================================================================================

elif menu == "Resumen":
    st.subheader("Resumen de tus finanzas")

    conn = get_connection()

    df_income = pd.read_sql(
        "SELECT * FROM income WHERE user_id = %s",
        conn,
        params=(int(user_id),),
    )

    df_expenses = pd.read_sql(
        "SELECT * FROM expenses WHERE user_id = %s",
        conn,
        params=(int(user_id),),
    )

    # Presupuestos del usuario
    df_budgets = pd.read_sql(
        "SELECT category, monthly_amount FROM budgets WHERE user_id = %s",
        conn,
        params=(int(user_id),),
    )

    conn.close()

    if df_income.empty and df_expenses.empty:
        st.info("Todavía no hay datos de ingresos ni gastos para mostrar el resumen.")
    else:
        # Asegurar datetime
        if not df_income.empty:
            df_income["date"] = pd.to_datetime(df_income["date"], errors="coerce")
            df_income = df_income.dropna(subset=["date"])
        if not df_expenses.empty:
            df_expenses["date"] = pd.to_datetime(df_expenses["date"], errors="coerce")
            df_expenses = df_expenses.dropna(subset=["date"])

        # Años disponibles
        years = []
        if not df_income.empty:
            years.extend(df_income["date"].dt.year.tolist())
        if not df_expenses.empty:
            years.extend(df_expenses["date"].dt.year.tolist())

        years = sorted(set(years), reverse=True)

        if not years:
            st.info("No hay fechas válidas para construir el resumen.")
        else:
            selected_year = st.selectbox("Año", years, index=0)

            # Meses disponibles en ese año
            months = []
            if not df_income.empty:
                months.extend(
                    df_income.loc[df_income["date"].dt.year == selected_year, "date"].dt.month.tolist()
                )
            if not df_expenses.empty:
                months.extend(
                    df_expenses.loc[df_expenses["date"].dt.year == selected_year, "date"].dt.month.tolist()
                )
            months = sorted(set(months))

            if not months:
                st.info("No hay datos en el año seleccionado.")
            else:
                month_names = {
                    1: "enero", 2: "febrero", 3: "marzo", 4: "abril",
                    5: "mayo", 6: "junio", 7: "julio", 8: "agosto",
                    9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre",
                }

                month_labels = [month_names[m].capitalize() for m in months]
                label_to_month = dict(zip(month_labels, months))

                default_labels: list[str] = []

                col_f1, col_f2 = st.columns([2, 1])
                with col_f1:
                    selected_month_labels = st.multiselect(
                        "Meses (puedes seleccionar uno, varios o ninguno si usas el año completo)",
                        month_labels,
                        default=default_labels,
                    )
                with col_f2:
                    use_full_year = st.checkbox("Usar año completo", value=False)

                if use_full_year or not selected_month_labels:
                    months_filter = months
                    periodo_texto = f"Año completo {selected_year}"
                    n_months_budget = 12
                else:
                    months_filter = [label_to_month[l] for l in selected_month_labels]
                    meses_txt = ", ".join([month_names[m].capitalize() for m in months_filter])
                    periodo_texto = f"{meses_txt} de {selected_year}"
                    n_months_budget = len(set(months_filter))

                st.markdown(f"**Periodo seleccionado:** {periodo_texto}")

                def filter_df(df):
                    if df.empty:
                        return df
                    mask = df["date"].dt.year == selected_year
                    if months_filter:
                        mask &= df["date"].dt.month.isin(months_filter)
                    return df[mask]

                df_income_period = filter_df(df_income)
                df_expenses_period = filter_df(df_expenses)

                # DF solo por año (serie ahorro)
                df_income_year = df_income[df_income["date"].dt.year == selected_year] if not df_income.empty else df_income
                df_expenses_year = df_expenses[df_expenses["date"].dt.year == selected_year] if not df_expenses.empty else df_expenses

                # KPIs
                total_income = float(df_income_period["amount"].sum()) if not df_income_period.empty else 0.0
                total_expenses = float(df_expenses_period["amount"].sum()) if not df_expenses_period.empty else 0.0
                savings = total_income - total_expenses
                savings_rate = (savings / total_income * 100.0) if total_income > 0 else 0.0

                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Ingresos", f"{total_income:,.2f} €")
                with col2:
                    st.metric("Gastos", f"{total_expenses:,.2f} €")
                with col3:
                    st.metric("Ahorro", f"{savings:,.2f} €")
                with col4:
                    bg_color = "#d4edda" if savings_rate >= 0 else "#f8d7da"
                    text_color = "#155724" if savings_rate >= 0 else "#721c24"
                    st.markdown(
                        f"""
                        <div style="padding:0.75rem;border-radius:0.5rem;background-color:{bg_color};text-align:center;">
                            <div style="font-size:0.9rem;">Tasa de ahorro</div>
                            <div style="font-size:1.3rem;font-weight:bold;color:{text_color};">
                                {savings_rate:,.1f} %
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

                # ==========================
                #   PRESUPUESTO VS GASTO
                # ==========================
                st.markdown("---")
                st.subheader("Presupuesto vs gasto real del periodo")

                if df_budgets.empty or float(df_budgets["monthly_amount"].sum()) == 0.0:
                    st.info("No hay presupuestos definidos todavía. Ve a la pestaña 'Presupuestos' para configurarlos.")
                else:
                    monthly_total_budget = float(df_budgets["monthly_amount"].sum())
                    budget_period = monthly_total_budget * n_months_budget

                    if budget_period <= 0:
                        st.info("El presupuesto para el periodo es 0 €. Configura importes mayores en 'Presupuestos'.")
                    else:
                        ratio = total_expenses / budget_period if budget_period > 0 else 0.0

                        colb1, colb2, colb3 = st.columns(3)
                        with colb1:
                            st.metric("Presupuesto del periodo", f"{budget_period:,.2f} €")
                        with colb2:
                            st.metric("Gasto del periodo", f"{total_expenses:,.2f} €")
                        with colb3:
                            st.metric("% presupuesto consumido", f"{ratio * 100:,.1f} %")

                        st.markdown("**Consumo de presupuesto del periodo**")
                        st.progress(max(0.0, min(1.0, ratio)))

                        if ratio <= 1:
                            st.caption(f"Has consumido aproximadamente el {ratio * 100:,.1f} % del presupuesto del periodo.")
                        else:
                            exceso = total_expenses - budget_period
                            st.warning(f"Has superado el presupuesto del periodo en {exceso:,.2f} € ({ratio * 100:,.1f} % del presupuesto).")

                st.markdown("---")

                # ===== SERIE TEMPORAL DE AHORRO =====
                st.subheader("Evolución mensual del ahorro")

                if df_income_year.empty and df_expenses_year.empty:
                    st.info("No hay datos de ingresos ni gastos para el año seleccionado.")
                else:
                    months_year = []
                    if not df_income_year.empty:
                        months_year.extend(df_income_year["date"].dt.month.tolist())
                    if not df_expenses_year.empty:
                        months_year.extend(df_expenses_year["date"].dt.month.tolist())
                    months_year = sorted(set(months_year))

                    data = []
                    for m in months_year:
                        inc_m = df_income_year.loc[df_income_year["date"].dt.month == m, "amount"].sum()
                        exp_m = df_expenses_year.loc[df_expenses_year["date"].dt.month == m, "amount"].sum()
                        data.append(
                            {
                                "month_num": m,
                                "month_label": month_names[m].capitalize(),
                                "income": float(inc_m),
                                "expenses": float(exp_m),
                                "savings": float(inc_m - exp_m),
                            }
                        )

                    if data:
                        df_sav = pd.DataFrame(data)

                        base_sav = alt.Chart(df_sav).encode(
                            x=alt.X("month_label:N", title="Mes",
                                    sort=alt.SortField(field="month_num", order="ascending")),
                            y=alt.Y("savings:Q", title="Ahorro (€)"),
                        )
                        bar_sav = base_sav.mark_bar(color="#3B82F6")  # Barras azules
                        text_sav = base_sav.mark_text(
                            align="center",
                            baseline="middle",
                            color="white",
                            fontWeight="bold"
                        ).encode(text=alt.Text("savings:Q", format=".0f"))

                        zero_line = (
                            alt.Chart(pd.DataFrame({"y": [0]}))
                            .mark_rule(color="red", strokeDash=[4, 4])
                            .encode(y="y:Q")
                        )

                        st.altair_chart(bar_sav + text_sav + zero_line, use_container_width=True)
                    else:
                        st.info("No hay datos mensuales suficientes para mostrar la evolución del ahorro.")

                st.markdown("---")

                # ===== DISTRIBUCIÓN POR CATEGORÍA =====
                st.subheader("Distribución por categoría")

                col_g1, col_g2 = st.columns(2)

                with col_g1:
                    st.markdown("**Gasto por categoría**")
                    if df_expenses_period.empty:
                        st.info("No hay gastos en el periodo seleccionado.")
                    else:
                        df_cat_exp = df_expenses_period.groupby("category", as_index=False)["amount"].sum()
                        base_exp = alt.Chart(df_cat_exp).encode(
                            x=alt.X("amount:Q", title="Gasto (€)"),
                            y=alt.Y("category:N", sort="-x", title="Categoría"),
                        )
                        st.altair_chart(
                            base_exp.mark_bar() + base_exp.mark_text(align="left", baseline="middle", dx=3).encode(
                                text=alt.Text("amount:Q", format=".2f")
                            ),
                            use_container_width=True,
                        )

                with col_g2:
                    st.markdown("**Ingresos por categoría**")
                    if df_income_period.empty:
                        st.info("No hay ingresos en el periodo seleccionado.")
                    else:
                        df_cat_inc = df_income_period.groupby("category", as_index=False)["amount"].sum()
                        base_inc = alt.Chart(df_cat_inc).encode(
                            x=alt.X("amount:Q", title="Ingresos (€)"),
                            y=alt.Y("category:N", sort="-x", title="Categoría"),
                        )
                        st.altair_chart(
                            base_inc.mark_bar() + base_inc.mark_text(align="left", baseline="middle", dx=3).encode(
                                text=alt.Text("amount:Q", format=".2f")
                            ),
                            use_container_width=True,
                        )

                # ===== FIJO vs VARIABLE =====
                st.subheader("Distribución de gasto fijo vs variable")

                if df_expenses_period.empty:
                    st.info("No hay gastos en el periodo seleccionado.")
                else:
                    df_type = df_expenses_period.copy()
                    df_type["expense_type"] = df_type["expense_type"].fillna("No definido")
                    df_type = df_type.groupby("expense_type", as_index=False)["amount"].sum()

                    total_type = df_type["amount"].sum()
                    if total_type <= 0:
                        st.info("No hay importe de gastos en el periodo seleccionado.")
                    else:
                        df_type["percent"] = df_type["amount"] / total_type * 100
                        df_type["percent_label"] = df_type["percent"].map(lambda x: f"{x:.1f}%")

                        # Colores personalizados para mejor distinción
                        color_scale = alt.Scale(
                            domain=["Fijo", "Variable", "No definido"],
                            range=["#3B82F6", "#10B981", "#6B7280"]  # Azul, Verde, Gris
                        )

                        pie = alt.Chart(df_type).mark_arc(innerRadius=0).encode(
                            theta=alt.Theta("amount:Q", stack=True),
                            color=alt.Color(
                                "expense_type:N",
                                title="Tipo de gasto",
                                scale=color_scale,
                                legend=alt.Legend(
                                    orient="bottom",
                                    direction="horizontal",
                                    titleAnchor="middle"
                                )
                            ),
                            tooltip=["expense_type", "amount", "percent_label"],
                        )
                        
                        # Texto con porcentajes dentro del gráfico
                        text_pie = alt.Chart(df_type).mark_text(
                            radius=60,
                            size=14,
                            color="white",
                            fontWeight="bold"
                        ).encode(
                            theta=alt.Theta("amount:Q", stack=True),
                            text=alt.Text("percent_label:N"),
                        )

                        st.altair_chart(pie + text_pie, use_container_width=True)

                st.markdown("---")

                # ===== TABLAS DETALLE =====
                tab1, tab2 = st.tabs(["Gastos del periodo", "Ingresos del periodo"])

                with tab1:
                    if df_expenses_period.empty:
                        st.info("No hay gastos en el periodo seleccionado.")
                    else:
                        df_show_exp = df_expenses_period.copy()
                        df_show_exp["date"] = df_show_exp["date"].dt.date
                        st.dataframe(
                            df_show_exp[["date", "amount", "category", "subcategory", "payment_method", "expense_type", "notes"]],
                            use_container_width=True,
                        )

                with tab2:
                    if df_income_period.empty:
                        st.info("No hay ingresos en el periodo seleccionado.")
                    else:
                        df_show_inc = df_income_period.copy()
                        df_show_inc["date"] = df_show_inc["date"].dt.date
                        st.dataframe(
                            df_show_inc[["date", "amount", "source", "category", "notes"]],
                            use_container_width=True,
                        )



# ========================================================================================================================================================================================
#       VISTA: PRESUPUESTOS
# ========================================================================================================================================================================================

elif menu == "Presupuestos":
    st.subheader("Presupuestos mensuales por categoría")

    st.markdown(
        "Define aquí cuánto quieres gastar **al mes** en cada categoría de gasto. "
        "Luego, en la pestaña de Resumen podremos comparar tu gasto real con estos límites."
    )

    # Leemos categorías de gasto
    df_cats = get_expense_categories(user_id)

    if df_cats.empty:
        st.warning(
            "No hay categorías de gasto definidas. "
            "Ve a la pestaña 'Categorías' para crearlas antes de configurar presupuestos."
        )
    else:
        # Leemos presupuestos actuales (si existen)
        conn = get_connection()
        try:
            df_budgets = pd.read_sql(
                "SELECT category, monthly_amount FROM budgets WHERE user_id = %s",
                conn,
                params=(user_id,),
            )
        except Exception:
            df_budgets = pd.DataFrame(columns=["category", "monthly_amount"])
        finally:
            conn.close()

        # DataFrame base con todas las categorías de gasto existentes
        df_base = df_cats.copy()
        df_base = df_base.rename(columns={"name": "category"})

        if df_budgets.empty:
            df_base["monthly_amount"] = 0.0
        else:
            df_base = df_base.merge(df_budgets, on="category", how="left")
            df_base["monthly_amount"] = df_base["monthly_amount"].fillna(0.0)

        st.markdown("Introduce tu presupuesto mensual objetivo para cada categoría de gasto:")

        with st.form("form_presupuestos"):
            new_values = {}

            for _, row in df_base.iterrows():
                cat = row["category"]
                val = float(row["monthly_amount"])
                new_values[cat] = st.number_input(
                    f"{cat}",
                    min_value=0.0,
                    step=10.0,
                    value=val,
                )

            submitted = st.form_submit_button("Guardar presupuestos")

        if submitted:
            conn = get_connection()
            cursor = conn.cursor()
            try:
                # Para simplificar: borramos lo del usuario y reinsertamos
                cursor.execute("DELETE FROM budgets WHERE user_id = %s", (user_id,))

                now = datetime.now()
                for cat, amount in new_values.items():
                    cursor.execute(
                        """
                        INSERT INTO budgets (user_id, category, monthly_amount, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (int(user_id), cat, float(amount), now, now),
                    )

                conn.commit()
                st.success("Presupuestos guardados correctamente.")
                st.rerun()
            except Exception as e:
                conn.rollback()
                st.error(f"Error guardando presupuestos: {e}")
            finally:
                cursor.close()
                conn.close()

        st.markdown("---")

        # Mostrar tabla resumen de presupuestos actuales
        conn = get_connection()
        df_budgets_view = pd.read_sql(
            "SELECT category, monthly_amount FROM budgets WHERE user_id = %s",
            conn,
            params=(user_id,),
        )
        conn.close()

        if df_budgets_view.empty:
            st.info("Todavía no has definido ningún presupuesto.")
        else:
            # KPI de presupuesto mensual total
            total_budget = float(df_budgets_view["monthly_amount"].sum())
            col_kpi, _ = st.columns([1, 3])
            with col_kpi:
                st.metric("Presupuesto mensual total", f"{total_budget:,.2f} €")

            st.markdown("**Presupuestos actuales por categoría:**")
            st.dataframe(df_budgets_view, use_container_width=True)


# ======================================================================================================================
#       VISTA: CATEGORIAS
# ======================================================================================================================

elif menu == "Categorías":
    st.subheader("Categorías de movimientos")
    st.markdown("Gestiona aquí las categorías que usarás en tus **gastos** y **ingresos**.")

    tab_g, tab_i = st.tabs(["Categorías de gasto", "Categorías de ingreso"])

    # =========================================
    #   TAB GASTOS
    # =========================================
    with tab_g:
        st.markdown("### Categorías de gasto")

        df_exp_cats = get_expense_categories(user_id)

        # Estado edición
        if "edit_exp_cat_old" not in st.session_state:
            st.session_state["edit_exp_cat_old"] = None

        col_a, col_b = st.columns(2)

        with col_a:
            st.markdown("**Listado actual**")
            if df_exp_cats.empty:
                st.info("Todavía no tienes categorías de gasto definidas.")
            else:
                st.dataframe(df_exp_cats, use_container_width=True)

        with col_b:
            # --------- AÑADIR ---------
            st.markdown("**Añadir nueva categoría de gasto**")
            with st.form("form_add_exp_cat"):
                new_exp_cat = st.text_input("Nombre de la categoría de gasto")
                submitted_add_exp = st.form_submit_button("Añadir categoría de gasto")

            if submitted_add_exp:
                name_clean = new_exp_cat.strip()
                if not name_clean:
                    st.error("El nombre de la categoría no puede estar vacío.")
                else:
                    try:
                        add_expense_category(user_id, name_clean)
                        st.success(f"Categoría de gasto '{name_clean}' añadida correctamente.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"No se ha podido añadir la categoría: {e}")

            st.markdown("---")

            # --------- EDITAR / RENOMBRAR ---------
            st.markdown("**Editar (renombrar) categoría de gasto**")
            if df_exp_cats.empty:
                st.info("No hay categorías de gasto para editar.")
            else:
                cat_to_edit = st.selectbox(
                    "Selecciona la categoría a editar",
                    df_exp_cats["name"].tolist(),
                    key="sel_edit_exp_cat",
                )

                if st.button("Cargar para editar", key="btn_load_edit_exp_cat"):
                    st.session_state["edit_exp_cat_old"] = cat_to_edit
                    st.rerun()

                old_name = st.session_state.get("edit_exp_cat_old")
                if old_name:
                    with st.form("form_edit_exp_cat"):
                        new_name = st.text_input("Nuevo nombre", value=old_name, key="txt_new_exp_cat")
                        colx1, colx2 = st.columns(2)
                        with colx1:
                            save = st.form_submit_button("Guardar cambio")
                        with colx2:
                            cancel = st.form_submit_button("Cancelar")

                    if cancel:
                        st.session_state["edit_exp_cat_old"] = None
                        st.rerun()

                    if save:
                        new_name_clean = new_name.strip()
                        if not new_name_clean:
                            st.error("El nuevo nombre no puede estar vacío.")
                        elif new_name_clean == old_name:
                            st.info("No has cambiado el nombre.")
                        else:
                            try:
                                # IMPORTANTE: renombrar en tabla de categorías + actualizar gastos/budgets
                                update_expense_category(user_id, old_name, new_name_clean)
                                st.success(f"Categoría renombrada: '{old_name}' → '{new_name_clean}'.")
                                st.session_state["edit_exp_cat_old"] = None
                                st.rerun()
                            except Exception as e:
                                st.error(f"No se ha podido renombrar la categoría: {e}")

            st.markdown("---")

            # --------- ELIMINAR ---------
            st.markdown("**Eliminar categoría de gasto**")
            if df_exp_cats.empty:
                st.info("No hay categorías de gasto para eliminar.")
            else:
                cat_exp_to_delete = st.selectbox(
                    "Selecciona la categoría a eliminar",
                    df_exp_cats["name"].tolist(),
                    key="delete_exp_cat_select",
                )
                if st.button("Eliminar categoría de gasto seleccionada", key="btn_delete_exp_cat"):
                    try:
                        delete_expense_category(user_id, cat_exp_to_delete)
                        st.success(f"Categoría de gasto '{cat_exp_to_delete}' eliminada correctamente.")
                        st.rerun()
                    except ValueError as ve:
                        st.warning(str(ve))
                    except Exception as e:
                        st.error(f"No se ha podido eliminar la categoría: {e}")

    # =========================================
    #   TAB INGRESOS
    # =========================================
    with tab_i:
        st.markdown("### Categorías de ingreso")

        df_inc_cats = get_income_categories(user_id)

        # Estado edición
        if "edit_inc_cat_old" not in st.session_state:
            st.session_state["edit_inc_cat_old"] = None

        col_c, col_d = st.columns(2)

        with col_c:
            st.markdown("**Listado actual**")
            if df_inc_cats.empty:
                st.info("Todavía no tienes categorías de ingreso definidas.")
            else:
                st.dataframe(df_inc_cats, use_container_width=True)

        with col_d:
            # --------- AÑADIR ---------
            st.markdown("**Añadir nueva categoría de ingreso**")
            with st.form("form_add_inc_cat"):
                new_inc_cat = st.text_input("Nombre de la categoría de ingreso")
                submitted_add_inc = st.form_submit_button("Añadir categoría de ingreso")

            if submitted_add_inc:
                name_clean = new_inc_cat.strip()
                if not name_clean:
                    st.error("El nombre de la categoría no puede estar vacío.")
                else:
                    try:
                        add_income_category(user_id, name_clean)
                        st.success(f"Categoría de ingreso '{name_clean}' añadida correctamente.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"No se ha podido añadir la categoría: {e}")

            st.markdown("---")

            # --------- EDITAR / RENOMBRAR ---------
            st.markdown("**Editar (renombrar) categoría de ingreso**")
            if df_inc_cats.empty:
                st.info("No hay categorías de ingreso para editar.")
            else:
                cat_to_edit = st.selectbox(
                    "Selecciona la categoría a editar",
                    df_inc_cats["name"].tolist(),
                    key="sel_edit_inc_cat",
                )

                if st.button("Cargar para editar", key="btn_load_edit_inc_cat"):
                    st.session_state["edit_inc_cat_old"] = cat_to_edit
                    st.rerun()

                old_name = st.session_state.get("edit_inc_cat_old")
                if old_name:
                    with st.form("form_edit_inc_cat"):
                        new_name = st.text_input("Nuevo nombre", value=old_name, key="txt_new_inc_cat")
                        colx1, colx2 = st.columns(2)
                        with colx1:
                            save = st.form_submit_button("Guardar cambio")
                        with colx2:
                            cancel = st.form_submit_button("Cancelar")

                    if cancel:
                        st.session_state["edit_inc_cat_old"] = None
                        st.rerun()

                    if save:
                        new_name_clean = new_name.strip()
                        if not new_name_clean:
                            st.error("El nuevo nombre no puede estar vacío.")
                        elif new_name_clean == old_name:
                            st.info("No has cambiado el nombre.")
                        else:
                            try:
                                # Renombrar en tabla de categorías + actualizar income
                                update_income_category(user_id, old_name, new_name_clean)
                                st.success(f"Categoría renombrada: '{old_name}' → '{new_name_clean}'.")
                                st.session_state["edit_inc_cat_old"] = None
                                st.rerun()
                            except Exception as e:
                                st.error(f"No se ha podido renombrar la categoría: {e}")

            st.markdown("---")

            # --------- ELIMINAR ---------
            st.markdown("**Eliminar categoría de ingreso**")
            if df_inc_cats.empty:
                st.info("No hay categorías de ingreso para eliminar.")
            else:
                cat_inc_to_delete = st.selectbox(
                    "Selecciona la categoría a eliminar",
                    df_inc_cats["name"].tolist(),
                    key="delete_inc_cat_select",
                )
                if st.button("Eliminar categoría de ingreso seleccionada", key="btn_delete_inc_cat"):
                    try:
                        delete_income_category(user_id, cat_inc_to_delete)
                        st.success(f"Categoría de ingreso '{cat_inc_to_delete}' eliminada correctamente.")
                        st.rerun()
                    except ValueError as ve:
                        st.warning(str(ve))
                    except Exception as e:
                        st.error(f"No se ha podido eliminar la categoría: {e}")


# ========================================================================================================================================================================================
#       VISTA: REGISTRO DE INVERSIONES
# ========================================================================================================================================================================================

elif menu == "Registro inversiones":
    st.subheader("Registro de inversiones")

    st.markdown(
        "Aquí registras tus **movimientos de inversión** por producto:\n"
        "- Marca si es **entrada** (aportación) o **salida** (venta / retirada).\n"
        "- Indica el **importe del movimiento** y el **valor total de la cuenta** tras ese movimiento.\n"
        "Lo normal es actualizarlo, por ejemplo, una vez al mes."
    )

    df_inv = get_investments_df(user_id)
    df_products = get_investment_products(user_id)

    if df_products.empty:
        st.warning(
            "Todavía no has definido ningún producto de inversión.\n\n"
            "Ve a **Configuración → Productos inversión** para crear tus productos "
            "(por ejemplo, 'Indexa 10/10', 'SP500 ETF', 'Renta fija', etc.)."
        )
    else:
        product_names = df_products["name"].tolist()

        # -------- FORMULARIO DE REGISTRO -------- #
        st.markdown("### Nuevo movimiento")

        with st.form("form_inversion"):
            col1, col2 = st.columns(2)
            with col1:
                fecha = st.date_input("Fecha", value=date.today())

                cuenta = st.selectbox(
                    "Producto de inversión",
                    options=product_names,
                    index=0,
                )

                # Obtener tipo de activo asociado al producto seleccionado
                asset_type = df_products.loc[
                    df_products["name"] == cuenta, "asset_type"
                ].iloc[0]

                st.markdown(f"**Tipo de activo:** {asset_type}")

            with col2:
                mov_type = st.radio(
                    "Tipo de movimiento",
                    ["Entrada (aportación)", "Salida (venta/retirada)"],
                    horizontal=True,
                )
                importe_mov = st.number_input(
                    "Importe del movimiento (€)",
                    min_value=0.0,
                    step=50.0,
                    value=0.0,
                )
                valor_actual = st.number_input(
                    "Valor total de la cuenta tras este movimiento (€)",
                    min_value=0.0,
                    step=100.0,
                    value=0.0,
                )

            notas = st.text_area("Notas (opcional)", height=80)

            submitted_inv = st.form_submit_button("Guardar movimiento")

        if submitted_inv:
            if not cuenta.strip():
                st.error("El producto no puede estar vacío.")
            elif importe_mov < 0:
                st.error("El importe del movimiento no puede ser negativo.")
            elif valor_actual < 0:
                st.error("El valor actual no puede ser negativo.")
            else:
                # Entrada = aportación positiva; salida = aportación negativa
                contrib_signed = importe_mov if "Entrada" in mov_type else -importe_mov

                insert_investment(
                    user_id,
                    fecha,
                    cuenta.strip(),
                    asset_type,
                    contrib_signed,
                    valor_actual,
                    notas.strip() or None,
                )
                st.success("Movimiento de inversión guardado correctamente.")
                st.rerun()

    # -------- ÚLTIMOS MOVIMIENTOS -------- #
    st.markdown("### Últimos movimientos registrados")

    df_last_mov = get_all_investments(user_id)
    if df_last_mov.empty:
        st.info("Todavía no hay movimientos registrados.")
    else:
        df_show = df_last_mov.copy()
        df_show["date"] = df_show["date"].dt.date
        st.dataframe(
            df_show[
                [
                    "id",
                    "date",
                    "account",
                    "asset_type",
                    "contribution",
                    "current_value",
                    "notes",
                ]
            ],
            use_container_width=True,
        )

        # Bloque para eliminar movimiento
        with st.expander("Eliminar un movimiento"):
            options_inv = {
                f"{row['id']} - {row['date']} - {row['account']} - {row['current_value']}€":
                int(row["id"])
                for _, row in df_show.iterrows()
            }

            selected_label_inv = st.selectbox(
                "Selecciona el movimiento a eliminar",
                list(options_inv.keys()),
                key="sel_delete_inv",
            )

            if st.button("Eliminar movimiento seleccionado"):
                delete_investment(user_id, options_inv[selected_label_inv])
                st.success("Movimiento eliminado correctamente.")
                st.rerun()

# ======================================================================================================================
#       VISTA: REGISTRO DE INVERSIONES
# ======================================================================================================================

elif menu == "Registro inversiones":
    st.subheader("Registro de inversiones")

    # Estado de edición
    if "edit_inv_id" not in st.session_state:
        st.session_state["edit_inv_id"] = None

    st.markdown(
        "Aquí registras tus **movimientos de inversión** por producto:\n"
        "- Marca si es **entrada** (aportación) o **salida** (venta / retirada).\n"
        "- Indica el **importe del movimiento** y el **valor total de la cuenta** tras ese movimiento.\n"
        "Lo normal es actualizarlo, por ejemplo, una vez al mes."
    )

    df_inv = get_investments_df(user_id)
    df_products = get_investment_products(user_id)

    if df_products.empty:
        st.warning(
            "Todavía no has definido ningún producto de inversión.\n\n"
            "Ve a **Configuración → Productos inversión** para crear tus productos "
            "(por ejemplo, 'Indexa 10/10', 'SP500 ETF', 'Renta fija', etc.)."
        )
    else:
        product_names = df_products["name"].tolist()

        # =========================
        #   NUEVO MOVIMIENTO
        # =========================
        st.markdown("### Nuevo movimiento")

        with st.form("form_inversion_create"):
            col1, col2 = st.columns(2)
            with col1:
                fecha = st.date_input("Fecha", value=date.today(), key="inv_create_date")

                cuenta = st.selectbox(
                    "Producto de inversión",
                    options=product_names,
                    index=0,
                    key="inv_create_account",
                )

                asset_type = df_products.loc[df_products["name"] == cuenta, "asset_type"].iloc[0]
                st.markdown(f"**Tipo de activo:** {asset_type}")

            with col2:
                mov_type = st.radio(
                    "Tipo de movimiento",
                    ["Entrada (aportación)", "Salida (venta/retirada)"],
                    horizontal=True,
                    key="inv_create_movtype",
                )
                importe_mov = st.number_input(
                    "Importe del movimiento (€)",
                    min_value=0.0,
                    step=50.0,
                    value=0.0,
                    key="inv_create_amount",
                )
                valor_actual = st.number_input(
                    "Valor total de la cuenta tras este movimiento (€)",
                    min_value=0.0,
                    step=100.0,
                    value=0.0,
                    key="inv_create_current",
                )

            notas = st.text_area("Notas (opcional)", height=80, key="inv_create_notes")

            submitted_inv = st.form_submit_button("Guardar movimiento")

        if submitted_inv:
            if not cuenta.strip():
                st.error("El producto no puede estar vacío.")
            elif importe_mov < 0:
                st.error("El importe del movimiento debe ser mayor que 0.")
            else:
                contrib_signed = importe_mov if "Entrada" in mov_type else -importe_mov

                insert_investment(
                    user_id,
                    fecha,
                    cuenta.strip(),
                    asset_type,
                    contrib_signed,
                    valor_actual,
                    notas.strip() or None,
                )
                st.success("Movimiento de inversión guardado correctamente.")
                st.rerun()

    # =========================
    #   ÚLTIMOS MOVIMIENTOS
    # =========================
    st.markdown("### Últimos movimientos registrados")

    df_last_mov = get_all_investments(user_id)
    if df_last_mov.empty:
        st.info("Todavía no hay movimientos registrados.")
    else:
        df_show = df_last_mov.copy()
        df_show["date"] = df_show["date"].dt.date

        st.dataframe(
            df_show[["id", "date", "account", "asset_type", "contribution", "current_value", "notes"]],
            use_container_width=True,
        )

        # =========================
        #   EDITAR MOVIMIENTO
        # =========================
        with st.expander("Editar un movimiento"):
            options_edit = {
                f"{row['id']} - {row['date']} - {row['account']} - {row['current_value']}€": int(row["id"])
                for _, row in df_show.iterrows()
            }

            selected_label_edit = st.selectbox(
                "Selecciona el movimiento a editar",
                list(options_edit.keys()),
                key="sel_edit_inv",
            )

            if st.button("Cargar para editar", key="btn_load_edit_inv"):
                st.session_state["edit_inv_id"] = options_edit[selected_label_edit]
                st.rerun()

            edit_id = st.session_state.get("edit_inv_id")

            if edit_id is not None:
                row = df_last_mov.loc[df_last_mov["id"] == edit_id]
                if row.empty:
                    st.warning("No se ha encontrado el movimiento seleccionado. Recarga la página.")
                else:
                    r = row.iloc[0]

                    # Defaults robustos
                    try:
                        fecha_def = pd.to_datetime(r["date"]).date()
                    except Exception:
                        fecha_def = date.today()

                    account_def = str(r["account"]) if pd.notna(r["account"]) else ""
                    asset_def = str(r["asset_type"]) if pd.notna(r["asset_type"]) else ""
                    contrib_def = float(r["contribution"]) if pd.notna(r["contribution"]) else 0.0
                    current_def = float(r["current_value"]) if pd.notna(r["current_value"]) else 0.0
                    notes_def = str(r["notes"]) if pd.notna(r["notes"]) else ""

                    # Movimiento: inferimos Entrada/Salida del signo de contribution
                    mov_def = "Entrada (aportación)" if contrib_def >= 0 else "Salida (venta/retirada)"
                    importe_def = abs(contrib_def)

                    # Para editar: permitir cambiar cuenta, y recalcular asset_type desde productos
                    df_products = get_investment_products(user_id)
                    product_names = df_products["name"].tolist() if not df_products.empty else []

                    acc_index = product_names.index(account_def) if account_def in product_names else 0
                    mov_options = ["Entrada (aportación)", "Salida (venta/retirada)"]
                    mov_index = mov_options.index(mov_def) if mov_def in mov_options else 0

                    with st.form("form_inv_edit"):
                        col1, col2 = st.columns(2)

                        with col1:
                            fecha_edit = st.date_input("Fecha", value=fecha_def, key="inv_edit_date")
                            cuenta_edit = st.selectbox(
                                "Producto de inversión",
                                options=product_names,
                                index=acc_index if product_names else 0,
                                key="inv_edit_account",
                            )

                            # asset_type ligado al producto seleccionado
                            asset_type_edit = df_products.loc[df_products["name"] == cuenta_edit, "asset_type"].iloc[0]
                            st.markdown(f"**Tipo de activo:** {asset_type_edit}")

                        with col2:
                            mov_type_edit = st.radio(
                                "Tipo de movimiento",
                                mov_options,
                                index=mov_index,
                                horizontal=True,
                                key="inv_edit_movtype",
                            )
                            importe_mov_edit = st.number_input(
                                "Importe del movimiento (€)",
                                min_value=0.0,
                                step=50.0,
                                value=float(importe_def),
                                key="inv_edit_amount",
                            )
                            valor_actual_edit = st.number_input(
                                "Valor total de la cuenta tras este movimiento (€)",
                                min_value=0.0,
                                step=100.0,
                                value=float(current_def),
                                key="inv_edit_current",
                            )

                        notas_edit = st.text_area("Notas (opcional)", height=80, value=notes_def, key="inv_edit_notes")

                        colb1, colb2 = st.columns(2)
                        with colb1:
                            save_edit = st.form_submit_button("Guardar cambios")
                        with colb2:
                            cancel_edit = st.form_submit_button("Cancelar edición")

                    if cancel_edit:
                        st.session_state["edit_inv_id"] = None
                        st.rerun()

                    if save_edit:
                        if not cuenta_edit.strip():
                            st.error("El producto no puede estar vacío.")
                        elif importe_mov_edit < 0:
                            st.error("El importe del movimiento no puede ser negativo.")
                        elif valor_actual_edit < 0:
                            st.error("El valor actual no puede ser negativo.")
                        else:
                            contrib_signed = (
                                float(importe_mov_edit)
                                if "Entrada" in mov_type_edit
                                else -float(importe_mov_edit)
                            )

                            update_investment(
                                user_id=user_id,
                                inv_id=int(edit_id),
                                date_val=fecha_edit,
                                account=cuenta_edit.strip(),
                                asset_type=asset_type_edit,
                                contribution=contrib_signed,
                                current_value=float(valor_actual_edit),
                                notes=notas_edit.strip() or None,
                            )

                            st.success("Movimiento actualizado correctamente.")
                            st.session_state["edit_inv_id"] = None
                            st.rerun()

        # =========================
        #   ELIMINAR MOVIMIENTO
        # =========================
        with st.expander("Eliminar un movimiento"):
            options_inv = {
                f"{row['id']} - {row['date']} - {row['account']} - {row['current_value']}€": int(row["id"])
                for _, row in df_show.iterrows()
            }

            selected_label_inv = st.selectbox(
                "Selecciona el movimiento a eliminar",
                list(options_inv.keys()),
                key="sel_delete_inv",
            )

            if st.button("Eliminar movimiento seleccionado", key="btn_delete_inv"):
                delete_investment(user_id, options_inv[selected_label_inv])
                st.success("Movimiento eliminado correctamente.")
                st.rerun()


# ======================================================================================================================
#       VISTA: PRODUCTOS DE INVERSION
# ======================================================================================================================

elif menu == "Productos de inversión":
    st.subheader("Productos de inversión")

    # Estado de edición
    if "edit_product_id" not in st.session_state:
        st.session_state["edit_product_id"] = None

    st.markdown(
        "Aquí defines los **productos de inversión** que podrás usar en el registro de inversiones.\n\n"
        "Por ejemplo:\n"
        "- Producto: `Indexa 10/10` → Tipo: `Fondo indexado`\n"
        "- Producto: `VWCE` → Tipo: `ETF`\n"
        "- Producto: `Cuenta remunerada X` → Tipo: `Cash`"
    )

    df_products = get_investment_products(user_id)

    # =========================
    #   NUEVO PRODUCTO
    # =========================
    st.markdown("### Añadir nuevo producto")

    with st.form("form_new_product"):
        col1, col2 = st.columns(2)
        with col1:
            new_name = st.text_input("Nombre del producto (obligatorio)", value="", key="prod_create_name")
        with col2:
            # Texto libre en vez de selectbox
            new_asset_type = st.text_input("Tipo de activo (texto libre)", value="", key="prod_create_asset")

        submitted_new_prod = st.form_submit_button("Guardar producto")

    if submitted_new_prod:
        name_clean = new_name.strip()
        asset_clean = new_asset_type.strip()

        if not name_clean:
            st.error("El nombre del producto no puede estar vacío.")
        elif not asset_clean:
            st.error("El tipo de activo no puede estar vacío.")
        else:
            try:
                insert_investment_product(user_id, name_clean, asset_clean)
                st.success("Producto de inversión creado correctamente.")
                st.rerun()
            except Exception as e:
                st.error(f"Error al crear el producto: {e}")

    st.markdown("---")

    # =========================
    #   LISTADO DE PRODUCTOS
    # =========================
    st.markdown("### Productos actuales")

    if df_products.empty:
        st.info("Todavía no has definido productos de inversión.")
    else:
        st.dataframe(
            df_products[["id", "name", "asset_type"]],
            use_container_width=True,
        )

        # =========================
        #   EDITAR PRODUCTO (UPDATE)
        # =========================
        with st.expander("Editar un producto"):
            options_edit = {
                f"{row['id']} - {row['name']} ({row['asset_type']})": int(row["id"])
                for _, row in df_products.iterrows()
            }

            selected_label_edit = st.selectbox(
                "Selecciona el producto a editar",
                list(options_edit.keys()),
                key="sel_edit_prod",
            )

            if st.button("Cargar para editar", key="btn_load_edit_prod"):
                st.session_state["edit_product_id"] = options_edit[selected_label_edit]
                st.rerun()

            edit_id = st.session_state.get("edit_product_id")

            if edit_id is not None:
                row = df_products.loc[df_products["id"] == edit_id]
                if row.empty:
                    st.warning("No se ha encontrado el producto seleccionado. Recarga la página.")
                else:
                    r = row.iloc[0]
                    name_def = str(r["name"]) if pd.notna(r["name"]) else ""
                    asset_def = str(r["asset_type"]) if pd.notna(r["asset_type"]) else ""

                    with st.form("form_edit_product"):
                        col1, col2 = st.columns(2)
                        with col1:
                            name_edit = st.text_input("Nombre del producto", value=name_def, key="prod_edit_name")
                        with col2:
                            asset_edit = st.text_input("Tipo de activo (texto libre)", value=asset_def, key="prod_edit_asset")

                        colb1, colb2 = st.columns(2)
                        with colb1:
                            save_edit = st.form_submit_button("Guardar cambios")
                        with colb2:
                            cancel_edit = st.form_submit_button("Cancelar edición")

                    if cancel_edit:
                        st.session_state["edit_product_id"] = None
                        st.rerun()

                    if save_edit:
                        name_clean = name_edit.strip()
                        asset_clean = asset_edit.strip()

                        if not name_clean:
                            st.error("El nombre del producto no puede estar vacío.")
                        elif not asset_clean:
                            st.error("El tipo de activo no puede estar vacío.")
                        else:
                            try:
                                update_investment_product(
                                    user_id=user_id,
                                    prod_id=int(edit_id),
                                    name=name_clean,
                                    asset_type=asset_clean,
                                )
                                st.success("Producto actualizado correctamente.")
                                st.session_state["edit_product_id"] = None
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error al actualizar el producto: {e}")

        # =========================
        #   ELIMINAR PRODUCTO
        # =========================
        with st.expander("Eliminar un producto"):
            options_prod = {
                f"{row['id']} - {row['name']} ({row['asset_type']})": int(row["id"])
                for _, row in df_products.iterrows()
            }

            selected_label_prod = st.selectbox(
                "Selecciona el producto a eliminar",
                list(options_prod.keys()),
                key="sel_delete_prod",
            )

            st.warning(
                "Nota: eliminar un producto NO elimina los movimientos históricos "
                "registrados en la tabla de inversiones; simplemente deja de aparecer "
                "como opción en el registro."
            )

            if st.button("Eliminar producto seleccionado", key="btn_delete_prod"):
                delete_investment_product(user_id, options_prod[selected_label_prod])
                st.success("Producto eliminado correctamente.")
                st.rerun()

# ==============================================================================================================================
#       VISTA: RESUMEN INVERSIONES
# ==============================================================================================================================

elif menu == "Resumen inversiones":
    st.subheader("Resumen de mis inversiones")

    df_inv = get_investments_df(user_id)

    if df_inv.empty:
        st.info("Todavía no has registrado inversiones.")
    else:
        # Asegurar fecha y ordenar correctamente
        df_sorted = df_inv.sort_values(["account", "date", "id"])
        df_sorted["date"] = pd.to_datetime(df_sorted["date"])

        # Aportación acumulada por cuenta
        df_sorted["aport_acum"] = df_sorted.groupby("account")["contribution"].cumsum()

        # Último estado por cuenta (foto actual)
        df_last = df_sorted.groupby("account", as_index=False).tail(1)

        df_last["gain"] = df_last["current_value"] - df_last["aport_acum"]
        df_last["return_pct"] = df_last.apply(
            lambda row: (row["gain"] / row["aport_acum"] * 100.0)
            if row["aport_acum"] != 0
            else 0.0,
            axis=1,
        )

        total_aportado = float(df_last["aport_acum"].sum())
        total_valor = float(df_last["current_value"].sum())
        total_plusvalia = total_valor - total_aportado
        total_rentab = (
            total_plusvalia / total_aportado * 100.0
            if total_aportado > 0
            else 0.0
        )

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Capital aportado", f"{total_aportado:,.2f} €")
        with col2:
            st.metric("Valor actual", f"{total_valor:,.2f} €")
        with col3:
            st.metric("Plusvalía", f"{total_plusvalia:,.2f} €")
        with col4:
            st.metric("Rentabilidad total", f"{total_rentab:,.2f} %")

        st.markdown("---")

        # ===== Distribución de la cartera por cuenta =====
        st.markdown("### Distribución de la cartera por cuenta")

        df_dist = df_last[["account", "current_value"]].copy()
        df_dist = df_dist.sort_values("current_value", ascending=False)

        chart_dist = (
            alt.Chart(df_dist)
            .mark_bar()
            .encode(
                x=alt.X("current_value:Q", title="Valor actual (€)"),
                y=alt.Y("account:N", sort="-x", title="Cuenta"),
                tooltip=["account", "current_value"],
            )
        )

        text_dist = (
            alt.Chart(df_dist)
            .mark_text(align="left", baseline="middle", dx=3)
            .encode(
                x="current_value:Q",
                y=alt.Y("account:N", sort="-x"),
                text=alt.Text("current_value:Q", format=".0f"),
            )
        )

        st.altair_chart(chart_dist + text_dist, use_container_width=True)

        st.markdown("### Evolución del valor total de la cartera")

        # Valor total por fecha (sumando todas las cuentas)
        df_total = df_sorted.groupby("date", as_index=False)["current_value"].sum()
        df_total = df_total.sort_values("date")

        chart_total = (
            alt.Chart(df_total)
            .mark_line(point=True)
            .encode(
                x=alt.X("date:T", title="Fecha"),
                y=alt.Y("current_value:Q", title="Valor total cartera (€)"),
                tooltip=["date", "current_value"],
            )
        )

        text_total = (
            alt.Chart(df_total)
            .mark_text(dy=-10)
            .encode(
                x="date:T",
                y="current_value:Q",
                text=alt.Text("current_value:Q", format=".0f"),
            )
        )

        st.altair_chart(chart_total + text_total, use_container_width=True)

        st.markdown("### Ganancia mensual (mercado)")

        # Agregamos por fecha (suponemos fotos periódicas)
        df_total = df_total.sort_values("date")
        df_total["prev_value"] = df_total["current_value"].shift(1)

        # aportaciones por fecha (sumando todas las cuentas)
        df_contrib = (
            df_sorted.groupby("date", as_index=False)["contribution"].sum()
        )
        df_month = df_total.merge(df_contrib, on="date", how="left")
        df_month["contribution"] = df_month["contribution"].fillna(0.0)

        # ganancia del periodo ≈ valor_actual - valor_anterior - aportación_periodo
        df_month["gain_month"] = (
            df_month["current_value"]
            - df_month["prev_value"]
            - df_month["contribution"]
        )

        # Para el primer registro, donde no hay prev_value, lo ponemos a 0
        df_month.loc[df_month["prev_value"].isna(), "gain_month"] = 0.0

        # Formato mes-año
        df_month["month_label"] = df_month["date"].dt.to_period("M").astype(str)

        chart_gain = (
            alt.Chart(df_month)
            .mark_bar()
            .encode(
                x=alt.X("month_label:N", title="Mes"),
                y=alt.Y("gain_month:Q", title="Ganancia de mercado (€)"),
                tooltip=["month_label", "gain_month"],
            )
        )

        zero_line = (
            alt.Chart(pd.DataFrame({"y": [0]}))
            .mark_rule(strokeDash=[4, 4], color="red")
            .encode(y="y:Q")
        )

        text_gain = (
            alt.Chart(df_month)
            .mark_text(dy=-10)
            .encode(
                x="month_label:N",
                y="gain_month:Q",
                text=alt.Text("gain_month:Q", format=".0f"),
            )
        )

        st.altair_chart(chart_gain + zero_line + text_gain, use_container_width=True)

        st.markdown("### Detalle por cuenta (estado actual)")

        df_view = df_last[
            [
                "account",
                "asset_type",
                "aport_acum",
                "current_value",
                "gain",
                "return_pct",
            ]
        ].copy()
        df_view = df_view.rename(
            columns={
                "account": "Cuenta",
                "asset_type": "Tipo activo",
                "aport_acum": "Aportado",
                "current_value": "Valor actual",
                "gain": "Plusvalía",
                "return_pct": "Rentabilidad (%)",
            }
        )
        st.dataframe(df_view, use_container_width=True)



# ==============================================================================================================================
#       VISTA: IMPORTAR DATOS (CSV)
# ==============================================================================================================================

elif menu == "Importar datos":
    st.subheader("Importar datos desde CSV")

    st.markdown(
        "Aquí puedes importar tus **gastos** e **ingresos** históricos desde archivos CSV.\n\n"
        "Te recomiendo exportar desde tu Excel dos archivos separados:\n"
        "- Uno para **gastos**\n"
        "- Otro para **ingresos**\n\n"
        "Formato esperado:\n"
        "- Gastos: `date`, `amount`, `category` (opcional: `subcategory`, `payment_method`, `expense_type`, `notes`)\n"
        "- Ingresos: `date`, `amount`, `category`, `source` (opcional: `notes`)\n\n"
        "Consejos:\n"
        "- Usa separador `;` al guardar el CSV.\n"
        "- Si usas coma como decimal (`102,5`), la app lo normaliza automáticamente."
    )

    tab_g, tab_i = st.tabs(["Importar gastos", "Importar ingresos"])

    # --------------------------------------------
    #   TAB: IMPORTAR GASTOS
    # --------------------------------------------
    with tab_g:
        st.markdown("### Importar gastos")

        file_gastos = st.file_uploader(
            "Sube un CSV con tus gastos",
            type=["csv"],
            key="file_gastos",
        )

        if file_gastos is not None:
            try:
                df_g = pd.read_csv(
                    file_gastos,
                    sep=";",
                    encoding="utf-8",
                )

                df_g.columns = [c.strip() for c in df_g.columns]

                if "category" in df_g.columns:
                    df_g["category"] = (
                        df_g["category"].astype(str).str.strip().str.capitalize()
                    )

                st.markdown("Vista previa del archivo:")
                st.dataframe(df_g.head(), use_container_width=True)

                required_cols = {"date", "amount", "category"}
                missing = required_cols - set(df_g.columns)
                if missing:
                    st.error(
                        f"Faltan columnas obligatorias en el CSV de gastos: {', '.join(missing)}.\n\n"
                        "Asegúrate de que el archivo tenga al menos: `date`, `amount`, `category`."
                    )
                else:
                    optional_cols = ["subcategory", "payment_method", "expense_type", "notes"]
                    for col in optional_cols:
                        if col not in df_g.columns:
                            df_g[col] = None

                    df_g["amount"] = (
                        df_g["amount"]
                        .astype(str)
                        .str.replace("€", "", regex=False)
                        .str.replace(",", ".", regex=False)
                        .str.strip()
                    )

                    df_g["date"] = pd.to_datetime(
                        df_g["date"],
                        dayfirst=True,
                        errors="coerce",
                    ).dt.date

                    before = len(df_g)
                    df_g = df_g.dropna(subset=["date"])
                    after = len(df_g)
                    if before != after:
                        st.warning(f"Se han descartado {before - after} filas por tener fechas no válidas.")

                    df_g["amount"] = pd.to_numeric(df_g["amount"], errors="coerce")
                    before2 = len(df_g)
                    df_g = df_g.dropna(subset=["amount"])
                    after2 = len(df_g)
                    if before2 != after2:
                        st.warning(f"Se han descartado {before2 - after2} filas por tener importes no válidos.")

                    if not df_g.empty:
                        st.markdown(f"Se van a importar **{len(df_g)}** gastos si confirmas.")

                        if st.button("Importar gastos", key="btn_import_gastos"):
                            conn = get_connection()
                            cursor = conn.cursor()
                            try:
                                # Categorías existentes (DEL USUARIO)
                                existing_cats = set(get_expense_categories(user_id)["name"].tolist())

                                # Categorías nuevas en el CSV
                                new_cats = set(df_g["category"].dropna().unique()) - existing_cats
                                for cat in new_cats:
                                    if isinstance(cat, str) and cat.strip():
                                        add_expense_category(user_id, cat.strip())

                                # Insertar gastos (CON user_id)
                                query = """
                                    INSERT INTO expenses
                                    (user_id, date, amount, category, subcategory, payment_method, expense_type, notes, created_at)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """
                                now = datetime.now()
                                rows = []
                                for _, r in df_g.iterrows():
                                    rows.append(
                                        (
                                            int(user_id),
                                            r["date"],
                                            float(r["amount"]),
                                            r["category"],
                                            (r["subcategory"] if pd.notna(r["subcategory"]) else None),
                                            (r["payment_method"] if pd.notna(r["payment_method"]) else None),
                                            (r["expense_type"] if pd.notna(r["expense_type"]) else None),
                                            (r["notes"] if pd.notna(r["notes"]) else None),
                                            now,
                                        )
                                    )

                                cursor.executemany(query, rows)
                                conn.commit()
                                st.success(f"Gastos importados correctamente: {len(rows)} registros.")
                                st.rerun()
                            except Exception as e:
                                conn.rollback()
                                st.error(f"Error al importar gastos: {e}")
                            finally:
                                cursor.close()
                                conn.close()
            except Exception as e:
                st.error(f"No se ha podido leer el CSV de gastos: {e}")

    # --------------------------------------------
    #   TAB: IMPORTAR INGRESOS
    # --------------------------------------------
    with tab_i:
        st.markdown("### Importar ingresos")

        file_ingresos = st.file_uploader(
            "Sube un CSV con tus ingresos",
            type=["csv"],
            key="file_ingresos",
        )

        if file_ingresos is not None:
            try:
                df_i = pd.read_csv(
                    file_ingresos,
                    sep=";",
                    encoding="utf-8",
                )

                df_i.columns = [c.strip() for c in df_i.columns]

                if "category" in df_i.columns:
                    df_i["category"] = (
                        df_i["category"].astype(str).str.strip().str.capitalize()
                    )

                st.markdown("Vista previa del archivo:")
                st.dataframe(df_i.head(), use_container_width=True)

                required_cols_i = {"date", "amount", "category", "source"}
                missing_i = required_cols_i - set(df_i.columns)
                if missing_i:
                    st.error(
                        f"Faltan columnas obligatorias en el CSV de ingresos: {', '.join(missing_i)}.\n\n"
                        "Asegúrate de que el archivo tenga al menos: `date`, `amount`, `category`, `source`."
                    )
                else:
                    if "notes" not in df_i.columns:
                        df_i["notes"] = None

                    df_i["amount"] = (
                        df_i["amount"]
                        .astype(str)
                        .str.replace("€", "", regex=False)
                        .str.replace(",", ".", regex=False)
                        .str.strip()
                    )

                    df_i["date"] = pd.to_datetime(
                        df_i["date"],
                        dayfirst=True,
                        errors="coerce",
                    ).dt.date

                    before = len(df_i)
                    df_i = df_i.dropna(subset=["date"])
                    after = len(df_i)
                    if before != after:
                        st.warning(f"Se han descartado {before - after} filas por tener fechas no válidas.")

                    df_i["amount"] = pd.to_numeric(df_i["amount"], errors="coerce")
                    before2 = len(df_i)
                    df_i = df_i.dropna(subset=["amount"])
                    after2 = len(df_i)
                    if before2 != after2:
                        st.warning(f"Se han descartado {before2 - after2} filas por tener importes no válidos.")

                    if not df_i.empty:
                        st.markdown(f"Se van a importar **{len(df_i)}** ingresos si confirmas.")

                        if st.button("Importar ingresos", key="btn_import_ingresos"):
                            conn = get_connection()
                            cursor = conn.cursor()
                            try:
                                existing_inc_cats = set(get_income_categories(user_id)["name"].tolist())
                                new_inc_cats = set(df_i["category"].dropna().unique()) - existing_inc_cats
                                for cat in new_inc_cats:
                                    if isinstance(cat, str) and cat.strip():
                                        add_income_category(user_id, cat.strip())

                                query_i = """
                                    INSERT INTO income
                                    (user_id, date, amount, source, category, notes, created_at)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                                """
                                now = datetime.now()
                                rows_i = []
                                for _, r in df_i.iterrows():
                                    rows_i.append(
                                        (
                                            int(user_id),
                                            r["date"],
                                            float(r["amount"]),
                                            r["source"],
                                            r["category"],
                                            (r["notes"] if pd.notna(r["notes"]) else None),
                                            now,
                                        )
                                    )

                                cursor.executemany(query_i, rows_i)
                                conn.commit()
                                st.success(f"Ingresos importados correctamente: {len(rows_i)} registros.")
                                st.rerun()
                            except Exception as e:
                                conn.rollback()
                                st.error(f"Error al importar ingresos: {e}")
                            finally:
                                cursor.close()
                                conn.close()
            except Exception as e:
                st.error(f"No se ha podido leer el CSV de ingresos: {e}")


# ========================================================================================================================================================================================
#       VISTA: REGISTRO DE EFECTIVO
# ========================================================================================================================================================================================

elif menu == "Registro efectivo":
    st.subheader("Registro de efectivo (cash)")

    st.markdown(
        "Aquí registras el **saldo de tus cuentas** (Caixa, Trade Republic, Revolut, etc.).\n\n"
        "Recomendación: crea un **snapshot** al mes (por ejemplo el día 01)."
    )

    # Estado de edición
    if "edit_cash_id" not in st.session_state:
        st.session_state["edit_cash_id"] = None

    df_cash = get_cash_df(user_id)
    existing_accounts = sorted(df_cash["account"].unique().tolist()) if not df_cash.empty else []

    # =========================
    #   CREAR SNAPSHOT
    # =========================
    st.markdown("### Nuevo snapshot de efectivo")
    with st.form("form_cash_create"):
        col1, col2 = st.columns(2)
        with col1:
            fecha = st.date_input("Fecha", value=date.today(), key="create_cash_date")
            account = st.text_input(
                "Cuenta (ej. 'Caixa', 'Trade Republic Cash')",
                value=existing_accounts[0] if existing_accounts else "",
                key="create_cash_account",
            )
        with col2:
            current_value = st.number_input(
                "Saldo (€)",
                min_value=0.0,
                step=50.0,
                value=0.0,
                key="create_cash_value",
            )

        notes = st.text_area("Notas (opcional)", height=80, key="create_cash_notes")
        submitted = st.form_submit_button("Guardar snapshot")

    if submitted:
        if not account.strip():
            st.error("La cuenta no puede estar vacía.")
        else:
            try:
                insert_cash_snapshot(
                    user_id,
                    fecha,
                    account.strip(),
                    current_value,
                    notes.strip() or None
                )
                st.success("Snapshot guardado.")
                st.rerun()
            except Exception as e:
                st.error(f"Error guardando snapshot: {e}")

    # =========================
    #   LISTADO + EDITAR / BORRAR
    # =========================
    st.markdown("### Últimos snapshots")
    df_last = get_all_cash(user_id)

    if df_last.empty:
        st.info("Todavía no hay snapshots de efectivo.")
    else:
        df_show = df_last.copy()
        df_show["date"] = df_show["date"].dt.date

        st.dataframe(
            df_show[["id", "date", "account", "current_value", "notes"]],
            use_container_width=True,
        )

        # ---------------------------
        #   EDITAR SNAPSHOT (UPDATE)
        # ---------------------------
        with st.expander("Editar un snapshot"):
            options_edit = {
                f"{row['id']} - {row['date']} - {row['account']} - {row['current_value']}€": int(row["id"])
                for _, row in df_show.iterrows()
            }

            selected_label_edit = st.selectbox(
                "Selecciona el snapshot a editar",
                list(options_edit.keys()),
                key="sel_edit_cash",
            )

            if st.button("Cargar para editar", key="btn_load_edit_cash"):
                st.session_state["edit_cash_id"] = options_edit[selected_label_edit]
                st.rerun()

            edit_id = st.session_state.get("edit_cash_id")

            if edit_id is not None:
                row = df_last.loc[df_last["id"] == edit_id]
                if row.empty:
                    st.warning("No se ha encontrado el snapshot seleccionado. Recarga la página.")
                else:
                    r = row.iloc[0]

                    try:
                        fecha_default = pd.to_datetime(r["date"]).date()
                    except Exception:
                        fecha_default = date.today()

                    account_default = str(r["account"]) if pd.notna(r["account"]) else ""
                    value_default = float(r["current_value"]) if pd.notna(r["current_value"]) else 0.0
                    notes_default = str(r["notes"]) if pd.notna(r["notes"]) else ""

                    with st.form("form_cash_edit"):
                        col1, col2 = st.columns(2)
                        with col1:
                            fecha_edit = st.date_input("Fecha", value=fecha_default, key="edit_cash_date")
                            account_edit = st.text_input("Cuenta", value=account_default, key="edit_cash_account")
                        with col2:
                            value_edit = st.number_input(
                                "Saldo (€)",
                                min_value=0.0,
                                step=50.0,
                                value=value_default,
                                key="edit_cash_value",
                            )

                        notes_edit = st.text_area("Notas (opcional)", height=80, value=notes_default, key="edit_cash_notes")

                        colb1, colb2 = st.columns(2)
                        with colb1:
                            save_edit = st.form_submit_button("Guardar cambios")
                        with colb2:
                            cancel_edit = st.form_submit_button("Cancelar edición")

                    if cancel_edit:
                        st.session_state["edit_cash_id"] = None
                        st.rerun()

                    if save_edit:
                        if not account_edit.strip():
                            st.error("La cuenta no puede estar vacía.")
                        else:
                            update_cash_snapshot(
                                user_id,
                                int(edit_id),
                                fecha_edit,
                                account_edit.strip(),
                                float(value_edit),
                                notes_edit.strip() or None,
                            )
                            st.success("Snapshot actualizado correctamente.")
                            st.session_state["edit_cash_id"] = None
                            st.rerun()

        # ---------------------------
        #   ELIMINAR SNAPSHOT
        # ---------------------------
        with st.expander("Eliminar un snapshot"):
            options = {
                f"{row['id']} - {row['date']} - {row['account']} - {row['current_value']}€": int(row["id"])
                for _, row in df_show.iterrows()
            }
            sel = st.selectbox(
                "Selecciona el snapshot a eliminar",
                list(options.keys()),
                key="sel_delete_cash",
            )
            if st.button("Eliminar snapshot seleccionado", key="btn_delete_cash"):
                delete_cash_snapshot(user_id, options[sel])
                st.success("Snapshot eliminado.")
                st.rerun()

# ========================================================================================================================================================================================
#       VISTA: PATRIMONIO
# ========================================================================================================================================================================================

elif menu == "Patrimonio":
    st.subheader("Patrimonio total")

    df_inv = get_investments_df(user_id)
    df_cash = get_cash_df(user_id)

    if df_inv.empty and df_cash.empty:
        st.info("Aún no hay datos de inversiones ni de efectivo.")
    else:
        years = set()
        if not df_inv.empty:
            years.update(df_inv["date"].dt.year.unique().tolist())
        if not df_cash.empty:
            years.update(df_cash["date"].dt.year.unique().tolist())

        years = sorted(list(years), reverse=True)
        selected_year = st.selectbox("Año", years, index=0)

        df_inv_y = df_inv[df_inv["date"].dt.year == selected_year] if not df_inv.empty else df_inv
        df_cash_y = df_cash[df_cash["date"].dt.year == selected_year] if not df_cash.empty else df_cash

        inv_last = pd.DataFrame()
        if not df_inv_y.empty:
            inv_last = (
                df_inv_y.sort_values(["account", "date", "id"])
                .groupby("account", as_index=False)
                .tail(1)
            )

        cash_last = pd.DataFrame()
        if not df_cash_y.empty:
            cash_last = (
                df_cash_y.sort_values(["account", "date", "id"])
                .groupby("account", as_index=False)
                .tail(1)
            )

        total_inv = float(inv_last["current_value"].sum()) if not inv_last.empty else 0.0
        total_cash = float(cash_last["current_value"].sum()) if not cash_last.empty else 0.0
        total_wealth = total_inv + total_cash

        month_names = {
            1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
            5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
            9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre",
        }

        def monthly_last_sum_investments(df):
            if df.empty:
                return pd.DataFrame(columns=["month", "inv_value"])
            df = df.copy()
            df["month"] = df["date"].dt.month
            last_per_month_acc = (
                df.sort_values(["account", "date", "id"])
                .groupby(["month", "account"], as_index=False)
                .tail(1)
            )
            out = last_per_month_acc.groupby("month", as_index=False)["current_value"].sum()
            out = out.rename(columns={"current_value": "inv_value"})
            return out

        def monthly_last_sum_cash(df):
            if df.empty:
                return pd.DataFrame(columns=["month", "cash_value"])
            df = df.copy()
            df["month"] = df["date"].dt.month
            last_per_month_acc = (
                df.sort_values(["account", "date", "id"])
                .groupby(["month", "account"], as_index=False)
                .tail(1)
            )
            out = last_per_month_acc.groupby("month", as_index=False)["current_value"].sum()
            out = out.rename(columns={"current_value": "cash_value"})
            return out

        df_m_inv = monthly_last_sum_investments(df_inv_y)
        df_m_cash = monthly_last_sum_cash(df_cash_y)

        df_month = pd.merge(df_m_inv, df_m_cash, on="month", how="outer").fillna(0.0)
        df_month["wealth"] = df_month["inv_value"] + df_month["cash_value"]
        df_month = df_month.sort_values("month")
        df_month["month_label"] = df_month["month"].map(lambda m: month_names.get(int(m), str(m)))

        ytd_pct = 0.0
        if not df_month.empty and len(df_month) >= 2:
            start = float(df_month.iloc[0]["wealth"])
            end = float(df_month.iloc[-1]["wealth"])
            if start > 0:
                ytd_pct = (end - start) / start * 100.0

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Patrimonio actual", f"{total_wealth:,.2f} €")
        with col2:
            st.metric("Efectivo", f"{total_cash:,.2f} €")
        with col3:
            st.metric("Inversiones", f"{total_inv:,.2f} €")

        bg_color = "#d4edda" if ytd_pct >= 0 else "#f8d7da"
        text_color = "#155724" if ytd_pct >= 0 else "#721c24"
        st.markdown(
            f"""
            <div style="margin-top:0.5rem; padding:0.75rem; border-radius:0.5rem; background-color:{bg_color}; text-align:center;">
                <div style="font-size:0.9rem;">Crecimiento YTD (aprox.)</div>
                <div style="font-size:1.3rem; font-weight:bold; color:{text_color};">
                    {ytd_pct:,.2f} %
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown("---")

        st.subheader("Evolución mensual del patrimonio")

        if df_month.empty:
            st.info("No hay suficientes snapshots en el año para dibujar la evolución.")
        else:
            base = alt.Chart(df_month).encode(
                x=alt.X("month_label:N", title="Mes"),
                y=alt.Y("wealth:Q", title="Patrimonio (€)"),
            )
            bar = base.mark_bar(color="#3B82F6")  # Barras azules
            text = base.mark_text(
                align="center",
                baseline="middle",
                color="white",
                fontWeight="bold"
            ).encode(text=alt.Text("wealth:Q", format=".0f"))
            st.altair_chart(bar + text, use_container_width=True)

        st.markdown("---")

        st.subheader("Distribución actual (último snapshot del año seleccionado)")

        df_dist = pd.DataFrame(
            [
                {"Tipo": "Efectivo", "Valor": total_cash},
                {"Tipo": "Inversiones", "Valor": total_inv},
            ]
        )

        base2 = alt.Chart(df_dist).encode(
            x=alt.X("Valor:Q", title="€"),
            y=alt.Y("Tipo:N", sort="-x", title=""),
        )
        bars = base2.mark_bar()
        txt = base2.mark_text(align="left", baseline="middle", dx=3).encode(
            text=alt.Text("Valor:Q", format=".0f")
        )
        st.altair_chart(bars + txt, use_container_width=True)

        st.markdown("### Detalle por cuenta (foto actual)")

        col_a, col_b = st.columns(2)

        with col_a:
            st.markdown("**Efectivo por cuenta**")
            if cash_last.empty:
                st.info("No hay efectivo registrado en el año.")
            else:
                df_cash_view = cash_last[["account", "current_value"]].copy()
                df_cash_view = df_cash_view.sort_values("current_value", ascending=False)
                st.dataframe(df_cash_view, use_container_width=True)

        with col_b:
            st.markdown("**Inversiones por producto**")
            if inv_last.empty:
                st.info("No hay inversiones registradas en el año.")
            else:
                df_inv_view = inv_last[["account", "asset_type", "current_value"]].copy()
                df_inv_view = df_inv_view.sort_values("current_value", ascending=False)
                st.dataframe(df_inv_view, use_container_width=True)