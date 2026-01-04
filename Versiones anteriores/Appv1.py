import streamlit as st
import pandas as pd
from datetime import date, datetime

from backend.db_connection import get_connection

# --------- FUNCIONES BBDD --------- #

def insert_income(date_val, amount, source, category, notes):
    conn = get_connection()
    cursor = conn.cursor()
    query = """
        INSERT INTO income (date, amount, source, category, notes, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.execute(
        query,
        (
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

def insert_expense(date_val, amount, category, subcategory, payment_method, expense_type, notes):
    conn = get_connection()
    cursor = conn.cursor()
    query = """
        INSERT INTO expenses (date, amount, category, subcategory, payment_method, expense_type, notes, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(
        query,
        (
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

def get_last_income(limit=20):
    conn = get_connection()
    query = """
        SELECT id, date, amount, source, category, notes
        FROM income
        ORDER BY date DESC, id DESC
        LIMIT %s
    """
    df = pd.read_sql(query, conn, params=(limit,))
    conn.close()
    return df

def get_last_expenses(limit=20):
    conn = get_connection()
    query = """
        SELECT id, date, amount, category, subcategory, payment_method, expense_type, notes
        FROM expenses
        ORDER BY date DESC, id DESC
        LIMIT %s
    """
    df = pd.read_sql(query, conn, params=(limit,))
    conn.close()
    return df

# --------- INTERFAZ STREAMLIT --------- #

st.set_page_config(page_title="Finanzas personales", layout="wide")
st.title("App de finanzas personales")

st.markdown("Elige si quieres registrar / consultar **gastos** o **ingresos**.")

modo = st.radio(
    "Sección:",
    ["Gastos", "Ingresos"],
    horizontal=True,
)

# --------- GASTOS --------- #

if modo == "Gastos":
    st.header("Registro de gastos")

    with st.form("form_gasto"):
        col1, col2 = st.columns(2)
        with col1:
            fecha = st.date_input("Fecha del gasto", value=date.today())
            importe = st.number_input("Importe (€)", min_value=0.0, step=1.0)
            categoria = st.selectbox(
                "Categoría",
                [
                    "Comida",
                    "Vivienda",
                    "Transporte",
                    "Ocio",
                    "Salud",
                    "Deporte",
                    "Ropa",
                    "Otros",
                ],
            )
        with col2:
            subcategoria = st.text_input("Subcategoría (opcional)", value="")
            payment_method = st.selectbox(
                "Método de pago",
                ["Tarjeta", "Efectivo", "Bizum", "Transferencia", "Otro"],
            )
            expense_type = st.selectbox(
                "Tipo de gasto",
                ["Variable", "Fijo"],
            )

        notas = st.text_area("Notas (opcional)", height=80)

        submitted = st.form_submit_button("Guardar gasto")

    if submitted:
        if importe <= 0:
            st.error("El importe debe ser mayor que 0.")
        else:
            insert_expense(
                fecha,
                importe,
                categoria,
                subcategoria.strip() or None,
                payment_method,
                expense_type,
                notas.strip() or None,
            )
            st.success("Gasto guardado correctamente.")

    st.subheader("Últimos gastos registrados")

    df_exp = get_last_expenses(limit=20)
    if df_exp.empty:
        st.info("Todavía no hay gastos registrados.")
    else:
        st.dataframe(df_exp, use_container_width=True)

# --------- INGRESOS --------- #

else:
    st.header("Registro de ingresos")

    with st.form("form_ingreso"):
        col1, col2 = st.columns(2)
        with col1:
            fecha = st.date_input("Fecha del ingreso", value=date.today(), key="fecha_ingreso")
            importe = st.number_input("Importe (€)", min_value=0.0, step=1.0, key="importe_ingreso")
        with col2:
            source = st.text_input("Fuente (empresa, cliente, banco…)", value="")
            category = st.selectbox(
                "Categoría",
                ["Nómina", "Extra", "Intereses / Dividendo", "Devolución", "Otro"],
            )

        notas = st.text_area("Notas (opcional)", height=80, key="notas_ingreso")

        submitted = st.form_submit_button("Guardar ingreso")

    if submitted:
        if importe <= 0:
            st.error("El importe debe ser mayor que 0.")
        else:
            insert_income(
                fecha,
                importe,
                source.strip() or None,
                category,
                notas.strip() or None,
            )
            st.success("Ingreso guardado correctamente.")

    st.subheader("Últimos ingresos registrados")

    df_inc = get_last_income(limit=20)
    if df_inc.empty:
        st.info("Todavía no hay ingresos registrados.")
    else:
        st.dataframe(df_inc, use_container_width=True)