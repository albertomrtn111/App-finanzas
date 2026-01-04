import streamlit as st
import pandas as pd
from datetime import date, datetime
import altair as alt

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


def delete_expense(expense_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM expenses WHERE id = %s", (expense_id,))
    conn.commit()
    cursor.close()
    conn.close()


def delete_income(income_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM income WHERE id = %s", (income_id,))
    conn.commit()
    cursor.close()
    conn.close()

def get_expense_categories():
    conn = get_connection()
    try:
        df = pd.read_sql(
            "SELECT id, name FROM expense_categories ORDER BY name",
            conn,
        )
    finally:
        conn.close()
    return df


def add_expense_category(name: str):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO expense_categories (name, created_at)
            VALUES (%s, %s)
            """,
            (name, datetime.now()),
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()


def delete_expense_category(name: str):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        # No permitir borrar si está en uso en gastos o presupuestos
        cursor.execute(
            "SELECT COUNT(*) FROM expenses WHERE category = %s",
            (name,),
        )
        used_exp = cursor.fetchone()[0]

        cursor.execute(
            "SELECT COUNT(*) FROM budgets WHERE category = %s",
            (name,),
        )
        used_bud = cursor.fetchone()[0]

        if used_exp > 0 or used_bud > 0:
            raise ValueError(
                "No se puede eliminar la categoría porque está en uso en gastos o presupuestos."
            )

        cursor.execute(
            "DELETE FROM expense_categories WHERE name = %s",
            (name,),
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()


def get_income_categories():
    conn = get_connection()
    try:
        df = pd.read_sql(
            "SELECT id, name FROM income_categories ORDER BY name",
            conn,
        )
    finally:
        conn.close()
    return df


def add_income_category(name: str):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO income_categories (name, created_at)
            VALUES (%s, %s)
            """,
            (name, datetime.now()),
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()


def delete_income_category(name: str):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        # No permitir borrar si está en uso en ingresos
        cursor.execute(
            "SELECT COUNT(*) FROM income WHERE category = %s",
            (name,),
        )
        used_inc = cursor.fetchone()[0]

        if used_inc > 0:
            raise ValueError(
                "No se puede eliminar la categoría porque está en uso en ingresos."
            )

        cursor.execute(
            "DELETE FROM income_categories WHERE name = %s",
            (name,),
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()

def insert_investment(date_val, account, asset_type, contribution, current_value, notes):
    """
    contribution: aportación neta en el periodo (puede ser 0 o negativa si retiras).
    current_value: valor total de la cuenta tras esta actualización.
    """
    conn = get_connection()
    cursor = conn.cursor()
    query = """
        INSERT INTO investments
        (date, account, asset_type, contribution, current_value, notes, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(
        query,
        (
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


def get_investments_df():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM investments", conn)
    conn.close()
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df


def get_last_investments(limit=20):
    conn = get_connection()
    df = pd.read_sql(
        """
        SELECT *
        FROM investments
        ORDER BY date DESC, id DESC
        LIMIT %s
        """,
        conn,
        params=(limit,),
    )
    conn.close()
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df


def delete_investment(inv_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM investments WHERE id = %s", (inv_id,))
    conn.commit()
    cursor.close()
    conn.close()

def get_investment_products():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM investment_products ORDER BY name", conn)
    conn.close()
    return df


def get_asset_types():
    conn = get_connection()
    df = pd.read_sql("SELECT name FROM investment_asset_types ORDER BY name", conn)
    conn.close()
    return df["name"].tolist()


def insert_investment_product(name, asset_type):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO investment_products (name, asset_type, created_at)
        VALUES (%s, %s, %s)
        """,
        (name, asset_type, datetime.now()),
    )
    conn.commit()
    cursor.close()
    conn.close()


def delete_investment_product(prod_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM investment_products WHERE id = %s", (prod_id,))
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


# --------- CONFIGURACIÓN PÁGINA --------- #

st.set_page_config(page_title="Finanzas personales", layout="wide")

# Estado inicial del menú (nuestra variable interna)
if "menu" not in st.session_state:
    st.session_state["menu"] = "Inicio"

menu_options = ["Inicio", "Registro", "Resumen", "Presupuestos", "Categorías"]

def _update_menu_from_radio():
    # Cuando el usuario cambia el radio, actualizamos nuestro estado interno
    st.session_state["menu"] = st.session_state["menu_radio"]


# ========================================================================================================================================================================================
#       VISTA: MENU LATERAL
# ========================================================================================================================================================================================

# --------- ESTADO INICIAL DEL MENÚ --------- #
if "menu" not in st.session_state:
    st.session_state["menu"] = "Inicio"

# Definimos las páginas en orden y con etiqueta bonita
pages = [
    ("🏠 Inicio", "Inicio"),
    ("➕ Registro", "Registro"),
    ("📊 Resumen", "Resumen"),
    ("📈 Registro inversiones", "Registro inversiones"),
    ("📊 Resumen inversiones", "Resumen inversiones"),
    ("💰 Presupuestos", "Presupuestos"),
    ("🏷️ Categorías", "Categorías"),
    ("💼 Productos inversión", "Productos inversión"),

]

labels = [p[0] for p in pages]
values = [p[1] for p in pages]

# Índice del radio según la página actual
try:
    current_index = values.index(st.session_state["menu"])
except ValueError:
    current_index = 0

st.sidebar.title("Menú")

choice_label = st.sidebar.radio(
    "",
    labels,
    index=current_index,
    label_visibility="collapsed",
)

# Mapear de la etiqueta elegida al nombre interno de la página
label_to_value = {label: value for label, value in pages}
st.session_state["menu"] = label_to_value[choice_label]

menu = st.session_state["menu"]

st.title("App de finanzas personales")

# ========================================================================================================================================================================================
#       VISTA: VISTA DE INICIO
# ========================================================================================================================================================================================

if menu == "Inicio":
    st.markdown("Selecciona una opción en alguno de los bloques:")

    col_ing, col_inv, col_conf = st.columns(3)

    # -----------------------------
    #   COLUMNA 1: INGRESOS Y GASTOS
    # -----------------------------
    with col_ing:
        st.markdown("### 💶 Ingresos y gastos")
        st.markdown("Registra tus gastos/ ingresos y revisa su evolución.")

        b_reg_ig = st.button(
            "➕ Registrar gastos / ingresos",
            use_container_width=True,
            key="btn_reg_ig",
        )
        b_res_ig = st.button(
            "📊 Ver resumen de mis finanzas",
            use_container_width=True,
            key="btn_res_ig",
        )

    # -----------------------------
    #   COLUMNA 2: INVERSIONES
    # -----------------------------
    with col_inv:
        st.markdown("### 📈 Inversiones")
        st.markdown("Registra tus aportaciones y revisa su evolución.")

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
    #   COLUMNA 3: CONFIGURACIÓN
    # -----------------------------
    with col_conf:
        st.markdown("### ⚙️ Configuración")
        st.markdown("Ajusta tus presupuestos y categorías de ingresos/gastos.")

        b_pres = st.button(
            "💰 Configurar presupuestos",
            use_container_width=True,
            key="btn_pres",
        )
        b_cats = st.button(
            "🏷️ Categorías",
            use_container_width=True,
            key="btn_cats",
        )

    # -------- LÓGICA DE NAVEGACIÓN --------
    if b_reg_ig:
        st.session_state["menu"] = "Registro"
        st.rerun()

    if b_res_ig:
        st.session_state["menu"] = "Resumen"
        st.rerun()

    if b_reg_inv:
        st.session_state["menu"] = "Registro inversiones"
        st.rerun()

    if b_res_inv:
        st.session_state["menu"] = "Resumen inversiones"
        st.rerun()

    if b_pres:
        st.session_state["menu"] = "Presupuestos"
        st.rerun()

    if b_cats:
        st.session_state["menu"] = "Categorías"
        st.rerun()

# ========================================================================================================================================================================================
#       VISTA: REGISTROS
# ========================================================================================================================================================================================

elif menu == "Registro":
    st.subheader("Registro de movimientos")

    modo = st.radio(
        "¿Qué quieres registrar / consultar?",
        ["Gastos", "Ingresos"],
        horizontal=True,
    )

    # --------- GASTOS --------- #
    if modo == "Gastos":
        st.header("Registro de gastos")

        df_cats = get_expense_categories()
        if df_cats.empty:
            st.warning(
                "No tienes categorías de gasto definidas. "
                "Ve a la pestaña 'Categorías' para crear al menos una."
            )
        else:
            with st.form("form_gasto"):
                col1, col2 = st.columns(2)
                with col1:
                    fecha = st.date_input("Fecha del gasto", value=date.today())
                    importe = st.number_input("Importe (€)", min_value=0.0, step=1.0)
                    categoria = st.selectbox(
                        "Categoría",
                        df_cats["name"].tolist(),
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

            # Bloque para eliminar gasto
            with st.expander("Eliminar un gasto"):
                options = {
                    f"{row['id']} - {row['date']} - {row['amount']}€ - {row['category']}":
                    int(row["id"])
                    for _, row in df_exp.iterrows()
                }

                selected_label = st.selectbox(
                    "Selecciona el gasto a eliminar",
                    list(options.keys()),
                    key="sel_delete_expense",
                )

                if st.button("Eliminar gasto seleccionado"):
                    delete_expense(options[selected_label])
                    st.success("Gasto eliminado correctamente.")
                    st.rerun()

    # --------- INGRESOS --------- #
    else:
        st.header("Registro de ingresos")

        df_inc_cats = get_income_categories()
        if df_inc_cats.empty:
            st.warning(
                "No tienes categorías de ingreso definidas. "
                "Ve a la pestaña 'Categorías' para crear al menos una."
            )
        else:
            with st.form("form_ingreso"):
                col1, col2 = st.columns(2)
                with col1:
                    fecha = st.date_input(
                        "Fecha del ingreso",
                        value=date.today(),
                        key="fecha_ingreso",
                    )
                    importe = st.number_input(
                        "Importe (€)",
                        min_value=0.0,
                        step=1.0,
                        key="importe_ingreso",
                    )
                with col2:
                    source = st.text_input(
                        "Fuente (empresa, cliente, banco…)",
                        value="",
                    )
                    category = st.selectbox(
                        "Categoría",
                        df_inc_cats["name"].tolist(),
                    )

                notas = st.text_area(
                    "Notas (opcional)",
                    height=80,
                    key="notas_ingreso",
                )

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

            # Bloque para eliminar ingreso
            with st.expander("Eliminar un ingreso"):
                options_inc = {
                    f"{row['id']} - {row['date']} - {row['amount']}€ - {row['category']}":
                    int(row["id"])
                    for _, row in df_inc.iterrows()
                }

                selected_label_inc = st.selectbox(
                    "Selecciona el ingreso a eliminar",
                    list(options_inc.keys()),
                    key="sel_delete_income",
                )

                if st.button("Eliminar ingreso seleccionado"):
                    delete_income(options_inc[selected_label_inc])
                    st.success("Ingreso eliminado correctamente.")
                    st.rerun()


# ========================================================================================================================================================================================
#       VISTA: RESUMEN
# ========================================================================================================================================================================================

elif menu == "Resumen":
    st.subheader("Resumen de tus finanzas")

    conn = get_connection()
    df_income = pd.read_sql("SELECT * FROM income", conn)
    df_expenses = pd.read_sql("SELECT * FROM expenses", conn)
    # Leemos también presupuestos
    df_budgets = pd.read_sql("SELECT category, monthly_amount FROM budgets", conn)
    conn.close()

    if df_income.empty and df_expenses.empty:
        st.info("Todavía no hay datos de ingresos ni gastos para mostrar el resumen.")
    else:
        # Aseguramos que la columna date es datetime
        if not df_income.empty:
            df_income["date"] = pd.to_datetime(df_income["date"])
        if not df_expenses.empty:
            df_expenses["date"] = pd.to_datetime(df_expenses["date"])

        # Obtenemos años disponibles a partir de ambas tablas
        years = []
        if not df_income.empty:
            years.extend(df_income["date"].dt.year.tolist())
        if not df_expenses.empty:
            years.extend(df_expenses["date"].dt.year.tolist())

        years = sorted(set(years), reverse=True)

        if not years:
            st.info("No hay fechas válidas para construir el resumen.")
        else:
            # Selector de año
            selected_year = st.selectbox("Año", years, index=0)

            # Meses disponibles en ese año
            months = []
            if not df_income.empty:
                months.extend(
                    df_income.loc[df_income["date"].dt.year == selected_year, "date"]
                    .dt.month
                    .tolist()
                )
            if not df_expenses.empty:
                months.extend(
                    df_expenses.loc[df_expenses["date"].dt.year == selected_year, "date"]
                    .dt.month
                    .tolist()
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

                # Etiquetas de los meses disponibles
                month_labels = [month_names[m].capitalize() for m in months]
                label_to_month = dict(zip(month_labels, months))

                # Por defecto: sin meses seleccionados → año completo
                default_labels: list[str] = []

                col_f1, col_f2 = st.columns([2, 1])
                with col_f1:
                    selected_month_labels = st.multiselect(
                        "Meses (puedes seleccionar uno, varios o ninguno si usas el año completo)",
                        month_labels,
                        default=default_labels,
                    )
                with col_f2:
                    use_full_year = st.checkbox(
                        "Usar año completo",
                        value=False,
                    )

                # Lógica de periodo seleccionado
                if use_full_year or not selected_month_labels:
                    months_filter = months
                    periodo_texto = f"Año completo {selected_year}"
                    # Para presupuesto: 12 meses completos
                    n_months_budget = 12
                else:
                    months_filter = [label_to_month[l] for l in selected_month_labels]
                    meses_txt = ", ".join(
                        [month_names[m].capitalize() for m in months_filter]
                    )
                    periodo_texto = f"{meses_txt} de {selected_year}"
                    # Para presupuesto: nº de meses seleccionados
                    n_months_budget = len(set(months_filter))

                st.markdown(f"**Periodo seleccionado:** {periodo_texto}")

                # Filtrado de datos según año y meses (para KPIs y barras)
                def filter_df(df):
                    if df.empty:
                        return df
                    mask = df["date"].dt.year == selected_year
                    if months_filter:
                        mask &= df["date"].dt.month.isin(months_filter)
                    return df[mask]

                df_income_period = filter_df(df_income)
                df_expenses_period = filter_df(df_expenses)

                # DF filtrados solo por año (para la serie temporal de ahorro)
                if not df_income.empty:
                    df_income_year = df_income[df_income["date"].dt.year == selected_year]
                else:
                    df_income_year = df_income

                if not df_expenses.empty:
                    df_expenses_year = df_expenses[df_expenses["date"].dt.year == selected_year]
                else:
                    df_expenses_year = df_expenses

                # KPIs básicos
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

                # Tasa de ahorro con fondo verde/rojo
                with col4:
                    bg_color = "#d4edda" if savings_rate >= 0 else "#f8d7da"
                    text_color = "#155724" if savings_rate >= 0 else "#721c24"
                    st.markdown(
                        f"""
                        <div style="
                            padding: 0.75rem;
                            border-radius: 0.5rem;
                            background-color: {bg_color};
                            text-align: center;
                        ">
                            <div style="font-size: 0.9rem;">Tasa de ahorro</div>
                            <div style="
                                font-size: 1.3rem;
                                font-weight: bold;
                                color: {text_color};
                            ">
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

                if df_budgets.empty or df_budgets["monthly_amount"].sum() == 0:
                    st.info(
                        "No hay presupuestos definidos todavía. "
                        "Ve a la pestaña 'Presupuestos' para configurarlos."
                    )
                else:
                    monthly_total_budget = float(df_budgets["monthly_amount"].sum())
                    budget_period = monthly_total_budget * n_months_budget

                    if budget_period <= 0:
                        st.info("El presupuesto para el periodo es 0 €. Configura importes mayores en 'Presupuestos'.")
                    else:
                        ratio = total_expenses / budget_period if budget_period > 0 else 0.0

                        colb1, colb2, colb3 = st.columns(3)
                        with colb1:
                            st.metric(
                                "Presupuesto del periodo",
                                f"{budget_period:,.2f} €",
                            )
                        with colb2:
                            st.metric(
                                "Gasto del periodo",
                                f"{total_expenses:,.2f} €",
                            )
                        with colb3:
                            st.metric(
                                "% presupuesto consumido",
                                f"{ratio * 100:,.1f} %",
                            )

                        st.markdown("**Consumo de presupuesto del periodo**")

                        # Barra de progreso (hasta 100 % visual) + texto real
                        prog_ratio = max(0.0, min(1.0, ratio))
                        st.progress(prog_ratio)

                        if ratio <= 1:
                            st.caption(
                                f"Has consumido aproximadamente el {ratio * 100:,.1f} % del presupuesto del periodo."
                            )
                        else:
                            exceso = total_expenses - budget_period
                            st.warning(
                                f"Has superado el presupuesto del periodo en {exceso:,.2f} € "
                                f"({ratio * 100:,.1f} % del presupuesto)."
                            )

                st.markdown("---")

                # ===== SERIE TEMPORAL DE AHORRO MENSUAL (por año) =====
                st.subheader("Evolución mensual del ahorro")

                if df_income_year.empty and df_expenses_year.empty:
                    st.info("No hay datos de ingresos ni gastos para el año seleccionado.")
                else:
                    # Meses con datos en el año
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
                        sav_m = inc_m - exp_m
                        data.append(
                            {
                                "month_num": m,
                                "month_label": month_names[m].capitalize(),
                                "income": float(inc_m),
                                "expenses": float(exp_m),
                                "savings": float(sav_m),
                            }
                        )

                    if data:
                        df_sav = pd.DataFrame(data)

                        # Línea de ahorro por mes
                        base_sav = alt.Chart(df_sav).encode(
                            x=alt.X("month_label:N", title="Mes"),
                            y=alt.Y("savings:Q", title="Ahorro (€)"),
                        )

                        line_sav = base_sav.mark_line(point=True)
                        text_sav = base_sav.mark_text(
                            dy=-10,
                        ).encode(
                            text=alt.Text("savings:Q", format=".0f")
                        )

                        # Línea roja discontinua en y = 0
                        zero_line = (
                            alt.Chart(pd.DataFrame({"y": [0]}))
                            .mark_rule(color="red", strokeDash=[4, 4])
                            .encode(y="y:Q")
                        )

                        st.altair_chart(line_sav + text_sav + zero_line, use_container_width=True)
                    else:
                        st.info("No hay datos mensuales suficientes para mostrar la evolución del ahorro.")

                st.markdown("---")

                # Gráficos de distribución por categoría: gastos (izquierda) e ingresos (derecha)
                st.subheader("Distribución por categoría")

                col_g1, col_g2 = st.columns(2)

                # ---- GASTOS ----
                with col_g1:
                    st.markdown("**Gasto por categoría**")
                    if df_expenses_period.empty:
                        st.info("No hay gastos en el periodo seleccionado.")
                    else:
                        df_cat_exp = (
                            df_expenses_period
                            .groupby("category", as_index=False)["amount"]
                            .sum()
                        )

                        base_exp = alt.Chart(df_cat_exp).encode(
                            x=alt.X("amount:Q", title="Gasto (€)"),
                            y=alt.Y(
                                "category:N",
                                sort="-x",  # mayor a menor
                                title="Categoría",
                            ),
                        )

                        bars_exp = base_exp.mark_bar()
                        text_exp = base_exp.mark_text(
                            align="left",
                            baseline="middle",
                            dx=3,
                        ).encode(
                            text=alt.Text("amount:Q", format=".2f")
                        )

                        st.altair_chart(bars_exp + text_exp, use_container_width=True)

                # ---- INGRESOS ----
                with col_g2:
                    st.markdown("**Ingresos por categoría**")
                    if df_income_period.empty:
                        st.info("No hay ingresos en el periodo seleccionado.")
                    else:
                        df_cat_inc = (
                            df_income_period
                            .groupby("category", as_index=False)["amount"]
                            .sum()
                        )

                        base_inc = alt.Chart(df_cat_inc).encode(
                            x=alt.X("amount:Q", title="Ingresos (€)"),
                            y=alt.Y(
                                "category:N",
                                sort="-x",  # mayor a menor
                                title="Categoría",
                            ),
                        )

                        bars_inc = base_inc.mark_bar()
                        text_inc = base_inc.mark_text(
                            align="left",
                            baseline="middle",
                            dx=3,
                        ).encode(
                            text=alt.Text("amount:Q", format=".2f")
                        )

                        st.altair_chart(bars_inc + text_inc, use_container_width=True)

                # ---- PIE CHART FIJO vs VARIABLE ----
                st.subheader("Distribución de gasto fijo vs variable")

                if df_expenses_period.empty:
                    st.info("No hay gastos en el periodo seleccionado.")
                else:
                    df_type = df_expenses_period.copy()
                    df_type["expense_type"] = df_type["expense_type"].fillna("No definido")
                    df_type = (
                        df_type
                        .groupby("expense_type", as_index=False)["amount"]
                        .sum()
                    )

                    total_type = df_type["amount"].sum()
                    if total_type <= 0:
                        st.info("No hay importe de gastos en el periodo seleccionado.")
                    else:
                        df_type["percent"] = df_type["amount"] / total_type * 100
                        df_type["percent_label"] = df_type["percent"].map(lambda x: f"{x:.1f}%")

                        pie = (
                            alt.Chart(df_type)
                            .mark_arc()
                            .encode(
                                theta=alt.Theta("amount:Q", stack=True),
                                color=alt.Color("expense_type:N", title="Tipo de gasto"),
                                tooltip=["expense_type", "amount", "percent_label"],
                            )
                        )

                        text_pie = (
                            alt.Chart(df_type)
                            .mark_text(radius=80, size=12)
                            .encode(
                                theta=alt.Theta("amount:Q", stack=True),
                                color=alt.Color("expense_type:N", legend=None),
                                text=alt.Text("percent_label:N"),
                            )
                        )

                        st.altair_chart(pie + text_pie, use_container_width=True)

                st.markdown("---")

                # Tablas de detalle
                tab1, tab2 = st.tabs(["Gastos del periodo", "Ingresos del periodo"])

                with tab1:
                    if df_expenses_period.empty:
                        st.info("No hay gastos en el periodo seleccionado.")
                    else:
                        df_show_exp = df_expenses_period.copy()
                        df_show_exp["date"] = df_show_exp["date"].dt.date
                        st.dataframe(
                            df_show_exp[
                                [
                                    "date",
                                    "amount",
                                    "category",
                                    "subcategory",
                                    "payment_method",
                                    "expense_type",
                                    "notes",
                                ]
                            ],
                            use_container_width=True,
                        )

                with tab2:
                    if df_income_period.empty:
                        st.info("No hay ingresos en el periodo seleccionado.")
                    else:
                        df_show_inc = df_income_period.copy()
                        df_show_inc["date"] = df_show_inc["date"].dt.date
                        st.dataframe(
                            df_show_inc[
                                [
                                    "date",
                                    "amount",
                                    "source",
                                    "category",
                                    "notes",
                                ]
                            ],
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
    df_cats = get_expense_categories()

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
                "SELECT category, monthly_amount FROM budgets",
                conn,
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
                # Para simplificar: borramos todo y reinsertamos
                cursor.execute("DELETE FROM budgets")

                now = datetime.now()
                for cat, amount in new_values.items():
                    cursor.execute(
                        """
                        INSERT INTO budgets (category, monthly_amount, created_at, updated_at)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (cat, float(amount), now, now),
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
            "SELECT category, monthly_amount FROM budgets",
            conn,
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

    
# ========================================================================================================================================================================================
#       VISTA: CATEGORIAS
# ========================================================================================================================================================================================

elif menu == "Categorías":
    st.subheader("Categorías de movimientos")

    st.markdown(
        "Gestiona aquí las categorías que usarás en tus **gastos** y **ingresos**."
    )

    tab_g, tab_i = st.tabs(["Categorías de gasto", "Categorías de ingreso"])

    # ----- TAB GASTOS -----
    with tab_g:
        st.markdown("### Categorías de gasto")

        df_exp_cats = get_expense_categories()
        col_a, col_b = st.columns(2)

        with col_a:
            st.markdown("**Listado actual**")
            if df_exp_cats.empty:
                st.info("Todavía no tienes categorías de gasto definidas.")
            else:
                st.dataframe(df_exp_cats, use_container_width=True)

        with col_b:
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
                        add_expense_category(name_clean)
                        st.success(f"Categoría de gasto '{name_clean}' añadida correctamente.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"No se ha podido añadir la categoría: {e}")

            st.markdown("**Eliminar categoría de gasto**")
            if df_exp_cats.empty:
                st.info("No hay categorías de gasto para eliminar.")
            else:
                cat_exp_to_delete = st.selectbox(
                    "Selecciona la categoría a eliminar",
                    df_exp_cats["name"].tolist(),
                    key="delete_exp_cat_select",
                )
                if st.button("Eliminar categoría de gasto seleccionada"):
                    try:
                        delete_expense_category(cat_exp_to_delete)
                        st.success(f"Categoría de gasto '{cat_exp_to_delete}' eliminada correctamente.")
                        st.rerun()
                    except ValueError as ve:
                        st.warning(str(ve))
                    except Exception as e:
                        st.error(f"No se ha podido eliminar la categoría: {e}")

    # ----- TAB INGRESOS -----
    with tab_i:
        st.markdown("### Categorías de ingreso")

        df_inc_cats = get_income_categories()
        col_c, col_d = st.columns(2)

        with col_c:
            st.markdown("**Listado actual**")
            if df_inc_cats.empty:
                st.info("Todavía no tienes categorías de ingreso definidas.")
            else:
                st.dataframe(df_inc_cats, use_container_width=True)

        with col_d:
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
                        add_income_category(name_clean)
                        st.success(f"Categoría de ingreso '{name_clean}' añadida correctamente.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"No se ha podido añadir la categoría: {e}")

            st.markdown("**Eliminar categoría de ingreso**")
            if df_inc_cats.empty:
                st.info("No hay categorías de ingreso para eliminar.")
            else:
                cat_inc_to_delete = st.selectbox(
                    "Selecciona la categoría a eliminar",
                    df_inc_cats["name"].tolist(),
                    key="delete_inc_cat_select",
                )
                if st.button("Eliminar categoría de ingreso seleccionada"):
                    try:
                        delete_income_category(cat_inc_to_delete)
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

    df_inv = get_investments_df()
    df_products = get_investment_products()

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
            elif importe_mov <= 0:
                st.error("El importe del movimiento debe ser mayor que 0.")
            elif valor_actual < 0:
                st.error("El valor actual no puede ser negativo.")
            else:
                # Entrada = aportación positiva; salida = aportación negativa
                contrib_signed = importe_mov if "Entrada" in mov_type else -importe_mov

                insert_investment(
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

    df_last_mov = get_last_investments(limit=20)
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
                delete_investment(options_inv[selected_label_inv])
                st.success("Movimiento eliminado correctamente.")
                st.rerun()






# ========================================================================================================================================================================================
#       VISTA: PRODUCTOS DE INVERSION 
# ========================================================================================================================================================================================

elif menu == "Productos inversión":
    st.subheader("Productos de inversión")

    st.markdown(
        "Aquí defines los **productos de inversión** que podrás usar en el registro de inversiones.\n\n"
        "Por ejemplo:\n"
        "- Producto: `Indexa 10/10` → Tipo: `Fondo indexado`\n"
        "- Producto: `VWCE` → Tipo: `ETF`\n"
        "- Producto: `Cuenta remunerada X` → Tipo: `Cash`"
    )

    df_products = get_investment_products()
    asset_types = get_asset_types()

    # -------- FORMULARIO: NUEVO PRODUCTO -------- #
    st.markdown("### Añadir nuevo producto")

    with st.form("form_new_product"):
        col1, col2 = st.columns(2)
        with col1:
            new_name = st.text_input("Nombre del producto (obligatorio)", value="")
        with col2:
            new_asset_type = st.selectbox(
                "Tipo de activo",
                asset_types if asset_types else ["Otro"],
            )

        submitted_new_prod = st.form_submit_button("Guardar producto")

    if submitted_new_prod:
        if not new_name.strip():
            st.error("El nombre del producto no puede estar vacío.")
        else:
            try:
                insert_investment_product(new_name.strip(), new_asset_type)
                st.success("Producto de inversión creado correctamente.")
                st.rerun()
            except Exception as e:
                st.error(f"Error al crear el producto: {e}")

    st.markdown("---")

    # -------- LISTADO DE PRODUCTOS -------- #
    st.markdown("### Productos actuales")

    if df_products.empty:
        st.info("Todavía no has definido productos de inversión.")
    else:
        st.dataframe(
            df_products[["id", "name", "asset_type"]],
            use_container_width=True,
        )

        # Bloque para eliminar producto
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

            if st.button("Eliminar producto seleccionado"):
                delete_investment_product(options_prod[selected_label_prod])
                st.success("Producto eliminado correctamente.")
                st.rerun()



# ==============================================================================================================================
#       VISTA: RESUMEN INVERSIONES
# ==============================================================================================================================


elif menu == "Resumen inversiones":
    st.subheader("Resumen de mis inversiones")

    df_inv = get_investments_df()

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