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


# --------- CONFIGURACIÓN PÁGINA --------- #

st.set_page_config(page_title="Finanzas personales", layout="wide")

# Estado inicial del menú (nuestra variable interna)
if "menu" not in st.session_state:
    st.session_state["menu"] = "Inicio"

menu_options = ["Inicio", "Registro", "Resumen"]


def _update_menu_from_radio():
    # Cuando el usuario cambia el radio, actualizamos nuestro estado interno
    st.session_state["menu"] = st.session_state["menu_radio"]


# --------- MENÚ LATERAL --------- #

st.sidebar.title("Menú")
st.sidebar.radio(
    "Ir a:",
    menu_options,
    index=menu_options.index(st.session_state["menu"]),
    key="menu_radio",
    on_change=_update_menu_from_radio,
)

menu = st.session_state["menu"]

st.title("App de finanzas personales")

# ==========================
#       VISTA: INICIO
# ==========================

if menu == "Inicio":
    st.subheader("¿Qué quieres hacer hoy?")
    st.markdown("Selecciona una opción para continuar:")

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        b1 = st.button("➕ Registrar gastos / ingresos", use_container_width=True)
        b2 = st.button("📊 Ver resumen de mis finanzas", use_container_width=True)

    if b1:
        st.session_state["menu"] = "Registro"
        st.rerun()

    if b2:
        st.session_state["menu"] = "Resumen"
        st.rerun()

# ==========================
#       VISTA: REGISTRO
# ==========================

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

# ==========================
#       VISTA: RESUMEN
# ==========================

elif menu == "Resumen":
    st.subheader("Resumen de tus finanzas")

    conn = get_connection()
    df_income = pd.read_sql("SELECT * FROM income", conn)
    df_expenses = pd.read_sql("SELECT * FROM expenses", conn)
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

                # Por defecto AHORA: sin meses seleccionados (solo año prefiltrado)
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

                if use_full_year or not selected_month_labels:
                    months_filter = months
                    periodo_texto = f"Año completo {selected_year}"
                else:
                    months_filter = [label_to_month[l] for l in selected_month_labels]
                    meses_txt = ", ".join(
                        [month_names[m].capitalize() for m in months_filter]
                    )
                    periodo_texto = f"{meses_txt} de {selected_year}"

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