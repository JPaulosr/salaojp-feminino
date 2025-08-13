import streamlit as st
import pandas as pd
import plotly.express as px
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials

st.set_page_config(layout="wide", page_title="💅 Dashboard Feminino", page_icon="💅")
st.title("💅 Dashboard Feminino")

# =========================
# CONFIGURAÇÃO GOOGLE SHEETS
# =========================
SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"
ABA_FEM_BASE = "Base de Dados Feminino"
ABA_STATUS_FEM = "clientes_status_feminino"

# =========================
# CONEXÃO
# =========================
@st.cache_resource
def conectar_sheets():
    info = st.secrets["GCP_SERVICE_ACCOUNT"]
    escopo = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    credenciais = Credentials.from_service_account_info(info, scopes=escopo)
    cliente = gspread.authorize(credenciais)
    return cliente.open_by_key(SHEET_ID)

# =========================
# CARREGAMENTO DE ABAS
# =========================
def _limpar_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]
    if "Data" in df.columns:
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
        df = df.dropna(subset=["Data"])
        df["Ano"] = df["Data"].dt.year
        df["Mês"] = df["Data"].dt.month
        df["Ano-Mês"] = df["Data"].dt.to_period("M").astype(str)
    if "Valor" in df.columns:
        df["ValorNum"] = pd.to_numeric(df["Valor"], errors="coerce").fillna(0)
    return df

@st.cache_data(ttl=300)
def carregar_base_feminina() -> pd.DataFrame:
    ss = conectar_sheets()
    ws = ss.worksheet(ABA_FEM_BASE)
    df = get_as_dataframe(ws)
    return _limpar_df(df)

@st.cache_data(ttl=300)
def carregar_status_feminino() -> pd.DataFrame:
    ss = conectar_sheets()
    ws = ss.worksheet(ABA_STATUS_FEM)
    df = get_as_dataframe(ws).dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]
    if "Cliente" not in df.columns:
        # tenta mapear nome da coluna de cliente, caso tenha outro nome
        possiveis = [c for c in df.columns if c.strip().lower() in ["nome", "nome do cliente"]]
        if possiveis:
            df["Cliente"] = df[possiveis[0]]
        else:
            df["Cliente"] = ""
    for col in ["Status", "Imagem", "Observação"]:
        if col not in df.columns:
            df[col] = ""
    df["Cliente_norm"] = df["Cliente"].astype(str).str.strip()
    return df

df = carregar_base_feminina()
if df.empty:
    st.warning("Sem dados na aba **Base de Dados Feminino**.")
    st.stop()

status_fem = carregar_status_feminino()

# =========================
# FILTROS DE ANO/MÊS
# =========================
st.sidebar.header("🎛️ Filtros")
anos_disponiveis = sorted(df["Ano"].dropna().unique(), reverse=True)
ano_escolhido = st.sidebar.selectbox("🗓️ Ano", anos_disponiveis)

meses_pt = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro",
}
meses_disponiveis = sorted(df[df["Ano"] == ano_escolhido]["Mês"].dropna().unique())
mes_opcoes = [meses_pt[m] for m in meses_disponiveis]
meses_selecionados = st.sidebar.multiselect("📆 Meses (opcional)", mes_opcoes, default=mes_opcoes)

if meses_selecionados:
    meses_numeros = [k for k, v in meses_pt.items() if v in meses_selecionados]
    df = df[(df["Ano"] == ano_escolhido) & (df["Mês"].isin(meses_numeros))]
else:
    df = df[df["Ano"] == ano_escolhido]

# =========================
# REGRAS DE "FIADO" (exclui só para receita)
# =========================
col_conta = next((c for c in df.columns if c.strip().lower() in
                  ["conta", "forma de pagamento", "pagamento", "status"]), None)
if col_conta:
    mask_fiado = df[col_conta].astype(str).str.strip().str.lower().eq("fiado")
else:
    mask_fiado = pd.Series(False, index=df.index)

df_receita = df[~mask_fiado].copy()

# =========================
# INDICADORES
# =========================
receita_total = df_receita["ValorNum"].sum() if "ValorNum" in df_receita.columns else 0.0
total_atendimentos = len(df)  # inclui fiado

# sua regra de clientes únicos (data limite)
data_limite = pd.to_datetime("2025-05-11")
antes = df[df["Data"] < data_limite]
depois = df[df["Data"] >= data_limite].drop_duplicates(subset=["Cliente", "Data"])
clientes_unicos = pd.concat([antes, depois])["Cliente"].nunique()

ticket_medio = receita_total / total_atendimentos if total_atendimentos else 0

c1, c2, c3, c4 = st.columns(4)
c1.metric("💰 Receita Total", f"R$ {receita_total:,.2f}".replace(",", "v").replace(".", ",").replace("v", "."))
c2.metric("📅 Total de Atendimentos", total_atendimentos)
c3.metric("🎯 Ticket Médio", f"R$ {ticket_medio:,.2f}".replace(",", "v").replace(".", ",").replace("v", "."))
c4.metric("🟢 Clientes Ativos", clientes_unicos)

# =========================
# GRÁFICOS
# =========================
st.markdown("### 📊 Receita por Funcionário (Feminino)")
if "Funcionário" in df_receita.columns:
    df_func = df_receita.groupby("Funcionário")["ValorNum"].sum().reset_index().rename(columns={"ValorNum": "Valor"})
    fig_func = px.bar(df_func, x="Funcionário", y="Valor", text_auto=True)
    fig_func.update_layout(height=400, yaxis_title="Receita (R$)", showlegend=False)
    st.plotly_chart(fig_func, use_container_width=True)
else:
    st.info("A coluna **Funcionário** não existe na base.")

st.markdown("### 🧾 Receita por Tipo")
df_tipo = df_receita.copy()
if "Serviço" in df_tipo.columns:
    df_tipo["Tipo"] = df_tipo["Serviço"].apply(
        lambda x: "Combo" if "combo" in str(x).lower()
        else "Produto" if any(k in str(x).lower() for k in ["gel", "produto"])
        else "Serviço"
    )
    df_pizza = df_tipo.groupby("Tipo")["ValorNum"].sum().reset_index().rename(columns={"ValorNum": "Valor"})
    fig_pizza = px.pie(df_pizza, values="Valor", names="Tipo", title="Distribuição de Receita")
    fig_pizza.update_traces(textinfo="percent+label")
    st.plotly_chart(fig_pizza, use_container_width=True)

# =========================
# TOP 10 CLIENTES (excluindo nomes genéricos)
# =========================
st.markdown("### 🥇 Top 10 Clientes (Feminino)")
nomes_excluir = ["boliviano", "brasileiro", "menino"]

cnt = df.groupby("Cliente")["Serviço"].count().rename("Qtd_Serviços") if "Serviço" in df.columns else pd.Series(dtype=int)
val = df_receita.groupby("Cliente")["ValorNum"].sum().rename("Valor") if "ValorNum" in df_receita.columns else pd.Series(dtype=float)

df_top = pd.concat([cnt, val], axis=1).reset_index().fillna(0)
df_top = df_top[~df_top["Cliente"].str.lower().isin(nomes_excluir)]
df_top = df_top.sort_values(by="Valor", ascending=False).head(10)
df_top["Valor Formatado"] = df_top["Valor"].apply(lambda x: f"R$ {x:,.2f}".replace(",", "v").replace(".", ",").replace("v", "."))
st.dataframe(df_top[["Cliente", "Qtd_Serviços", "Valor Formatado"]], use_container_width=True)

st.markdown("---")

# =========================
# SINCRONIZAÇÃO: FEMININO -> clientes_status_feminino
# =========================
st.subheader("🔁 Sincronização de clientes (Feminino → clientes_status_feminino)")

def normalizar_nome(s):
    return str(s).strip()

def montar_df_upsert(df_fem_base: pd.DataFrame, df_status_fem: pd.DataFrame) -> pd.DataFrame:
    clientes_base = (
        df_fem_base["Cliente"]
        .dropna()
        .astype(str)
        .map(normalizar_nome)
        .unique()
        .tolist()
        if "Cliente" in df_fem_base.columns else []
    )
    existentes = set(df_status_fem["Cliente_norm"].dropna().astype(str).tolist())
    novos = [c for c in clientes_base if c and c not in existentes]

    if not novos:
        return pd.DataFrame(columns=df_status_fem.columns)

    colunas = list(df_status_fem.columns)
    for col in ["Cliente", "Cliente_norm", "Status", "Imagem", "Observação"]:
        if col not in colunas:
            colunas.append(col)

    linhas = []
    for c in novos:
        linha = {k: "" for k in colunas}
        linha["Cliente"] = c
        linha["Cliente_norm"] = c
        linha["Status"] = "Ativo"
        linha["Imagem"] = ""
        linha["Observação"] = ""
        linhas.append(linha)
    return pd.DataFrame(linhas)[colunas]

if st.button("🚀 Sincronizar clientes (Feminino → clientes_status_feminino)"):
    try:
        base_fem = carregar_base_feminina()
        status_fem_atual = carregar_status_feminino()
        df_novos = montar_df_upsert(base_fem, status_fem_atual)
        if df_novos.empty:
            st.success("Tudo certo! Nenhum cliente novo para adicionar.")
        else:
            ss = conectar_sheets()
            ws = ss.worksheet(ABA_STATUS_FEM)

            status_fem_limpo = status_fem_atual.drop(columns=["Cliente_norm"], errors="ignore")
            df_novos_limpo = df_novos.drop(columns=["Cliente_norm"], errors="ignore")

            colunas_final = list(status_fem_limpo.columns)
            df_final = pd.concat(
                [status_fem_limpo, df_novos_limpo.reindex(columns=colunas_final)],
                ignore_index=True
            )

            ws.clear()
            set_with_dataframe(ws, df_final, include_index=False, include_column_header=True, resize=True)
            st.success(f"Clientes adicionados: {len(df_novos)}")
    except Exception as e:
        st.error(f"Falha na sincronização: {e}")

st.caption("Criado para a base **Feminino** • Sincroniza clientes para **clientes_status_feminino**")
