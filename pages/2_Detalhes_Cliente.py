import streamlit as st
import pandas as pd
import plotly.express as px
from babel.dates import format_date
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials
import unicodedata
from datetime import datetime

st.set_page_config(layout="wide")
st.title("💅 Detalhes da Cliente (Feminino)")

# ========================
# CONFIG DA PLANILHA
# ========================
SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"
BASE_ALVOS = [
    "base de dados feminino", "base de dados - feminino",
    "base de dados (feminino)", "base de dados feminino "
]

# ========================
# UTILS
# ========================
def parse_valor(v):
    """Converte valores para float"""
    if pd.isna(v):
        return 0.0
    try:
        v = str(v).replace("R$", "").replace(".", "").replace(",", ".").strip()
        return float(v)
    except:
        return 0.0

def moeda(v):
    try:
        return f"R$ {float(v):,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
    except:
        return "R$ 0,00"

def normalize_text(text):
    return ''.join(
        c for c in unicodedata.normalize('NFD', str(text))
        if unicodedata.category(c) != 'Mn'
    ).lower().strip()

@st.cache_data(ttl=300)
def carregar_dados():
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=["https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"]
    )
    gc = gspread.authorize(creds)
    planilha = gc.open_by_key(SHEET_ID)
    abas = [s.title for s in planilha.worksheets()]

    aba_encontrada = None
    for alvo in BASE_ALVOS:
        for nome_aba in abas:
            if normalize_text(nome_aba) == normalize_text(alvo):
                aba_encontrada = nome_aba
                break
        if aba_encontrada:
            break

    if not aba_encontrada:
        st.error("Aba 'Base de Dados Feminino' não encontrada.")
        return pd.DataFrame()

    ws = planilha.worksheet(aba_encontrada)
    df = get_as_dataframe(ws, evaluate_formulas=True)
    df = df.dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]
    return df

# ========================
# CARREGA DADOS
# ========================
df = carregar_dados()
if df.empty:
    st.stop()

df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
df["ValorNum"] = df["Valor"].apply(parse_valor)

# ========================
# FILTRO DE CLIENTE
# ========================
clientes = sorted(df["Cliente"].dropna().unique())
cliente_sel = st.selectbox("Selecione a cliente", clientes)

# Mantém ano atual por padrão mesmo após reload
ano_atual = datetime.now().year
if "ano_selecionado" not in st.session_state:
    st.session_state["ano_selecionado"] = ano_atual

anos = sorted(df["Data"].dt.year.dropna().unique(), reverse=True)
ano_sel = st.selectbox(
    "Selecione o ano",
    anos,
    index=anos.index(st.session_state["ano_selecionado"]) if st.session_state["ano_selecionado"] in anos else 0
)
st.session_state["ano_selecionado"] = ano_sel

# ========================
# FILTRAR DADOS
# ========================
dados_cli = df[(df["Cliente"] == cliente_sel) & (df["Data"].dt.year == ano_sel)].copy()

# ========================
# GRÁFICO RECEITA MENSAL
# ========================
receita_mes = dados_cli.groupby(dados_cli["Data"].dt.month)["ValorNum"].sum().reset_index()
receita_mes.columns = ["MêsNum", "Valor"]
receita_mes["Mês"] = receita_mes["MêsNum"].apply(lambda x: format_date(datetime(ano_sel, x, 1), "MMMM", locale="pt_BR").title())
fig = px.bar(receita_mes, x="Mês", y="Valor", text=receita_mes["Valor"].apply(moeda),
             title=f"Receita Mensal - {ano_sel}", template="plotly_white")
st.plotly_chart(fig, use_container_width=True)

# ========================
# SERVIÇOS (no período)
# ========================
if "Serviço" in dados_cli.columns:
    serv = (
        dados_cli.groupby("Serviço")["ValorNum"].sum()
        .reset_index().sort_values("ValorNum", ascending=False)
    )
    serv["Valor"] = serv["ValorNum"].apply(moeda)
    st.markdown("**Serviços realizados (no período)**")
    st.dataframe(serv[["Serviço", "Valor"]], use_container_width=True)

# ========================
# DETALHES (no período) — Data em dd/mm/aaaa
# ========================
cols = ["Data", "Serviço", "Conta", "ValorNum"] if "Serviço" in dados_cli.columns else ["Data", "Conta", "ValorNum"]
hist = dados_cli[cols].copy().rename(columns={"ValorNum": "Valor"})

# Formatações
hist["Data"] = pd.to_datetime(hist["Data"], errors="coerce")
hist["DataBR"] = hist["Data"].dt.strftime("%d/%m/%Y")
hist["Valor"] = hist["Valor"].apply(moeda)
hist["Mês"] = hist["Data"].apply(lambda x: format_date(x, "MMMM yyyy", locale="pt_BR").title())

# Ordena e renomeia DataBR -> Data para exibir
hist.sort_values("Data", ascending=False, inplace=True)
hist.rename(columns={"DataBR": "Data"}, inplace=True)

# Exibe apenas colunas existentes (evita KeyError)
ordem_desejada = ["Data", "Serviço", "Conta", "Valor", "Mês"]
exibir_cols = [c for c in ordem_desejada if c in hist.columns]

st.markdown("**Histórico de atendimentos (no período)**")
st.dataframe(hist[exibir_cols].reset_index(drop=True), use_container_width=True)
