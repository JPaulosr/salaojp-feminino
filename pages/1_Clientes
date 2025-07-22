import streamlit as st
import pandas as pd
import plotly.express as px
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials

st.set_page_config(layout="wide")
st.title("🧍‍♂️ Clientes - Receita Total")

# === CONFIGURAÇÃO GOOGLE SHEETS ===
SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"
BASE_ABA = "Base de Dados"
STATUS_ABA = "clientes_status"

# === Função para conectar ao Google Sheets ===
@st.cache_resource
def conectar_sheets():
    info = st.secrets["GCP_SERVICE_ACCOUNT"]
    escopo = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credenciais = Credentials.from_service_account_info(info, scopes=escopo)
    cliente = gspread.authorize(credenciais)
    return cliente.open_by_key(SHEET_ID)

# === Carregar dados principais ===
@st.cache_data
def carregar_dados():
    planilha = conectar_sheets()
    aba = planilha.worksheet(BASE_ABA)
    df = get_as_dataframe(aba).dropna(how="all")
    df.columns = [col.strip() for col in df.columns]
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
    df = df.dropna(subset=["Data"])
    df["Ano"] = df["Data"].dt.year.astype(int)
    return df

@st.cache_data
def carregar_status():
    try:
        planilha = conectar_sheets()
        aba = planilha.worksheet(STATUS_ABA)
        df_status = get_as_dataframe(aba).dropna(how="all")
        df_status.columns = [col.strip() for col in df_status.columns]
        return df_status[["Cliente", "Status"]]
    except:
        return pd.DataFrame(columns=["Cliente", "Status"])

# === Atualizar status de clientes automaticamente ===
def atualizar_status_clientes(ultimos_status):
    try:
        planilha = conectar_sheets()
        aba_status = planilha.worksheet(STATUS_ABA)
        dados = aba_status.get_all_records()

        atualizados = 0
        for i, linha in enumerate(dados, start=2):  # começa na linha 2
            nome = linha.get("Cliente", "").strip()
            status_atual = linha.get("Status", "").strip()
            status_novo = ultimos_status.get(nome)

            if status_novo and status_novo != status_atual:
                aba_status.update_cell(i, 2, status_novo)  # coluna 2 = "Status"
                atualizados += 1

        return atualizados
    except Exception as e:
        st.warning(f"⚠️ Erro ao atualizar status dos clientes: {e}")
        return 0

# === Executa carregamento e atualiza status ===
df = carregar_dados()
df_status = carregar_status()

# === Lógica de atualização de status ===
hoje = pd.Timestamp.today().normalize()
limite_dias = 90

ultimos = df.groupby("Cliente")["Data"].max().reset_index()
ultimos["DiasDesde"] = (hoje - ultimos["Data"]).dt.days
ultimos["StatusNovo"] = ultimos["DiasDesde"].apply(lambda x: "Inativo" if x > limite_dias else "Ativo")

status_atualizado = dict(zip(ultimos["Cliente"], ultimos["StatusNovo"]))
qtd = atualizar_status_clientes(status_atualizado)
if qtd > 0:
    st.success(f"🔄 {qtd} cliente(s) tiveram seus status atualizados automaticamente.")

# === Indicadores ===
clientes_unicos = df["Cliente"].nunique()
contagem_status = df_status["Status"].value_counts().to_dict()
ativos = contagem_status.get("Ativo", 0)
ignorados = contagem_status.get("Ignorado", 0)
inativos = contagem_status.get("Inativo", 0)

st.markdown("### 📊 Indicadores Gerais")
col1, col2, col3, col4 = st.columns(4)
col1.metric("👥 Clientes únicos", clientes_unicos)
col2.metric("✅ Ativos", ativos)
col3.metric("🚫 Ignorados", ignorados)
col4.metric("🚩 Inativos", inativos)

# === Remove nomes genéricos ===
nomes_ignorar = ["boliviano", "brasileiro", "menino", "menino boliviano"]
normalizar = lambda s: str(s).lower().strip()
df = df[~df["Cliente"].apply(lambda x: normalizar(x) in nomes_ignorar)]

# === Ranking geral ===
ranking = df.groupby("Cliente")["Valor"].sum().reset_index()
ranking = ranking.sort_values(by="Valor", ascending=False)
ranking["Valor Formatado"] = ranking["Valor"].apply(
    lambda x: f"R$ {x:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
)

# === Busca dinâmica ===
st.subheader("📟 Receita total por cliente")
busca = st.text_input("🔎 Filtrar por nome").lower().strip()
if busca:
    ranking_exibido = ranking[ranking["Cliente"].str.lower().str.contains(busca)]
else:
    ranking_exibido = ranking.copy()
st.dataframe(ranking_exibido[["Cliente", "Valor Formatado"]], use_container_width=True)

# === Top 5 ===
st.subheader("🏆 Top 5 Clientes por Receita")
top5 = ranking.head(5)
fig_top = px.bar(
    top5,
    x="Cliente",
    y="Valor",
    text=top5["Valor"].apply(lambda x: f"R$ {x:,.0f}".replace(",", "v").replace(".", ",").replace("v", ".")),
    labels={"Valor": "Receita (R$)"},
    color="Cliente"
)
fig_top.update_traces(textposition="outside")
fig_top.update_layout(showlegend=False, height=400, template="plotly_white")
st.plotly_chart(fig_top, use_container_width=True)

# === Comparativo ===
st.subheader("⚖️ Comparar dois clientes")
clientes_disponiveis = ranking["Cliente"].tolist()
col1, col2 = st.columns(2)
c1 = col1.selectbox("👤 Cliente 1", clientes_disponiveis)
c2 = col2.selectbox("👤 Cliente 2", clientes_disponiveis, index=1 if len(clientes_disponiveis) > 1 else 0)

df_c1 = df[df["Cliente"] == c1]
df_c2 = df[df["Cliente"] == c2]

def resumo_cliente(df_cliente):
    total = df_cliente["Valor"].sum()
    servicos = df_cliente["Serviço"].nunique()
    media = df_cliente.groupby("Data")["Valor"].sum().mean()
    servicos_detalhados = df_cliente["Serviço"].value_counts().rename("Quantidade")
    return pd.Series({
        "Total Receita": f"R$ {total:,.2f}".replace(",", "v").replace(".", ",").replace("v", "."),
        "Serviços Distintos": servicos,
        "Tique Médio": f"R$ {media:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
    }), servicos_detalhados

resumo1, servicos1 = resumo_cliente(df_c1)
resumo2, servicos2 = resumo_cliente(df_c2)

resumo_geral = pd.concat([resumo1.rename(c1), resumo2.rename(c2)], axis=1)
servicos_comparativo = pd.concat([servicos1.rename(c1), servicos2.rename(c2)], axis=1).fillna(0).astype(int)

st.dataframe(resumo_geral, use_container_width=True)
st.markdown("**Serviços Realizados por Tipo**")
st.dataframe(servicos_comparativo, use_container_width=True)

# === Navegar para detalhamento ===
st.subheader("🔍 Ver detalhamento de um cliente")
cliente_escolhido = st.selectbox("📌 Escolha um cliente", clientes_disponiveis)

if st.button("➡ Ver detalhes"):
    st.session_state["cliente"] = cliente_escolhido
    st.switch_page("pages/2_DetalhesCliente.py")
