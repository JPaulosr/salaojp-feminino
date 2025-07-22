import streamlit as st
import pandas as pd
from google.oauth2.service_account import Credentials
import gspread

st.set_page_config(page_title="Painel do Cliente", layout="wide")
st.title("💇 Painel do Cliente")

# --- Autenticação com Google Sheets ---
@st.cache_data
def carregar_base():
    escopos = ["https://www.googleapis.com/auth/spreadsheets"]
    credenciais = Credentials.from_service_account_info(
        st.secrets["GCP_SERVICE_ACCOUNT"], scopes=escopos
    )
    cliente = gspread.authorize(credenciais)
    planilha = cliente.open_by_url(st.secrets["PLANILHA_URL"])
    aba = planilha.worksheet("Base de Dados")
    dados = aba.get_all_records()
    return pd.DataFrame(dados)

base = carregar_base()

# --- Normalização e preparação ---
base.columns = [col.strip().capitalize() for col in base.columns]
base["Data"] = pd.to_datetime(base["Data"], errors="coerce")

# --- Seleção do cliente ---
clientes_unicos = sorted(base["Cliente"].dropna().unique())
cliente_selecionado = st.selectbox("🔎 Selecione seu nome", clientes_unicos)

# --- Dados do cliente ---
dados_cliente = base[base["Cliente"] == cliente_selecionado].copy()

if dados_cliente.empty:
    st.warning("Nenhum atendimento encontrado.")
else:
    st.subheader(f"📋 Histórico de {cliente_selecionado}")
    st.dataframe(dados_cliente[["Data", "Serviço", "Profissional", "Valor"]].sort_values("Data", ascending=False))

    st.subheader("📅 Próximos cuidados")
    ult_data = dados_cliente["Data"].max()
    if ult_data:
        st.markdown(f"- Último atendimento: **{ult_data.strftime('%d/%m/%Y')}**")
        st.markdown("- Recomendação: retorno a cada **30 dias**")
        proxima = ult_data + pd.Timedelta(days=30)
        st.markdown(f"- Próxima visita sugerida: **{proxima.strftime('%d/%m/%Y')}**")

# --- Imagem do cliente (opcional) ---
st.subheader("🖼️ Sua imagem no sistema")
status_url = "clientes_status"
try:
    aba_status = gspread.authorize(Credentials.from_service_account_info(
        st.secrets["GCP_SERVICE_ACCOUNT"], scopes=["https://www.googleapis.com/auth/spreadsheets"])
    ).open_by_url(st.secrets["PLANILHA_URL"]).worksheet(status_url)
    df_status = pd.DataFrame(aba_status.get_all_records())
    imagem = df_status[df_status["Cliente"] == cliente_selecionado]["Foto"].values
    if imagem and imagem[0]:
        st.image(imagem[0], width=300)
    else:
        st.info("Imagem não cadastrada.")
except:
    st.warning("Não foi possível carregar imagem do cliente.")
