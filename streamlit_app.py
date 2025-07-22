import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# Autenticação com Google Sheets
scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
credentials = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
client = gspread.authorize(credentials)

# Nome da planilha e aba
spreadsheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE/edit")
sheet = spreadsheet.worksheet("Base de Dados")

# Carregar dados
dados = pd.DataFrame(sheet.get_all_records())
dados["Data"] = pd.to_datetime(dados["Data"], dayfirst=True)

# Sidebar – Seleção do Cliente
st.sidebar.markdown("🔎 **Selecione seu nome**")
clientes_unicos = sorted(dados["Cliente"].unique())
nome_cliente = st.sidebar.selectbox(" ", clientes_unicos)

# Filtrar dados do cliente selecionado
dados_cliente = dados[dados["Cliente"] == nome_cliente]

# Título
st.markdown(f"### 📋 Histórico de {nome_cliente.lower()}")

# Verificação segura das colunas
colunas_esperadas = ["Data", "Serviço", "Profissional", "Valor"]
colunas_disponiveis = dados_cliente.columns.tolist()
colunas_para_exibir = [col for col in colunas_esperadas if col in colunas_disponiveis]

if colunas_para_exibir:
    st.dataframe(dados_cliente[colunas_para_exibir].sort_values("Data", ascending=False))
else:
    st.warning("❗ Nenhum dado disponível para exibir o histórico desse cliente.")
