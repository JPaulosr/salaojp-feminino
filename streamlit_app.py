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
dados["Data"] = pd.to_datetime(dados["Data"], dayfirst=True, errors="coerce")
dados = dados.dropna(subset=["Data", "Cliente"])

# Normalizar nomes (tira espaços e padroniza para comparação)
dados["Cliente"] = dados["Cliente"].astype(str).str.strip()

# Sidebar – Seleção do Cliente
st.sidebar.markdown("🔎 **Selecione seu nome**")
clientes_unicos = sorted(dados["Cliente"].unique())
nome_cliente = st.sidebar.selectbox(" ", clientes_unicos)

# Filtrar dados do cliente selecionado
dados_cliente = dados[dados["Cliente"] == nome_cliente]

# Título principal
st.markdown(f"### 📋 Histórico de {nome_cliente}")

# Verificação segura das colunas
colunas_esperadas = ["Data", "Serviço", "Profissional", "Valor"]
colunas_disponiveis = dados_cliente.columns.tolist()
colunas_para_exibir = [col for col in colunas_esperadas if col in colunas_disponiveis]

# Exibir tabela ou aviso
if not dados_cliente.empty and colunas_para_exibir:
    tabela = dados_cliente[colunas_para_exibir].sort_values("Data", ascending=False).copy()
    tabela["Data"] = tabela["Data"].dt.strftime("%d/%m/%Y")  # formato brasileiro
    st.dataframe(tabela, use_container_width=True)
else:
    st.info("⚠️ Nenhum atendimento encontrado para este nome ainda. Assim que houver registros, eles aparecerão aqui.")
