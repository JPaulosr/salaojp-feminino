# 11F_Adicionar_Atendimento.py
# Adicionar Atendimento — versão FEMININO

import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from datetime import datetime

# =========================
# CONFIGURAÇÃO GOOGLE SHEETS
# =========================
SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"

# Possíveis nomes da aba feminina
ABAS_FEMININO_ALVOS = [
    "Base de Dados Feminino",
    "base de dados feminino",
    "Base de Dados - Feminino",
    "base de dados - feminino",
    "Base de Dados (Feminino)",
    "base de dados (feminino)",
    "Base de Dados Feminino ",
]

COLS_OFICIAIS = [
    "Data", "Serviço", "Valor", "Conta", "Cliente", "Combo",
    "Funcionário", "Fase", "Tipo", "Período"
]
COLS_FIADO = ["StatusFiado", "IDLancFiado", "VencimentoFiado", "DataPagamento"]

# =========================
# CONEXÃO
# =========================
@st.cache_resource
def conectar_sheets():
    info = st.secrets["GCP_SERVICE_ACCOUNT"]
    escopo = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credenciais = Credentials.from_service_account_info(info, scopes=escopo)
    cliente = gspread.authorize(credenciais)
    return cliente.open_by_key(SHEET_ID)

def obter_aba_feminino():
    sh = conectar_sheets()
    titulos = [ws.title for ws in sh.worksheets()]
    for alvo in ABAS_FEMININO_ALVOS:
        if alvo in titulos:
            return sh.worksheet(alvo)
    try:
        return sh.worksheet("Base de Dados Feminino")
    except gspread.exceptions.WorksheetNotFound:
        return sh.add_worksheet(title="Base de Dados Feminino", rows=1000, cols=40)

def ler_cabecalho(aba):
    try:
        headers = aba.row_values(1)
        return [h.strip() for h in headers] if headers else []
    except Exception:
        return []

# =========================
# CARGA E SALVAMENTO
# =========================
def carregar_base():
    aba = obter_aba_feminino()
    df = get_as_dataframe(aba).dropna(how="all")
    df.columns = [str(col).strip() for col in df.columns]
    for coluna in [*COLS_OFICIAIS, *COLS_FIADO]:
        if coluna not in df.columns:
            df[coluna] = ""
    norm = {"manha": "Manhã", "Manha": "Manhã", "manha ": "Manhã", "tarde": "Tarde", "noite": "Noite"}
    df["Período"] = df["Período"].astype(str).str.strip().replace(norm)
    df.loc[~df["Período"].isin(["Manhã", "Tarde", "Noite"]), "Período"] = ""
    df["Combo"] = df["Combo"].fillna("")
    return df, aba

def salvar_base(df_final):
    aba = obter_aba_feminino()
    headers_existentes = ler_cabecalho(aba)
    if not headers_existentes:
        headers_existentes = [*COLS_OFICIAIS, *COLS_FIADO]
    colunas_alvo = list(dict.fromkeys([*headers_existentes, *COLS_OFICIAIS, *COLS_FIADO]))
    for col in colunas_alvo:
        if col not in df_final.columns:
            df_final[col] = ""
    df_final = df_final[colunas_alvo]
    aba.clear()
    set_with_dataframe(aba, df_final, include_index=False, include_column_header=True)

# =========================
# HELPERS
# =========================
def obter_valor_servico(servico):
    for chave in valores_servicos.keys():
        if chave.lower() == servico.lower():
            return valores_servicos[chave]
    return 0.0

def ja_existe_atendimento(cliente, data, servico, combo=""):
    df, _ = carregar_base()
    df["Combo"] = df["Combo"].fillna("")
    existe = df[
        (df["Cliente"] == cliente) &
        (df["Data"] == data) &
        (df["Serviço"] == servico) &
        (df["Combo"] == combo)
    ]
    return not existe.empty

def _preencher_fiado_vazio(linha: dict):
    for c in COLS_FIADO:
        linha.setdefault(c, "")
    return linha

# =========================
# VALORES PADRÃO
# =========================
valores_servicos = {
    "Progressiva": 150.0,
    "Escova": 35.0,
    "Designer de Henna": 30.0,
    "Pé/Mão": 50.0,
    "Manicure": 25.0,
    "Pedicure": 30.0,
}

# =========================
# UI
# =========================
st.set_page_config(page_title="Adicionar Atendimento (Feminino)", page_icon="💅", layout="wide")
st.title("💅 Adicionar Atendimento — Feminino")

df_existente, _ = carregar_base()
df_existente["DataParsed"] = pd.to_datetime(df_existente["Data"], errors="coerce")

clientes_existentes = sorted(df_existente["Cliente"].dropna().astype(str).unique())
servicos_existentes = sorted(df_existente["Serviço"].dropna().astype(str).str.strip().unique())
contas_existentes = sorted(df_existente["Conta"].dropna().astype(str).unique())
combos_existentes = sorted(df_existente["Combo"].dropna().astype(str).unique())
funcionarios_existentes = sorted(df_existente["Funcionário"].dropna().astype(str).unique())

# Se não houver funcionário registrado, usa "Meire"
if not funcionarios_existentes:
    funcionarios_existentes = ["Meire"]

# =========================
# FORMULÁRIO
# =========================
col1, col2 = st.columns(2)
with col1:
    data = st.date_input("Data", value=datetime.today()).strftime("%d/%m/%Y")
    cliente = st.selectbox("Nome da Cliente", [""] + clientes_existentes)
    novo_nome = st.text_input("Ou digite um novo nome de cliente")
    cliente = novo_nome.strip() if novo_nome.strip() else cliente

    ultimo = df_existente[df_existente["Cliente"] == cliente]
    if not ultimo.empty:
        ultimo = ultimo.sort_values("DataParsed", ascending=False).iloc[0]
        conta_sugerida = str(ultimo.get("Conta", "") or "")
        funcionario_sugerido = str(ultimo.get("Funcionário", "") or "Meire")
        combo_sugerido = str(ultimo.get("Combo", "") or "")
    else:
        conta_sugerida = ""
        funcionario_sugerido = "Meire"
        combo_sugerido = ""

    conta = st.selectbox("Forma de Pagamento", list(dict.fromkeys(([conta_sugerida] if conta_sugerida else []) + contas_existentes)))
    combo = st.selectbox("Combo (opcional — use 'serv1+serv2')", [""] + list(dict.fromkeys(([combo_sugerido] if combo_sugerido else []) + combos_existentes)))

with col2:
    funcionario = st.selectbox(
        "Funcionário",
        funcionarios_existentes,
        index=(funcionarios_existentes.index(funcionario_sugerido) if funcionario_sugerido in funcionarios_existentes else 0)
    )
    tipo = st.selectbox("Tipo", ["Serviço", "Produto"])

fase = "Dono + funcionário"
periodo_opcao = st.selectbox("Período do Atendimento", ["Manhã", "Tarde", "Noite"])

if "combo_salvo_fem" not in st.session_state:
    st.session_state.combo_salvo_fem = False
if "simples_salvo_fem" not in st.session_state:
    st.session_state.simples_salvo_fem = False

if st.button("🧹 Limpar formulário"):
    st.session_state.combo_salvo_fem = False
    st.session_state.simples_salvo_fem = False
    st.rerun()

# =========================
# SALVAMENTO
# =========================
def salvar_combo(combo_txt, valores_customizados):
    df, _ = carregar_base()
    servicos = [s.strip() for s in combo_txt.split("+") if s.strip()]
    novas = []
    for servico in servicos:
        valor = valores_customizados.get(servico, obter_valor_servico(servico))
        linha = {
            "Data": data,
            "Serviço": servico,
            "Valor": valor,
            "Conta": conta,
            "Cliente": cliente,
            "Combo": combo_txt,
            "Funcionário": funcionario,
            "Fase": fase,
            "Tipo": tipo,
            "Período": periodo_opcao,
        }
        novas.append(_preencher_fiado_vazio(linha))
    salvar_base(pd.concat([df, pd.DataFrame(novas)], ignore_index=True))

def salvar_simples(servico, valor):
    df, _ = carregar_base()
    nova = {
        "Data": data,
        "Serviço": servico,
        "Valor": valor,
        "Conta": conta,
        "Cliente": cliente,
        "Combo": "",
        "Funcionário": funcionario,
        "Fase": fase,
        "Tipo": tipo,
        "Período": periodo_opcao,
    }
    nova = _preencher_fiado_vazio(nova)
    salvar_base(pd.concat([df, pd.DataFrame([nova])], ignore_index=True))

# =========================
# LÓGICA DE SALVAR
# =========================
if combo:
    st.subheader("💰 Edite os valores do combo antes de salvar:")
    valores_customizados = {}
    for servico in [s.strip() for s in combo.split("+") if s.strip()]:
        valor_padrao = obter_valor_servico(servico)
        valor = st.number_input(f"{servico} (padrão: R$ {valor_padrao})", value=valor_padrao, step=1.0, key=f"valor_{servico}")
        valores_customizados[servico] = valor

    if not st.session_state.combo_salvo_fem:
        if st.button("✅ Confirmar e Salvar Combo"):
            if any(ja_existe_atendimento(cliente, data, s, combo) for s in combo.split("+")):
                st.warning("⚠️ Combo já registrado para esta cliente e data.")
            else:
                salvar_combo(combo, valores_customizados)
                st.session_state.combo_salvo_fem = True
                st.success(f"✅ Atendimento salvo com sucesso para {cliente} no dia {data}.")
    else:
        if st.button("➕ Novo Atendimento"):
            st.session_state.combo_salvo_fem = False
            st.rerun()
else:
    st.subheader("✂️ Selecione o serviço e valor:")
    servico = st.selectbox("Serviço", sorted(set(servicos_existentes).union(valores_servicos.keys())))
    valor_sugerido = obter_valor_servico(servico)
    valor = st.number_input("Valor", value=valor_sugerido, step=1.0)

    if not st.session_state.simples_salvo_fem:
        if st.button("📁 Salvar Atendimento"):
            if ja_existe_atendimento(cliente, data, servico):
                st.warning("⚠️ Atendimento já registrado para esta cliente, data e serviço.")
            else:
                salvar_simples(servico, valor)
                st.session_state.simples_salvo_fem = True
                st.success(f"✅ Atendimento salvo com sucesso para {cliente} no dia {data}.")
    else:
        if st.button("➕ Novo Atendimento"):
            st.session_state.simples_salvo_fem = False
            st.rerun()
