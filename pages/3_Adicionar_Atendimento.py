# 11F_Adicionar_Atendimento.py
# Adicionar Atendimento — versão FEMININO
# - Lê e grava na aba "Base de Dados Feminino" (aceita variações de nome)
# - Preserva TODAS as colunas existentes (inclusive FIADO)
# - Combo com valores editáveis (um registro por serviço)
# - Autopreenchimento de conta/funcionário/combo pelo último atendimento do cliente
# - Evita duplicidade (Cliente + Data + Serviço + Combo)
# - Campo oficial de Período (Manhã/Tarde/Noite)
# - Botão "Limpar formulário"

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

# Tentativas de nomes para a aba Feminino (aceita variações comuns que você usa)
ABAS_FEMININO_ALVOS = [
    "Base de Dados Feminino",
    "base de dados feminino",
    "Base de Dados - Feminino",
    "base de dados - feminino",
    "Base de Dados (Feminino)",
    "base de dados (feminino)",
    "Base de Dados Feminino ",
]

# Colunas oficiais e colunas de FIADO (preservadas se existirem)
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
    # Procura por uma das variações de nome
    titulos = [ws.title for ws in sh.worksheets()]
    for alvo in ABAS_FEMININO_ALVOS:
        if alvo in titulos:
            return sh.worksheet(alvo)
    # Se não achar, usa o nome "Base de Dados Feminino" como padrão (cria se necessário)
    try:
        return sh.worksheet("Base de Dados Feminino")
    except gspread.exceptions.WorksheetNotFound:
        return sh.add_worksheet(title="Base de Dados Feminino", rows=1000, cols=40)

def ler_cabecalho(aba):
    try:
        headers = aba.row_values(1)
        headers = [h.strip() for h in headers] if headers else []
        return headers
    except Exception:
        return []

# =========================
# CARGA E SALVAMENTO
# =========================
def carregar_base():
    aba = obter_aba_feminino()
    df = get_as_dataframe(aba).dropna(how="all")
    df.columns = [str(col).strip() for col in df.columns]

    # Garante oficiais + fiado sem remover colunas já existentes
    for coluna in [*COLS_OFICIAIS, *COLS_FIADO]:
        if coluna not in df.columns:
            df[coluna] = ""

    # Normaliza Período
    if "Período" not in df.columns:
        df["Período"] = ""
    norm = {"manha": "Manhã", "Manha": "Manhã", "manha ": "Manhã", "tarde": "Tarde", "noite": "Noite"}
    df["Período"] = df["Período"].astype(str).str.strip().replace(norm)
    df.loc[~df["Período"].isin(["Manhã", "Tarde", "Noite"]), "Período"] = ""

    df["Combo"] = df["Combo"].fillna("")

    return df, aba

def salvar_base(df_final):
    aba = obter_aba_feminino()
    headers_existentes = ler_cabecalho(aba)

    # Se não houver cabeçalho ainda, inicia com oficiais + fiado
    if not headers_existentes:
        headers_existentes = [*COLS_OFICIAIS, *COLS_FIADO]

    # Constrói ordem-alvo preservando tudo
    colunas_alvo = list(dict.fromkeys([*headers_existentes, *COLS_OFICIAIS, *COLS_FIADO]))
    for col in colunas_alvo:
        if col not in df_final.columns:
            df_final[col] = ""

    # Reordena e escreve
    df_final = df_final[colunas_alvo]
    aba.clear()
    set_with_dataframe(aba, df_final, include_index=False, include_column_header=True)

# =========================
# REGRAS & HELPERS
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
# VALORES PADRÃO DE SERVIÇOS (inclui masculino e feminino)
# Ajuste livre conforme sua tabela
# =========================
valores_servicos = {
    # Masculino (mantidos)
    "Corte": 25.0,
    "Pezinho": 7.0,
    "Barba": 15.0,
    "Sobrancelha": 7.0,
    "Luzes": 45.0,
    "Pintura": 35.0,
    "Alisamento": 40.0,
    "Gel": 10.0,
    "Pomada": 15.0,
    # Feminino (exemplos comuns — ajuste aos seus valores)
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

# Normaliza datas (aceita dd/mm/aaaa e objetos datetime)
def _parse_data_txt(v):
    if pd.isna(v) or str(v).strip() == "":
        return pd.NaT
    if isinstance(v, (datetime, )):
        return pd.to_datetime(v)
    # tenta dd/mm/aaaa, depois iso
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return pd.to_datetime(str(v), format=fmt, errors="raise")
        except Exception:
            pass
    return pd.to_datetime(v, errors="coerce")

df_existente["__DataParsed"] = df_existente["Data"].apply(_parse_data_txt)

# Listas (usa a base toda, não só 2025)
clientes_existentes = sorted(df_existente["Cliente"].dropna().astype(str).unique())
servicos_existentes = sorted(df_existente["Serviço"].dropna().astype(str).str.strip().unique())
contas_existentes = sorted(df_existente["Conta"].dropna().astype(str).unique())
combos_existentes = sorted(df_existente["Combo"].dropna().astype(str).unique())

# =========================
# SELEÇÃO & AUTOPREENCHIMENTO
# =========================
col1, col2 = st.columns(2)

with col1:
    data = st.date_input("Data", value=datetime.today()).strftime("%d/%m/%Y")

    cliente = st.selectbox("Nome da Cliente", [""] + clientes_existentes)
    novo_nome = st.text_input("Ou digite um novo nome de cliente")
    cliente = novo_nome.strip() if novo_nome.strip() else cliente

    # Último registro da cliente (para sugerir conta/funcionário/combo)
    ultimo = df_existente[df_existente["Cliente"] == cliente]
    if not ultimo.empty:
        ultimo = ultimo.sort_values("__DataParsed", ascending=False).iloc[0]
        conta_sugerida = str(ultimo.get("Conta", "") or "")
        funcionario_sugerido = str(ultimo.get("Funcionário", "") or "JPaulo")
        combo_sugerido = str(ultimo.get("Combo", "") or "")
    else:
        conta_sugerida = ""
        funcionario_sugerido = "JPaulo"
        combo_sugerido = ""

    conta = st.selectbox(
        "Forma de Pagamento",
        list(dict.fromkeys(([conta_sugerida] if conta_sugerida else []) + contas_existentes + ["Carteira", "Nubank"]))
    )
    combo = st.selectbox(
        "Combo (opcional — use 'serv1+serv2')",
        [""] + list(dict.fromkeys(([combo_sugerido] if combo_sugerido else []) + combos_existentes))
    )

with col2:
    funcionario = st.selectbox(
        "Funcionário",
        ["JPaulo", "Vinicius"],
        index=(["JPaulo", "Vinicius"].index(funcionario_sugerido) if funcionario_sugerido in ["JPaulo", "Vinicius"] else 0)
    )
    tipo = st.selectbox("Tipo", ["Serviço", "Produto"])

fase = "Dono + funcionário"

# Período
periodo_opcao = st.selectbox("Período do Atendimento", ["Manhã", "Tarde", "Noite"])

# Estado
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
    servicos = [s for s in combo_txt.split("+") if s.strip()]
    novas = []
    for servico in servicos:
        servico_formatado = servico.strip()
        valor = valores_customizados.get(servico_formatado, obter_valor_servico(servico_formatado))
        linha = {
            "Data": data,
            "Serviço": servico_formatado,
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
    df_final = pd.concat([df, pd.DataFrame(novas)], ignore_index=True)
    salvar_base(df_final)

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
    df_final = pd.concat([df, pd.DataFrame([nova])], ignore_index=True)
    salvar_base(df_final)

# =========================
# FORMULÁRIO
# =========================
if combo:
    st.subheader("💰 Edite os valores do combo antes de salvar:")
    valores_customizados = {}
    for servico in [s for s in combo.split("+") if s.strip()]:
        servico_formatado = servico.strip()
        valor_padrao = obter_valor_servico(servico_formatado)
        valor = st.number_input(
            f"{servico_formatado} (padrão: R$ {valor_padrao})",
            value=float(valor_padrao), step=1.0, key=f"valor_fem_{servico_formatado}"
        )
        valores_customizados[servico_formatado] = valor

    if not st.session_state.combo_salvo_fem:
        if st.button("✅ Confirmar e Salvar Combo"):
            duplicado = any(ja_existe_atendimento(cliente, data, s.strip(), combo) for s in combo.split("+") if s.strip())
            if duplicado:
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
    # Lista de serviços: usa união dos existentes com os do dicionário
    servicos_opcoes = sorted(set(servicos_existentes).union(valores_servicos.keys()))
    servico = st.selectbox("Serviço", servicos_opcoes)
    valor_sugerido = obter_valor_servico(servico) or 0.0
    valor = st.number_input("Valor", value=float(valor_sugerido), step=1.0)

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
