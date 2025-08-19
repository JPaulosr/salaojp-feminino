
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
from datetime import datetime

st.set_page_config(page_title="🔄 Sincronizar Clientes (Feminino)", layout="wide")
st.title("🔄 Sincronizar Clientes (Feminino)")

# === CONFIG GOOGLE SHEETS ===
SHEET_ID   = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"
BASE_ABA   = "Base de Dados"
STATUS_ABA = "clientes_status"

# === REGRA FEMININO ===
COLUNA_TIPO        = "Tipo"          # quando existir, filtra por Tipo == "Feminino"
VALOR_TIPO_FEM     = "Feminino"
COLUNA_FUNCIONARIO = "Funcionário"    # fallback quando 'Tipo' não existir
FUNCIONARIOS_FEM   = ["Daniela"]      # ajuste aqui se houver mais profissionais femininos

# === RECORTE DE DATA: a partir de 01/08/2025 (inclusive) ===
DATA_INICIO_STR = "01/08/2025"        # dd/mm/YYYY
DATA_FMT        = "%d/%m/%Y"
DATA_INICIO     = datetime.strptime(DATA_INICIO_STR, DATA_FMT)

@st.cache_resource
def conectar_sheets():
    info = st.secrets["GCP_SERVICE_ACCOUNT"]
    escopo = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credenciais = Credentials.from_service_account_info(info, scopes=escopo)
    cliente = gspread.authorize(credenciais)
    return cliente.open_by_key(SHEET_ID)

def carregar_bases():
    planilha = conectar_sheets()
    base   = get_as_dataframe(planilha.worksheet(BASE_ABA)).dropna(how="all")
    status = get_as_dataframe(planilha.worksheet(STATUS_ABA)).dropna(how="all")

    # Normaliza nomes das colunas (tira espaços extras)
    base.columns   = [str(c).strip() for c in base.columns]
    status.columns = [str(c).strip() for c in status.columns]
    return base, status, planilha

def parse_data_coluna(df: pd.DataFrame, col="Data") -> pd.DataFrame:
    if col not in df.columns:
        return df.iloc[0:0]  # sem coluna Data: retorna vazio para evitar erro
    # tenta dd/mm/YYYY e variantes
    df = df.copy()
    df[col] = pd.to_datetime(df[col].astype(str).str.strip(), errors="coerce", dayfirst=True)
    return df.dropna(subset=[col])

def filtrar_feminino(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # 1) Se existe a coluna Tipo, usa Tipo == Feminino
    if COLUNA_TIPO in df.columns:
        fem = df[df[COLUNA_TIPO].astype(str).str.strip().str.casefold() == VALOR_TIPO_FEM.casefold()]
        return fem
    # 2) Caso não exista, filtra por Funcionário ∈ FUNCIONARIOS_FEM (caso exista)
    if COLUNA_FUNCIONARIO in df.columns:
        fem = df[df[COLUNA_FUNCIONARIO].astype(str).isin(FUNCIONARIOS_FEM)]
        return fem
    # 3) Se não tem nenhuma das colunas, não há como identificar "feminino"
    return df.iloc[0:0]

def escolher_coluna_foto(status_cols):
    # Detecta a coluna de foto existente para manter o padrão do seu clientes_status
    foto_opcoes = ["link_foto", "foto", "imagem", "url_foto", "foto_link", "link", "image", "Foto"]
    for c in foto_opcoes:
        for sc in status_cols:
            if sc.strip().lower() == c.lower():
                return sc
    # Caso não haja nenhuma, criaremos "Foto"
    return "Foto"

def montar_novos_df(novos_clientes, status_df):
    # Garante que os novos registros tenham as MESMAS colunas do clientes_status
    status_cols = list(status_df.columns)
    if "Cliente" not in status_cols:
        status_cols = ["Cliente"] + status_cols

    # Define nomes de colunas-chave
    col_cliente = "Cliente"
    col_status  = "Status" if "Status" in status_cols else None
    col_foto    = escolher_coluna_foto(status_cols)
    col_familia = "Família" if "Família" in status_cols else None

    # Inicializa base vazia com as colunas do status
    base_rows = {c: [] for c in status_cols}

    for nome in novos_clientes:
        for c in status_cols:
            if c == col_cliente:
                base_rows[c].append(nome)
            elif c == col_status:
                base_rows[c].append("Ativo")
            elif c == col_foto:
                base_rows[c].append("")
            elif c == col_familia:
                base_rows[c].append("")
            else:
                # qualquer outra coluna extra do clientes_status recebe vazio
                base_rows[c].append("")

    novos_df = pd.DataFrame(base_rows)
    # Ordena pelo nome do cliente (opcional)
    if col_cliente in novos_df.columns:
        novos_df = novos_df.sort_values(by=col_cliente, key=lambda s: s.astype(str).str.casefold()).reset_index(drop=True)
    return novos_df

# === Carregar e preparar ===
base_df, status_df, planilha = carregar_bases()

# Garantir colunas essenciais
if "Cliente" not in base_df.columns:
    st.error("A aba 'Base de Dados' não possui a coluna 'Cliente'.")
    st.stop()

# Filtra FEMININO
base_df = filtrar_feminino(base_df)

# Filtra por DATA >= 01/08/2025
base_df = parse_data_coluna(base_df, "Data")
base_df = base_df[base_df["Data"] >= DATA_INICIO]

# Extrai conjuntos de clientes (normalizando minimamente)
clientes_base = set(base_df["Cliente"].dropna().astype(str).str.strip())
clientes_status = set(status_df["Cliente"].dropna().astype(str).str.strip()) if "Cliente" in status_df.columns else set()

novos_clientes = sorted(list(clientes_base - clientes_status), key=lambda s: s.casefold())

st.markdown(
    f"### 👥 Clientes novos (Feminino) desde **{DATA_INICIO_STR}**: "
    f"`{len(novos_clientes)}`"
)

if novos_clientes:
    novos_df_preview = pd.DataFrame({
        "Cliente": novos_clientes
    })
    st.dataframe(novos_df_preview, use_container_width=True)

    if st.button("✅ Adicionar ao clientes_status"):
        aba_status = planilha.worksheet(STATUS_ABA)

        # Monta DF com as mesmas colunas do clientes_status
        novos_df = montar_novos_df(novos_clientes, status_df)

        # Concatena e grava
        status_atualizado = pd.concat([status_df, novos_df], ignore_index=True)
        set_with_dataframe(aba_status, status_atualizado)

        st.success(f"{len(novos_clientes)} novos clientes (feminino) adicionados com sucesso!")
else:
    st.success("Nenhum cliente novo (feminino) para adicionar no recorte informado. ✅")
