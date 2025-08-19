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
BASE_ABA   = "Base de Dados Feminino"      # << usa a aba feminina
STATUS_ABA = "clientes_status_feminino"    # << e o status feminino

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
    base.columns   = [str(c).strip() for c in base.columns]
    status.columns = [str(c).strip() for c in status.columns]
    return base, status, planilha

def parse_data(df: pd.DataFrame, col="Data") -> pd.DataFrame:
    if col not in df.columns:
        return df.iloc[0:0]
    df = df.copy()
    df[col] = pd.to_datetime(df[col].astype(str).str.strip(), errors="coerce", dayfirst=True)
    return df.dropna(subset=[col])

def escolher_coluna_foto(status_cols):
    foto_opcoes = ["Foto", "link_foto", "foto", "imagem", "url_foto", "foto_link", "link", "image"]
    for cand in foto_opcoes:
        for sc in status_cols:
            if sc.strip().lower() == cand.lower():
                return sc
    return "Foto"

def montar_novos_df(novos_clientes, status_df):
    status_cols = list(status_df.columns) if len(status_df.columns) else ["Cliente", "Status", "Foto"]
    if "Cliente" not in status_cols:
        status_cols = ["Cliente"] + status_cols
    col_cliente = "Cliente"
    col_status  = "Status" if "Status" in status_cols else None
    col_foto    = escolher_coluna_foto(status_cols)
    # campos comuns que vi no seu print: "Observação"
    col_obs     = "Observação" if "Observação" in status_cols else None

    base_rows = {c: [] for c in status_cols}
    for nome in novos_clientes:
        for c in status_cols:
            if c == col_cliente:
                base_rows[c].append(nome)
            elif c == col_status:
                base_rows[c].append("Ativo")
            elif c == col_foto:
                base_rows[c].append("")
            elif c == col_obs:
                base_rows[c].append("")
            else:
                base_rows[c].append("")
    novos_df = pd.DataFrame(base_rows)
    if col_cliente in novos_df.columns:
        novos_df = novos_df.sort_values(by=col_cliente, key=lambda s: s.astype(str).str.casefold()).reset_index(drop=True)
    return novos_df

def norm(s: str) -> str:
    return str(s).strip().casefold()

# === Carregar e preparar ===
base_df, status_df, planilha = carregar_bases()

if "Cliente" not in base_df.columns:
    st.error(f"A aba '{BASE_ABA}' não possui a coluna 'Cliente'.")
    st.stop()

# Recorte por data
base_df = parse_data(base_df, "Data")
base_df = base_df[base_df["Data"] >= DATA_INICIO]

# Conjuntos normalizados
clientes_base_raw = base_df["Cliente"].dropna().astype(str).str.strip()
clientes_status_raw = status_df["Cliente"].dropna().astype(str).str.strip() if "Cliente" in status_df.columns else pd.Series([], dtype=str)

clientes_status_norm = {norm(x) for x in clientes_status_raw}
novos_clientes = sorted([x for x in clientes_base_raw.unique() if norm(x) not in clientes_status_norm],
                        key=lambda s: s.casefold())

st.markdown(f"### 👥 Clientes novos (Feminino) desde **{DATA_INICIO_STR}**: `{len(novos_clientes)}`")

if novos_clientes:
    st.dataframe(pd.DataFrame({"Cliente": novos_clientes}), use_container_width=True)
    if st.button("✅ Adicionar ao clientes_status_feminino"):
        aba_status = planilha.worksheet(STATUS_ABA)
        novos_df = montar_novos_df(novos_clientes, status_df)
        status_atualizado = pd.concat([status_df, novos_df], ignore_index=True)
        set_with_dataframe(aba_status, status_atualizado)
        st.success(f"{len(novos_clientes)} novos clientes adicionados com sucesso!")
else:
    st.success("Nenhum cliente novo (feminino) para adicionar no recorte informado. ✅")
