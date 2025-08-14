# pages/30_teste.py

import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials

st.set_page_config(layout="wide")
st.title("🔧 Teste — Leitura da Planilha")

# ---- CONFIG ----
SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"
ABA = "Base de Dados"  # troque se quiser testar outra aba
GID = "0"              # gid da aba (só para fallback CSV)

@st.cache_data(ttl=300, show_spinner=True)
def carregar_dados(sheet_id: str, aba: str, gid: str):
    """
    1) Tenta via Service Account (aceita GCP_SERVICE_ACCOUNT ou gcp_service_account)
    2) Fallback via CSV público
    3) Normaliza colunas básicas
    """
    # Tentativa: service account
    try:
        sa_info = st.secrets.get("GCP_SERVICE_ACCOUNT") or st.secrets.get("gcp_service_account")
        if not sa_info:
            raise KeyError("segredo não encontrado")
        creds = Credentials.from_service_account_info(
            sa_info,
            scopes=[
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive",
            ],
        )
        client = gspread.authorize(creds)
        ws = client.open_by_key(sheet_id).worksheet(aba)
        df = get_as_dataframe(ws, evaluate_formulas=False).dropna(how="all")
        fonte = "service_account"
    except Exception:
        url_csv = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
        df = pd.read_csv(url_csv)
        fonte = "csv"

    # normalização mínima
    df.columns = [str(c).strip() for c in df.columns]
    if "Data" in df.columns:
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce", dayfirst=True)
    if "Valor" in df.columns:
        df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce").fillna(0.0)

    return df, fonte

def main():
    df, fonte = carregar_dados(SHEET_ID, ABA, GID)
    st.caption(f"Fonte: **{fonte}** — linhas: {len(df)} • colunas: {len(df.columns)}")
    st.dataframe(df.head(50), use_container_width=True)

if __name__ == "__main__":
    main()
