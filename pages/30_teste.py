# 6F_Dashboard_Feminino.py
# Dashboard Feminino — conta atendimentos por Cliente+Data (combos não duplicam)

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime

st.set_page_config(page_title="Dashboard Feminino", page_icon="💅", layout="wide")
st.title("💅 Dashboard Feminino")

# =========================
# CONFIG
# =========================
SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"
ABAS_FEMININO_ALVOS = [
    "Base de Dados Feminino", "base de dados feminino",
    "Base de Dados - Feminino", "base de dados - feminino",
    "Base de Dados (Feminino)", "base de dados (feminino)",
    "Base de Dados Feminino "
]
PLOTLY_TEMPLATE = "plotly_dark"

MESES_PT = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
}

# =========================
# HELPERS
# =========================
def parse_valor(v):
    """Converte 'R$ 1.234,56' / '25,00' / numero -> float"""
    if pd.isna(v): 
        return 0.0
    if isinstance(v, (int, float)): 
        return float(v)
    s = str(v).strip().replace("R$", "").replace(" ", "")
    # remove milhares, troca decimal
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0

def parse_data(v):
    """Aceita dd/mm/aaaa, aaaa-mm-dd ou Timestamp; retorna Timestamp com data."""
    if isinstance(v, pd.Timestamp):
        return v.normalize()
    # tenta formatos comuns
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return pd.to_datetime(str(v), format=fmt, errors="raise").normalize()
        except Exception:
            pass
    return pd.to_datetime(v, errors="coerce").normalize()

# =========================
# CARREGAMENTO
# =========================
@st.cache_data(ttl=300)
def carregar_dados():
    # 1) tenta via Service Account
    try:
        import gspread
        from gspread_dataframe import get_as_dataframe
        from google.oauth2.service_account import Credentials

        info = (st.secrets.get("GCP_SERVICE_ACCOUNT")
                or st.secrets.get("gcp_service_account"))
        escopo = ["https://spreadsheets.google.com/feeds",
                  "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(info, scopes=escopo)
        cliente = gspread.authorize(creds)
        sh = cliente.open_by_key(SHEET_ID)

        # encontra a aba feminina
        ws = None
        titulos = [w.title for w in sh.worksheets()]
        for alvo in ABAS_FEMININO_ALVOS:
            if alvo in titulos:
                ws = sh.worksheet(alvo); break
        if ws is None:
            ws = sh.worksheet("Base de Dados Feminino")

        df = get_as_dataframe(ws).dropna(how="all")
    except Exception:
        # 2) fallback via CSV público (requer gid correto)
        GID = "400923272"
        url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}"
        df = pd.read_csv(url)

    # limpeza base
    df.columns = [str(c).strip() for c in df.columns]
    if "Data" not in df.columns:  # se não tiver, evita quebrar
        df["Data"] = np.nan
    if "Valor" not in df.columns:
        df["Valor"] = 0

    df["Data_dt"] = df["Data"].apply(parse_data)
    df["Ano"] = df["Data_dt"].dt.year
    df["MesNum"] = df["Data_dt"].dt.month
    df["Mes"] = df["MesNum"].map(MESES_PT)
    df["ValorNum"] = df["Valor"].apply(parse_valor)
    df["Cliente"] = df.get("Cliente", "").astype(str).str.strip()

    return df

df = carregar_dados()
df_valid = df[df["Data_dt"].notna()].copy()

# =========================
# SIDEBAR – FILTROS
# =========================
st.sidebar.header("Filtros")
anos = sorted(df_valid["Ano"].dropna().astype(int).unique().tolist())
ano_padrao = anos[-1] if anos else datetime.today().year
ano_sel = st.sidebar.selectbox("Ano", anos, index=(anos.index(ano_padrao) if anos else 0))

meses_disponiveis = [MESES_PT[m] for m in sorted(df_valid.loc[df_valid["Ano"] == ano_sel, "MesNum"].dropna().unique().tolist())]
meses_sel = st.sidebar.multiselect("Meses (opcional)", meses_disponiveis)

# aplica filtros
f = df_valid[df_valid["Ano"] == ano_sel].copy()
if meses_sel:
    meses_num_sel = [k for k, v in MESES_PT.items() if v in meses_sel]
    f = f[f["MesNum"].isin(meses_num_sel)]

# =========================
# MÉTRICAS (com atendimento por Cliente+Data)
# =========================
# atendimentos únicos por dia (combos/linhas -> 1)
atendimentos_unicos = (
    f.loc[f["Cliente"].ne(""), ["Cliente", "Data_dt"]]
      .dropna()
      .assign(DataDia=lambda x: x["Data_dt"].dt.date)
      .drop_duplicates(subset=["Cliente", "DataDia"])
)

total_atendimentos = len(atendimentos_unicos)
receita_total = float(f["ValorNum"].sum())
ticket_medio = (receita_total / total_atendimentos) if total_atendimentos > 0 else 0.0
clientes_ativos = f["Cliente"].replace("", np.nan).dropna().nunique()

c1, c2, c3, c4 = st.columns(4)
c1.metric("💰 Receita Total", f"R$ {receita_total:,.2f}".replace(",", "v").replace(".", ",").replace("v", "."))
c2.metric("🧾 Total de Atendimentos", f"{total_atendimentos}")
c3.metric("🎯 Ticket Médio", f"R$ {ticket_medio:,.2f}".replace(",", "v").replace(".", ",").replace("v", "."))
c4.metric("🟢 Clientes Ativos", f"{clientes_ativos}")

# =========================
# RECEITA MENSAL (ano selecionado)
# =========================
receita_mensal = (
    f.groupby("MesNum", as_index=False)["ValorNum"].sum()
     .assign(Mes=lambda d: d["MesNum"].map(MESES_PT))
     .sort_values("MesNum")
)

fig = px.bar(
    receita_mensal,
    x="Mes",
    y="ValorNum",
    text="ValorNum",
    labels={"Mes": "Mês", "ValorNum": "Receita (R$)"},
    template=PLOTLY_TEMPLATE
)
fig.update_traces(texttemplate="%{text:.0f}", textposition="outside")
fig.update_layout(title="📅 Receita Mensal (Ano selecionado)", yaxis_title="Receita (R$)")

st.plotly_chart(fig, use_container_width=True)

st.caption("ℹ️ **Regra**: *Total de Atendimentos* conta **Cliente + Data** (combos em várias linhas valem 1 atendimento no dia).")
