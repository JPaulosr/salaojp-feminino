# pages/2F_DetalhesCliente.py
import streamlit as st
import pandas as pd
import plotly.express as px
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials
import unicodedata

st.set_page_config(layout="wide")
st.title("👩 Detalhes da Cliente (Feminino)")

SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"

BASE_ALVOS = [
    "base de dados feminino", "base de dados - feminino",
    "base de dados (feminino)", "base de dados feminino "
]
STATUS_ALVOS = [
    "clientes_status_feminino", "clientes status feminino",
    "clientes_status feminino", "status_feminino"
]

# ---------------- Utils ----------------
def norm_ws(s: str) -> str:
    if s is None: return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    return " ".join(s.lower().strip().split())

def parse_valor_qualquer(v):
    if pd.isna(v): return 0.0
    if isinstance(v, (int, float)): return float(v)
    s = str(v).strip().replace("\u00A0", "")
    s = s.replace("R$", "").replace("r$", "").replace(" ", "")
    tem_virg, tem_ponto = ("," in s), ("." in s)
    if tem_virg and tem_ponto:
        s = s.replace(".", "").replace(",", ".")
    elif tem_virg and not tem_ponto:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        x = pd.to_numeric(s, errors="coerce")
        return float(x) if pd.notna(x) else 0.0

def achar_col(df, nomes):
    alvo = [n.strip().lower() for n in nomes]
    for c in df.columns:
        if c.strip().lower() in alvo:
            return c
    return None

def find_worksheet(planilha, alvos_norm):
    wss = planilha.worksheets()
    titulos = [ws.title for ws in wss]
    tnorms  = [norm_ws(t) for t in titulos]
    for ws, t in zip(wss, tnorms):
        if t in alvos_norm:
            return ws
    for ws, t in zip(wss, tnorms):
        if any(a in t for a in alvos_norm):
            return ws
    st.error("❌ Não encontrei a aba feminina. Guias disponíveis:\n- " + "\n- ".join(titulos))
    st.stop()

@st.cache_resource
def conectar_sheets():
    info = st.secrets["GCP_SERVICE_ACCOUNT"]
    scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    cred = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(cred).open_by_key(SHEET_ID)

@st.cache_data
def carregar_base_feminino():
    sh = conectar_sheets()
    ws = find_worksheet(sh, [norm_ws(x) for x in BASE_ALVOS])
    df = get_as_dataframe(ws).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]

    if "Data" not in df.columns:
        st.error("❌ Coluna 'Data' não encontrada na aba feminina."); st.stop()

    # Normaliza campos chave
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce", dayfirst=True)
    df = df.dropna(subset=["Data"])

    col_serv = achar_col(df, ["Serviço", "Servico"])
    if col_serv and col_serv != "Serviço": df.rename(columns={col_serv: "Serviço"}, inplace=True)
    col_valor = achar_col(df, ["Valor"])
    if col_valor and col_valor != "Valor": df.rename(columns={col_valor: "Valor"}, inplace=True)
    col_conta = achar_col(df, ["Conta", "Forma de pagamento", "Pagamento", "Status"])
    if col_conta and col_conta != "Conta": df.rename(columns={col_conta: "Conta"}, inplace=True)
    col_cli = achar_col(df, ["Cliente"])
    if col_cli and col_cli != "Cliente": df.rename(columns={col_cli: "Cliente"}, inplace=True)

    # Parser de moeda
    df["ValorNum"] = df["Valor"].apply(parse_valor_qualquer)

    # Chaves para evitar duplicados no selectbox
    df["ClienteRaw"]   = df["Cliente"].astype(str)
    df["ClienteKey"]   = df["ClienteRaw"].str.strip().str.lower()       # chave única
    df["ClienteLabel"] = df["ClienteRaw"].str.strip().str.title()       # exibição

    # Remover nomes genéricos (se existirem)
    ban = {"boliviano", "brasileiro", "menino", "menino boliviano"}
    df = df[~df["ClienteKey"].isin(ban)]

    return df

@st.cache_data
def carregar_status_feminino():
    try:
        sh = conectar_sheets()
        ws = find_worksheet(sh, [norm_ws(x) for x in STATUS_ALVOS])
        df = get_as_dataframe(ws).dropna(how="all")
        df.columns = [c.strip() for c in df.columns]
        c_cli = achar_col(df, ["Cliente"]); c_sta = achar_col(df, ["Status"])
        if not c_cli or not c_sta:
            return pd.DataFrame(columns=["Cliente","Status"])
        out = df[[c_cli, c_sta]].copy()
        out.columns = ["Cliente","Status"]
        out["Cliente"] = out["Cliente"].astype(str).str.strip()
        out["Status"]  = out["Status"].astype(str).str.strip()
        # Criar chave para *join* seguro
        out["ClienteKey"] = out["Cliente"].str.strip().str.lower()
        return out
    except Exception:
        return pd.DataFrame(columns=["Cliente","Status","ClienteKey"])

# ---------- Dados ----------
df = carregar_base_feminino()
df_status = carregar_status_feminino()

# ---------- Lista ÚNICA de clientes ----------
# Mapa chave -> label (primeira ocorrência)
label_por_key = (
    df.drop_duplicates("ClienteKey")[["ClienteKey", "ClienteLabel"]]
      .set_index("ClienteKey")["ClienteLabel"]
      .to_dict()
)

# Ordenar por label
opcoes_keys = sorted(label_por_key.keys(), key=lambda k: label_por_key[k])

# Pré-seleção vinda da página anterior
pre = st.session_state.get("cliente")
pre_key = None
if pre:
    pre_key = str(pre).strip().lower()
    if pre_key not in label_por_key:
        pre_key = None

# Selectbox usando a KEY como valor e LABEL como exibição
st.subheader("👤 Cliente")
cliente_key = st.selectbox(
    "Cliente",
    options=opcoes_keys,
    index=(opcoes_keys.index(pre_key) if pre_key in opcoes_keys else 0) if opcoes_keys else None,
    format_func=lambda k: label_por_key.get(k, k.title()),
    placeholder="Selecione a cliente",
)

# Filtra os dados pela chave única
dados_cli = df[df["ClienteKey"] == cliente_key].copy().sort_values("Data")
cliente_label = label_por_key.get(cliente_key, cliente_key.title())

# ---------- Indicadores ----------
total = float(dados_cli["ValorNum"].sum())
visitas = int(dados_cli["Data"].dt.date.nunique())
ticket_medio = dados_cli.groupby(dados_cli["Data"].dt.date)["ValorNum"].sum().mean()
ticket_medio = 0.0 if pd.isna(ticket_medio) else float(ticket_medio)

col1, col2, col3 = st.columns(3)
col1.metric("💰 Receita total", f"R$ {total:,.2f}".replace(",", "v").replace(".", ",").replace("v","."))
col2.metric("🗓️ Visitas (dias distintos)", visitas)
col3.metric("🧾 Tíquete médio", f"R$ {ticket_medio:,.2f}".replace(",", "v").replace(".", ",").replace("v","."))

# ---------- Gráfico mensal ----------
mensal = (dados_cli.resample("M", on="Data")["ValorNum"].sum()
          .reset_index().rename(columns={"ValorNum":"Receita"}))
mensal["Mês"] = mensal["Data"].dt.strftime("%b/%Y")

fig = px.bar(
    mensal, x="Mês", y="Receita",
    text=mensal["Receita"].apply(lambda x: f"R$ {x:,.0f}".replace(",", "v").replace(".", ",").replace("v",".")),
    labels={"Receita": "Receita (R$)"},
    template="plotly_dark", height=380, title=f"Receita mensal — {cliente_label}"
)
fig.update_traces(textposition="outside", cliponaxis=False)
fig.update_layout(showlegend=False)
st.plotly_chart(fig, use_container_width=True)

# ---------- Serviços ----------
if "Serviço" in dados_cli.columns:
    tb = (dados_cli.groupby("Serviço")["ValorNum"].sum()
          .reset_index().sort_values("ValorNum", ascending=False))
    tb["Valor"] = tb["ValorNum"].apply(lambda x: f"R$ {x:,.2f}".replace(",", "v").replace(".", ",").replace("v","."))
    st.markdown("**Serviços realizados**")
    st.dataframe(tb[["Serviço","Valor"]], use_container_width=True)

# ---------- Histórico detalhado ----------
cols = ["Data","Serviço","Conta","ValorNum"] if "Serviço" in dados_cli.columns else ["Data","Conta","ValorNum"]
hist = dados_cli[cols].copy().rename(columns={"ValorNum":"Valor"})
hist["Valor"] = hist["Valor"].apply(lambda x: f"R$ {x:,.2f}".replace(",", "v").replace(".", ",").replace("v","."))
hist.sort_values("Data", ascending=False, inplace=True)
st.markdown("**Histórico**")
st.dataframe(hist, use_container_width=True)

# ---------- Status atual ----------
if not df_status.empty:
    status_atual = df_status.loc[df_status["ClienteKey"] == cliente_key, "Status"]
    if not status_atual.empty:
        st.info(f"📌 Status atual no cadastro: **{status_atual.iloc[0]}**")
