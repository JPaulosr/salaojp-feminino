# pages/2F_DetalhesCliente.py
import streamlit as st
import pandas as pd
import plotly.express as px
from babel.dates import format_date
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials
import unicodedata
import requests
from PIL import Image
from io import BytesIO

st.set_page_config(layout="wide")
st.title("💅 Detalhes da Cliente (Feminino)")

# ========================
# CONFIG DA PLANILHA
# ========================
SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"
BASE_ALVOS = [
    "base de dados feminino", "base de dados - feminino",
    "base de dados (feminino)", "base de dados feminino "
]

# ========================
# UTILS
# ========================
def parse_valor(v):
    """Converte 'R$ 1.234,56', '25,00', '25.0', '25.00' ou número em float."""
    if pd.isna(v): return 0.0
    if isinstance(v, (int, float)): return float(v)
    s = str(v).strip().replace("\u00A0", "")
    s = s.replace("R$", "").replace("r$", "").replace(" ", "")
    tem_virg = "," in s
    tem_ponto = "." in s
    if tem_virg and tem_ponto:  # PT-BR
        s = s.replace(".", "").replace(",", ".")
    elif tem_virg and not tem_ponto:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        x = pd.to_numeric(s, errors="coerce")
        return float(x) if pd.notna(x) else 0.0

def moeda(v):  # formatação BR
    return f"R$ {float(v):,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")

def norm_ws(s: str) -> str:
    if s is None: return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    return " ".join(s.lower().strip().split())

def achar_col(df, nomes):
    alvo = [n.strip().lower() for n in nomes]
    for c in df.columns:
        if c.strip().lower() in alvo:
            return c
    return None

def find_worksheet(sh, alvos_norm):
    """Encontra a worksheet pela lista de nomes possíveis (normalizados)."""
    wss = sh.worksheets()
    titulos = [ws.title for ws in wss]
    tnorms  = [norm_ws(t) for t in titulos]
    for ws, t in zip(wss, tnorms):
        if t in alvos_norm:  # match exato
            return ws
    for ws, t in zip(wss, tnorms):
        if any(a in t for a in alvos_norm):  # contém
            return ws
    st.error("❌ Não encontrei a aba feminina. Guias disponíveis:\n- " + "\n- ".join(titulos))
    st.stop()

# ========================
# CONEXÃO (para descobrir o gid com segurança)
# ========================
@st.cache_resource
def conectar_sheets():
    info = st.secrets["GCP_SERVICE_ACCOUNT"]
    scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    cred = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(cred).open_by_key(SHEET_ID)

# ========================
# CARREGAR DADOS (rápido e só-leitura via export CSV por gid)
# ========================
@st.cache_data(ttl=300)
def carregar_dados():
    sh = conectar_sheets()
    ws = find_worksheet(sh, [norm_ws(x) for x in BASE_ALVOS])
    gid = ws.id  # <-- id numérico da aba (gid)
    csv_url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={gid}"
    df = pd.read_csv(csv_url)

    # Normaliza colunas
    df.columns = [c.strip() for c in df.columns]
    # Datas
    if "Data" not in df.columns:
        st.error("A aba feminina precisa ter a coluna 'Data'."); st.stop()
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce", dayfirst=True)
    df = df.dropna(subset=["Data"]).copy()

    # Serviço
    if "Serviço" not in df.columns and "Servico" in df.columns:
        df.rename(columns={"Servico": "Serviço"}, inplace=True)
    # Valor numérico
    if "Valor" not in df.columns:
        st.error("A aba feminina precisa ter a coluna 'Valor'."); st.stop()
    df["ValorNum"] = df["Valor"].apply(parse_valor)

    # Conta (forma de pagamento)
    if "Conta" not in df.columns:
        df["Conta"] = "Indefinido"
    df["Conta"] = df["Conta"].fillna("Indefinido").astype(str).str.strip().str.title()

    # Cliente
    if "Cliente" not in df.columns:
        st.error("A aba feminina precisa ter a coluna 'Cliente'."); st.stop()
    df["ClienteRaw"]   = df["Cliente"].astype(str)
    df["ClienteKey"]   = df["ClienteRaw"].str.strip().str.lower()
    df["ClienteLabel"] = df["ClienteRaw"].str.strip().str.title()

    # Remove nomes genéricos (se houver)
    ban = {"boliviano", "brasileiro", "menino", "menino boliviano"}
    df = df[~df["ClienteKey"].isin(ban)]

    return df

df = carregar_dados()

# ========================
# SELECT DE CLIENTE (sem duplicatas) + FOTO ABAIXO DO NOME
# ========================
labels_por_key = (
    df.drop_duplicates("ClienteKey")[["ClienteKey", "ClienteLabel"]]
      .set_index("ClienteKey")["ClienteLabel"].to_dict()
)
opcoes_keys = sorted(labels_por_key.keys(), key=lambda k: labels_por_key[k])

pre = st.session_state.get("cliente")
pre_key = str(pre).strip().lower() if pre else None  # corrigido (.lower(), não .str.lower())
if pre_key not in labels_por_key:
    pre_key = None

st.subheader("👤 Cliente")
cliente_key = st.selectbox(
    "Cliente",
    options=opcoes_keys,
    index=(opcoes_keys.index(pre_key) if (pre_key and pre_key in opcoes_keys) else 0) if opcoes_keys else None,
    format_func=lambda k: labels_por_key.get(k, k.title()),
)

cliente_label = labels_por_key.get(cliente_key, cliente_key.title())

# >>> FOTO logo abaixo do nome
if "Foto" in df.columns:
    foto_urls = (
        df.loc[df["ClienteKey"] == cliente_key, "Foto"]
          .dropna().astype(str).unique().tolist()
    )
    if foto_urls:
        try:
            resp = requests.get(foto_urls[0], timeout=8)
            resp.raise_for_status()
            img = Image.open(BytesIO(resp.content))
            st.image(img, caption=cliente_label, width=240)
        except Exception:
            st.info("Não foi possível carregar a foto. Verifique o link/permissões.")
    else:
        st.info("Nenhuma foto cadastrada para esta cliente.")
# <<< FOTO

dados_cli_all = df[df["ClienteKey"] == cliente_key].copy()

# ========================
# FILTRO: FORMA DE PAGAMENTO
# ========================
st.subheader("💳 Forma de pagamento")
formas = sorted(dados_cli_all["Conta"].dropna().unique().tolist())
formas_sel = st.multiselect("Filtrar por forma de pagamento", options=formas, default=formas)
dados_cli = dados_cli_all[dados_cli_all["Conta"].isin(formas_sel)].copy()

# ========================
# MÉTRICAS
# ========================
col1, col2, col3, col4 = st.columns(4)
total = float(dados_cli["ValorNum"].sum())
visitas = int(dados_cli["Data"].dt.date.nunique())
ticket_medio = dados_cli.groupby(dados_cli["Data"].dt.date)["ValorNum"].sum().mean()
ticket_medio = 0.0 if pd.isna(ticket_medio) else float(ticket_medio)
fiado_total = float(dados_cli[dados_cli["Conta"].str.lower()=="fiado"]["ValorNum"].sum())

col1.metric("💰 Receita total (filtro)", moeda(total))
col2.metric("🗓️ Visitas (dias distintos)", visitas)
col3.metric("🧾 Tíquete médio", moeda(ticket_medio))
col4.metric("📌 Fiado no filtro", moeda(fiado_total))

# ========================
# RECEITA MENSAL (pt-BR em ordem cronológica)
# ========================
if dados_cli.empty:
    st.info("Sem registros para esta combinação de cliente + forma de pagamento.")
else:
    mensal = (
        dados_cli
        .assign(YM=dados_cli["Data"].dt.to_period("M"))
        .groupby("YM", as_index=False)["ValorNum"].sum()
        .rename(columns={"ValorNum": "Receita"})
        .sort_values("YM")
    )
    mensal["MesAno"] = mensal["YM"].dt.to_timestamp().apply(
        lambda x: format_date(x, "MMMM yyyy", locale="pt_BR").title()
    )

    fig = px.bar(
        mensal,
        x="MesAno", y="Receita",
        text=mensal["Receita"].apply(lambda v: moeda(v).replace(",00", "")),
        labels={"Receita": "Receita (R$)", "MesAno": "Mês"},
        template="plotly_dark",
        title=f"📅 Receita mensal — {cliente_label}",
        height=380,
    )
    fig.update_traces(textposition="outside", cliponaxis=False)
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

    # ========================
    # SERVIÇOS
    # ========================
    if "Serviço" in dados_cli.columns:
        serv = (
            dados_cli.groupby("Serviço")["ValorNum"].sum()
            .reset_index().sort_values("ValorNum", ascending=False)
        )
        serv["Valor"] = serv["ValorNum"].apply(moeda)
        st.markdown("**Serviços realizados (no filtro)**")
        st.dataframe(serv[["Serviço", "Valor"]], use_container_width=True)

    # ========================
    # DETALHES
    # ========================
    cols = ["Data", "Serviço", "Conta", "ValorNum"] if "Serviço" in dados_cli.columns else ["Data", "Conta", "ValorNum"]
    hist = dados_cli[cols].copy().rename(columns={"ValorNum": "Valor"})
    hist["Valor"] = hist["Valor"].apply(moeda)
    hist.sort_values("Data", ascending=False, inplace=True)
    hist["Mês"] = hist["Data"].apply(lambda x: format_date(x, "MMMM yyyy", locale="pt_BR").title())
    st.markdown("**Histórico de atendimentos (no filtro)**")
    st.dataframe(hist, use_container_width=True)
