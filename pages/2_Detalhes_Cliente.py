# pages/2F_DetalhesCliente.py
import streamlit as st
import pandas as pd
import plotly.express as px
from babel.dates import format_date
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials
import unicodedata

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
STATUS_ALVOS = [
    "clientes_status_feminino", "clientes status feminino",
    "clientes_status feminino", "status_feminino"
]

# Logo padrão quando não houver foto válida
FOTO_PADRAO = "https://res.cloudinary.com/db8ipmete/image/upload/v1752463905/Logo_sal%C3%A3o_kz9y9c.png"

# ========================
# UTILS
# ========================
def parse_valor(v):
    if pd.isna(v): return 0.0
    if isinstance(v, (int, float)): return float(v)
    s = str(v).strip().replace("\u00A0", "")
    s = s.replace("R$", "").replace("r$", "").replace(" ", "")
    tem_virg = "," in s
    tem_ponto = "." in s
    if tem_virg and tem_ponto:
        s = s.replace(".", "").replace(",", ".")
    elif tem_virg and not tem_ponto:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        x = pd.to_numeric(s, errors="coerce")
        return float(x) if pd.notna(x) else 0.0

def moeda(v):
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
# CONEXÃO (usa secret correto, com fallback)
# ========================
@st.cache_resource
def conectar_sheets():
    # Usa a mesma chave das outras páginas: GCP_SERVICE_ACCOUNT
    info = st.secrets.get("GCP_SERVICE_ACCOUNT") or st.secrets.get("gcp_service_account")
    if not info:
        st.error("Credenciais não encontradas em st.secrets['GCP_SERVICE_ACCOUNT'].")
        st.stop()
    scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    cred = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(cred).open_by_key(SHEET_ID)

# ========================
# CARREGAR DADOS (via export CSV por gid para ser rápido)
# ========================
@st.cache_data(ttl=300)
def carregar_dados():
    sh = conectar_sheets()
    ws = find_worksheet(sh, [norm_ws(x) for x in BASE_ALVOS])
    gid = ws.id
    csv_url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={gid}"
    df = pd.read_csv(csv_url)

    df.columns = [c.strip() for c in df.columns]

    if "Data" not in df.columns:
        st.error("A aba feminina precisa ter a coluna 'Data'."); st.stop()
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce", dayfirst=True)
    df = df.dropna(subset=["Data"]).copy()
    df["Ano"] = df["Data"].dt.year.astype(int)
    df["MesNum"] = df["Data"].dt.month
    df["MesNome"] = df["Data"].dt.to_period("M").dt.to_timestamp().apply(
        lambda x: format_date(x, "MMMM", locale="pt_BR").title()
    )

    if "Serviço" not in df.columns and "Servico" in df.columns:
        df.rename(columns={"Servico": "Serviço"}, inplace=True)

    if "Valor" not in df.columns:
        st.error("A aba feminina precisa ter a coluna 'Valor'."); st.stop()
    df["ValorNum"] = df["Valor"].apply(parse_valor)

    if "Conta" not in df.columns:
        df["Conta"] = "Indefinido"
    df["Conta"] = df["Conta"].fillna("Indefinido").astype(str).str.strip().str.title()

    if "Cliente" not in df.columns:
        st.error("A aba feminina precisa ter a coluna 'Cliente'."); st.stop()
    df["ClienteRaw"]   = df["Cliente"].astype(str)
    df["ClienteKey"]   = df["ClienteRaw"].str.strip().str.lower()
    df["ClienteLabel"] = df["ClienteRaw"].str.strip().str.title()

    # Foto na própria base (opcional)
    possiveis_col_foto = ["Foto", "Imagem", "Link Imagem", "Link", "URL", "Foto URL", "Imagem URL", "Foto_Url"]
    col_foto_base = achar_col(df, possiveis_col_foto)

    # Remove nomes genéricos
    ban = {"boliviano", "brasileiro", "menino", "menino boliviano"}
    df = df[~df["ClienteKey"].isin(ban)]

    return df, col_foto_base

@st.cache_data(ttl=300)
def carregar_status():
    """Tenta carregar uma aba de status com coluna de foto."""
    try:
        sh = conectar_sheets()
        ws = find_worksheet(sh, [norm_ws(x) for x in STATUS_ALVOS])
        df = get_as_dataframe(ws).dropna(how="all")
        df.columns = [c.strip() for c in df.columns]
        if "Cliente" not in df.columns:
            return pd.DataFrame()
        df["ClienteKey"] = df["Cliente"].astype(str).str.strip().str.lower()
        possiveis_col_foto = ["Foto", "Imagem", "Link Imagem", "Link", "URL", "Foto URL", "Imagem URL", "Foto_Url"]
        col_foto = achar_col(df, possiveis_col_foto)
        if not col_foto:
            return pd.DataFrame()
        df["FotoURL"] = df[col_foto].astype(str).str.strip()
        return df[["ClienteKey", "FotoURL"]].dropna()
    except Exception:
        return pd.DataFrame()

df, col_foto_base = carregar_dados()
df_status = carregar_status()

# ========================
# SELECT DE CLIENTE
# ========================
labels_por_key = (
    df.drop_duplicates("ClienteKey")[["ClienteKey", "ClienteLabel"]]
      .set_index("ClienteKey")["ClienteLabel"].to_dict()
)
opcoes_keys = sorted(labels_por_key.keys(), key=lambda k: labels_por_key[k])

pre = st.session_state.get("cliente")
pre_key = str(pre).strip().lower() if pre else None
if pre_key not in labels_por_key:
    pre_key = None

st.subheader("👤 Cliente")
cliente_key = st.selectbox(
    "Cliente",
    options=opcoes_keys,
    index=(opcoes_keys.index(pre_key) if pre_key in opcoes_keys else 0) if opcoes_keys else None,
    format_func=lambda k: labels_por_key.get(k, k.title()),
)
cliente_label = labels_por_key.get(cliente_key, cliente_key.title())
dados_cli_all = df[df["ClienteKey"] == cliente_key].copy()

# ========================
# FOTO DA CLIENTE
# ========================
def url_valida(u: str) -> bool:
    if not u: return False
    s = str(u).strip()
    if s.lower() in {"nan", "none", "null", "0"}: return False
    return s.lower().startswith(("http://", "https://"))

foto_url = None
if not df_status.empty:
    mapa_foto = dict(zip(df_status["ClienteKey"], df_status["FotoURL"]))
    foto_url = mapa_foto.get(cliente_key)

if (not url_valida(foto_url)) and col_foto_base:
    serie = dados_cli_all[col_foto_base].dropna().astype(str).str.strip()
    if len(serie):
        candidato = serie.iloc[0]
        if url_valida(candidato):
            foto_url = candidato

if not url_valida(foto_url):
    foto_url = FOTO_PADRAO

st.image(foto_url, caption=cliente_label, use_container_width=False, width=220)

# ========================
# FILTRO: ANO e MÊS (mantém ano atual por padrão)
# ========================
st.subheader("📅 Filtros de período")
anos_disp = sorted(dados_cli_all["Ano"].unique().tolist())
ano_atual = pd.Timestamp.today().year
if ano_atual not in anos_disp:
    anos_disp.append(ano_atual)
    anos_disp = sorted(anos_disp)

if "ano_cli_sel" not in st.session_state:
    st.session_state["ano_cli_sel"] = ano_atual if ano_atual in anos_disp else anos_disp[-1]

ano_sel = st.selectbox("Ano", ["Todos"] + anos_disp, index=(["Todos"] + anos_disp).index(st.session_state["ano_cli_sel"]))
st.session_state["ano_cli_sel"] = ano_sel

if ano_sel == "Todos":
    base_periodo = dados_cli_all.copy()
else:
    base_periodo = dados_cli_all[dados_cli_all["Ano"] == ano_sel].copy()

meses_ordem = (base_periodo
               .drop_duplicates(["MesNum", "MesNome"])
               .sort_values("MesNum"))
mes_opcoes = ["Todos"] + meses_ordem["MesNome"].tolist()

if "mes_cli_sel" not in st.session_state or st.session_state["mes_cli_sel"] not in mes_opcoes:
    st.session_state["mes_cli_sel"] = "Todos"

mes_sel = st.selectbox("Mês", mes_opcoes, index=mes_opcoes.index(st.session_state["mes_cli_sel"]))
st.session_state["mes_cli_sel"] = mes_sel

dados_cli = base_periodo.copy()
if mes_sel != "Todos":
    dados_cli = dados_cli[dados_cli["MesNome"] == mes_sel]

# ========================
# MÉTRICAS
# ========================
col1, col2, col3, col4 = st.columns(4)
total = float(dados_cli["ValorNum"].sum())
visitas = int(dados_cli["Data"].dt.date.nunique())
ticket_medio = dados_cli.groupby(dados_cli["Data"].dt.date)["ValorNum"].sum().mean()
ticket_medio = 0.0 if pd.isna(ticket_medio) else float(ticket_medio)
fiado_total = float(dados_cli[dados_cli["Conta"].str.lower()=="fiado"]["ValorNum"].sum())

col1.metric("💰 Receita total (período)", moeda(total))
col2.metric("🗓️ Visitas (dias distintos)", visitas)
col3.metric("🧾 Tíquete médio", moeda(ticket_medio))
col4.metric("📌 Fiado no período", moeda(fiado_total))

# ========================
# RECEITA MENSAL
# ========================
if dados_cli.empty:
    st.info("Sem registros para esta combinação de cliente + período.")
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
    # SERVIÇOS (no período)
    # ========================
    if "Serviço" in dados_cli.columns:
        serv = (
            dados_cli.groupby("Serviço")["ValorNum"].sum()
            .reset_index().sort_values("ValorNum", ascending=False)
        )
        serv["Valor"] = serv["ValorNum"].apply(moeda)
        st.markdown("**Serviços realizados (no período)**")
        st.dataframe(serv[["Serviço", "Valor"]], use_container_width=True)

    # ========================
# DETALHES (no período) — Data em dd/mm/aaaa (FIX duplicatas)
# ========================
cols = ["Data", "Serviço", "Conta", "ValorNum"] if "Serviço" in dados_cli.columns else ["Data", "Conta", "ValorNum"]
hist = dados_cli[cols].copy().rename(columns={"ValorNum": "Valor"})

# Converte e guarda datetime em coluna auxiliar
hist["_DataDT"] = pd.to_datetime(hist["Data"], errors="coerce")

# Coluna Mês (pt-BR)
hist["Mês"] = hist["_DataDT"].apply(lambda x: format_date(x, "MMMM yyyy", locale="pt_BR").title())

# Sobrescreve a própria 'Data' com formato brasileiro (sem criar outra coluna)
hist["Data"] = hist["_DataDT"].dt.strftime("%d/%m/%Y")

# Formata valor
hist["Valor"] = hist["Valor"].apply(moeda)

# Ordena pelo datetime real e remove a auxiliar
hist.sort_values("_DataDT", ascending=False, inplace=True)
hist.drop(columns=["_DataDT"], inplace=True)

# Garante a ordem e só exibe colunas que existem (evita KeyError se 'Serviço' não existir)
ordem_desejada = ["Data", "Serviço", "Conta", "Valor", "Mês"]
exibir_cols = [c for c in ordem_desejada if c in hist.columns]

st.markdown("**Histórico de atendimentos (no período)**")
st.dataframe(hist[exibir_cols].reset_index(drop=True), use_container_width=True)
