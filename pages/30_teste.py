# pages/2F_DetalhesCliente.py
import streamlit as st
import pandas as pd
import plotly.express as px
from babel.dates import format_date
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials
import unicodedata
import requests  # <- checagem/baixa imagem com segurança

# --- Cloudinary (opcional, recomendado para fotos) ---
try:
    import cloudinary, cloudinary.api
    cloudinary.config(
        cloud_name=st.secrets["CLOUDINARY"]["cloud_name"],
        api_key=st.secrets["CLOUDINARY"]["api_key"],
        api_secret=st.secrets["CLOUDINARY"]["api_secret"],
    )
    CLOUD_OK = True
except Exception:
    CLOUD_OK = False

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
STATUS_ALVOS = ["clientes_status_feminino", "clientes status feminino"]

# Cloudinary / Logo padrão
PASTA_CLOUD = "Salao feminino"  # pasta nova no Cloudinary
LOGO_URL_DEFAULT = st.secrets.get(
    "LOGO_URL_DEFAULT",
    "https://via.placeholder.com/320x320.png?text=Sem+Foto"
)

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
    if tem_virg and tem_ponto:
        s = s.replace(".", "").replace(",", ".")  # PT-BR
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

# ---------- imagem segura ----------
def url_imagem_acessivel(url: str) -> bool:
    if not isinstance(url, str) or not url.startswith(("http://", "https://")):
        return False
    try:
        r = requests.head(url, timeout=6, allow_redirects=True)
        if r.status_code >= 400 or "text/html" in r.headers.get("content-type", ""):
            r = requests.get(url, timeout=8, stream=True)
        return r.status_code < 400 and "image" in r.headers.get("content-type", "")
    except Exception:
        return False

def mostrar_imagem_segura(url: str, caption: str, width: int = 220):
    if url_imagem_acessivel(url):
        try:
            r = requests.get(url, timeout=8)
            r.raise_for_status()
            st.image(r.content, width=width, caption=caption)  # bytes
            return
        except Exception:
            pass
    st.image(LOGO_URL_DEFAULT, width=width, caption=caption)

# ========================
# CONEXÃO
# ========================
@st.cache_resource
def conectar_sheets():
    info = st.secrets["GCP_SERVICE_ACCOUNT"]
    scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    cred = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(cred).open_by_key(SHEET_ID)

# ========================
# CARREGAR DADOS BASE (CSV por gid)
# ========================
@st.cache_data(ttl=300)
def carregar_base_feminino():
    sh = conectar_sheets()
    ws = find_worksheet(sh, [norm_ws(x) for x in BASE_ALVOS])
    gid = ws.id
    csv_url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={gid}"
    df = pd.read_csv(csv_url)

    df.columns = [c.strip() for c in df.columns]

    if "Data" not in df.columns: st.error("A aba feminina precisa ter 'Data'."); st.stop()
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce", dayfirst=True)
    df = df.dropna(subset=["Data"]).copy()

    if "Serviço" not in df.columns and "Servico" in df.columns:
        df.rename(columns={"Servico": "Serviço"}, inplace=True)

    if "Valor" not in df.columns: st.error("A aba feminina precisa ter 'Valor'."); st.stop()
    df["ValorNum"] = df["Valor"].apply(parse_valor)

    if "Conta" not in df.columns: df["Conta"] = "Indefinido"
    df["Conta"] = df["Conta"].fillna("Indefinido").astype(str).str.strip().str.title()

    if "Cliente" not in df.columns: st.error("A aba feminina precisa ter 'Cliente'."); st.stop()
    df["ClienteRaw"]   = df["Cliente"].astype(str)
    df["ClienteKey"]   = df["ClienteRaw"].str.strip().str.lower()
    df["ClienteLabel"] = df["ClienteRaw"].str.strip().str.title()

    ban = {"boliviano", "brasileiro", "menino", "menino boliviano"}
    df = df[~df["ClienteKey"].isin(ban)]

    return df

# ========================
# CARREGAR STATUS FEMININO (para campo Foto)
# ========================
@st.cache_data(ttl=300)
def carregar_status_feminino():
    sh = conectar_sheets()
    ws = find_worksheet(sh, [norm_ws(x) for x in STATUS_ALVOS])
    df = get_as_dataframe(ws).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    if "Cliente" not in df.columns or "Foto" not in df.columns:
        return pd.DataFrame(columns=["Cliente", "Foto", "ClienteKey"])
    df["Cliente"] = df["Cliente"].astype(str)
    df["Foto"] = df["Foto"].astype(str)
    df["ClienteKey"] = df["Cliente"].str.strip().str.lower()
    return df[["Cliente", "Foto", "ClienteKey"]]

# ========================
# URL DA FOTO (Cloudinary -> Planilha -> Logo)
# ========================
def foto_da_cliente(cliente_label: str, cliente_key: str, df_status: pd.DataFrame) -> str:
    # 1) Cloudinary
    if CLOUD_OK:
        public_id = cliente_label.strip().lower().replace(" ", "_")
        pid_path = f"{PASTA_CLOUD}/{public_id}"
        try:
            resp = cloudinary.api.resource(pid_path)
            url = resp.get("secure_url")
            if url:
                return url
        except Exception:
            pass
    # 2) Planilha
    try:
        row = df_status.loc[df_status["ClienteKey"] == cliente_key]
        if not row.empty:
            url = str(row.iloc[0]["Foto"]).strip()
            if url:
                if "drive.google.com" in url and "id=" in url:
                    file_id = url.split("id=")[-1].split("&")[0]
                    url = f"https://drive.google.com/uc?id={file_id}"
                return url
    except Exception:
        pass
    # 3) Logo
    return LOGO_URL_DEFAULT

# ========================
# EXECUÇÃO
# ========================
df = carregar_base_feminino()
df_status = carregar_status_feminino()

labels_por_key = (
    df.drop_duplicates("ClienteKey")[["ClienteKey", "ClienteLabel"]]
      .set_index("ClienteKey")["ClienteLabel"].to_dict()
)
opcoes_keys = sorted(labels_por_key.keys(), key=lambda k: labels_por_key[k])

pre = st.session_state.get("cliente")
pre_key = str(pre).strip().lower() if pre else None
if pre_key not in labels_por_key:
    pre_key = None

left, right = st.columns([2, 1])

with left:
    st.subheader("👤 Cliente")
    cliente_key = st.selectbox(
        "Cliente",
        options=opcoes_keys,
        index=(opcoes_keys.index(pre_key) if pre_key in opcoes_keys else 0) if opcoes_keys else None,
        format_func=lambda k: labels_por_key.get(k, k.title()),
    )
    cliente_label = labels_por_key.get(cliente_key, cliente_key.title())

with right:
    st.markdown("**Foto**")
    url_foto = foto_da_cliente(cliente_label, cliente_key, df_status)
    mostrar_imagem_segura(url_foto, cliente_label, width=220)

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
# RECEITA MENSAL
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
    hist.sort_values("Data", descending=True, inplace=True)
    hist["Mês"] = hist["Data"].apply(lambda x: format_date(x, "MMMM yyyy", locale="pt_BR").title())
    st.markdown("**Histórico de atendimentos (no filtro)**")
    st.dataframe(hist, use_container_width=True)
