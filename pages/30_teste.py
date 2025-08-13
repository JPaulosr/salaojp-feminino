import streamlit as st
import pandas as pd
import plotly.express as px
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials
import requests
from PIL import Image
from io import BytesIO

st.set_page_config(layout="wide")
st.title("💅 Detalhes da Cliente (Feminino)")

# =========================
# CONFIGURAÇÃO GOOGLE SHEETS
# =========================
SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"
ABA_FEMININO = "Base de Dados Feminino"
GID_FEMININO = "400923272"  # gid da aba feminina (fallback CSV)

# ---------- Helpers ----------
def moeda_br(v):
    try:
        v = float(v)
    except Exception:
        v = 0.0
    return f"R$ {v:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")

@st.cache_data(ttl=300, show_spinner=True)
def carregar_dados():
    """
    1) Tenta via Service Account (st.secrets['gcp_service_account'])
    2) Se não houver segredo, faz fallback para CSV público da aba feminina
    """
    # ---------- Tentativa 1: Service Account ----------
    try:
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=[
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive",
            ],
        )
        client = gspread.authorize(creds)
        ws = client.open_by_key(SHEET_ID).worksheet(ABA_FEMININO)
        df = get_as_dataframe(ws, evaluate_formulas=False).dropna(how="all")
        fonte = "service_account"
    except Exception:
        # ---------- Tentativa 2: CSV público (precisa da planilha com permissão de leitura para 'qualquer pessoa com o link') ----------
        url_csv = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID_FEMININO}"
        df = pd.read_csv(url_csv)
        fonte = "csv"

    # Tipos básicos
    cols_minimas = ["Data", "Cliente", "Valor"]
    faltantes = [c for c in cols_minimas if c not in df.columns]
    if faltantes:
        st.error(f"A aba precisa ter as colunas: {', '.join(cols_minimas)}. Faltando: {', '.join(faltantes)}")
        st.stop()

    # Normalizações
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce", dayfirst=True)
    df = df.dropna(subset=["Data"]).copy()
    df["Ano"] = df["Data"].dt.year

    if "Conta" in df.columns:
        df["Conta"] = df["Conta"].astype(str).str.strip()
    else:
        df["Conta"] = "Indefinido"

    df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce").fillna(0.0)

    return df, fonte

df, fonte = carregar_dados()
if fonte == "csv":
    st.caption("ℹ️ Lendo pelo CSV público (segredo de Service Account não encontrado).")

# =========================
# SELEÇÃO DE CLIENTE
# =========================
clientes = sorted(df["Cliente"].dropna().astype(str).unique())
cliente_sel = st.selectbox("👩 Cliente", clientes)

# Mostrar foto ABAIXO do nome (se tiver coluna Foto)
if "Foto" in df.columns:
    foto_url = df.loc[df["Cliente"] == cliente_sel, "Foto"].dropna().astype(str).unique()
    if len(foto_url) > 0:
        try:
            resp = requests.get(foto_url[0], timeout=8)
            resp.raise_for_status()
            img = Image.open(BytesIO(resp.content))
            st.image(img, caption=cliente_sel, width=240)
        except Exception:
            st.info("Não foi possível carregar a foto. (Verifique o link/permite público)")
    else:
        st.info("Nenhuma foto cadastrada para esta cliente.")

# =========================
# FILTRO POR ANO
# =========================
anos = sorted(df["Ano"].dropna().unique())
colA, colB = st.columns([2, 1])
with colA:
    ano_sel = st.selectbox("📅 Selecionar Ano", ["Todos"] + [int(a) for a in anos], index=0)
with colB:
    comparar_anos = st.checkbox(
        "Comparar anos",
        value=False,
        help="Mostra todos os anos juntos no gráfico, agrupados por mês."
    )

# Aplica filtro de cliente e, se NÃO estiver comparando anos, também filtra o ano escolhido
df_filtrado = df[df["Cliente"] == cliente_sel].copy()
if not comparar_anos and ano_sel != "Todos":
    df_filtrado = df_filtrado[df_filtrado["Ano"] == ano_sel]

# =========================
# FILTRO POR FORMA DE PAGAMENTO
# =========================
formas_pag = sorted(df_filtrado["Conta"].dropna().unique())
forma_sel = st.multiselect("💳 Filtrar por forma de pagamento", options=formas_pag, default=formas_pag)
if forma_sel:
    df_filtrado = df_filtrado[df_filtrado["Conta"].isin(forma_sel)]

# =========================
# MÉTRICAS
# =========================
receita_total = float(df_filtrado["Valor"].sum())
# visitas = nº de dias distintos com atendimento
visitas = int(df_filtrado["Data"].dt.date.nunique()) if not df_filtrado.empty else 0
ticket_medio = float(
    df_filtrado.groupby(df_filtrado["Data"].dt.date)["Valor"].sum().mean() or 0.0
) if not df_filtrado.empty else 0.0
fiado_total = float(
    df_filtrado.loc[df_filtrado["Conta"].str.lower() == "fiado", "Valor"].sum()
) if "Conta" in df_filtrado.columns else 0.0

c1, c2, c3, c4 = st.columns(4)
c1.metric("💰 Receita total (filtro)", moeda_br(receita_total))
c2.metric("📅 Visitas (dias distintos)", visitas)
c3.metric("🧾 Tíquete médio", moeda_br(ticket_medio))
c4.metric("📌 Fiado no filtro", moeda_br(fiado_total))

# =========================
# GRÁFICO MENSAL
# =========================
if df_filtrado.empty:
    st.info("Sem registros para os filtros atuais.")
else:
    df_filtrado["MesNum"] = df_filtrado["Data"].dt.month
    meses_pt = {1:"Jan",2:"Fev",3:"Mar",4:"Abr",5:"Mai",6:"Jun",7:"Jul",8:"Ago",9:"Set",10:"Out",11:"Nov",12:"Dez"}
    df_filtrado["Mes"] = df_filtrado["MesNum"].map(meses_pt)

    if comparar_anos:
        base = (
            df_filtrado
            .groupby(["Ano", "MesNum", "Mes"], as_index=False)["Valor"].sum()
            .sort_values(["Ano", "MesNum"])
        )
        fig = px.bar(
            base,
            x="Mes", y="Valor",
            color="Ano", barmode="group",
            text=base["Valor"].apply(lambda v: moeda_br(v).replace(",00","")),
            labels={"Valor":"Receita (R$)", "Mes":"Mês", "Ano":"Ano"},
            title=f"📊 Receita mensal — {cliente_sel} (comparativo de anos)",
            template="plotly_dark",
            category_orders={"Mes": [meses_pt[m] for m in range(1,13)]},
        )
    else:
        base = (
            df_filtrado
            .groupby(["MesNum", "Mes"], as_index=False)["Valor"].sum()
            .sort_values("MesNum")
        )
        subtitulo = f" — {ano_sel}" if ano_sel != "Todos" else ""
        fig = px.bar(
            base,
            x="Mes", y="Valor",
            text=base["Valor"].apply(lambda v: moeda_br(v).replace(",00","")),
            labels={"Valor":"Receita (R$)", "Mes":"Mês"},
            title=f"📊 Receita mensal — {cliente_sel}{subtitulo}",
            template="plotly_dark",
            category_orders={"Mes": [meses_pt[m] for m in range(1,13)]},
        )

    fig.update_traces(textposition="outside", cliponaxis=False)
    fig.update_layout(showlegend=comparar_anos)
    st.plotly_chart(fig, use_container_width=True)

# =========================
# TABELA DETALHES
# =========================
with st.expander("🔎 Detalhes dos atendimentos (no filtro)"):
    tabela = df_filtrado[["Data","Conta","Valor"]].copy()
    tabela["Valor"] = tabela["Valor"].apply(moeda_br)
    tabela.sort_values("Data", ascending=False, inplace=True)
    st.dataframe(tabela, use_container_width=True)
