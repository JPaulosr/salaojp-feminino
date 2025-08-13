# pages/2F_DetalhesCliente.py
import streamlit as st
import pandas as pd
import plotly.express as px
from babel.dates import format_date

st.set_page_config(layout="wide")
st.title("💅 Detalhes da Cliente (Feminino)")

# ========================
# CONFIG DA PLANILHA
# ========================
SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"
ABA_FEMININO = "Base de Dados Feminino"

# ========================
# UTILS
# ========================
def parse_valor(v):
    """Converte 'R$ 1.234,56', '25,00', '25.0', '25.00' ou número em float."""
    if pd.isna(v):
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace("\u00A0", "")
    s = s.replace("R$", "").replace("r$", "").replace(" ", "")
    tem_virg = "," in s
    tem_ponto = "." in s
    if tem_virg and tem_ponto:
        # PT-BR (milhar '.' e decimal ',')
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

# ========================
# CARREGA DADOS (somente leitura, rápido)
# ========================
@st.cache_data(ttl=300)
def carregar_dados():
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={ABA_FEMININO}"
    df = pd.read_csv(url)
    # Normaliza colunas esperadas
    df.columns = [c.strip() for c in df.columns]
    if "Data" not in df.columns or "Cliente" not in df.columns or "Valor" not in df.columns:
        st.error("A aba feminina precisa ter as colunas: Data, Cliente, Valor (e idealmente Conta/Serviço).")
        st.stop()
    # Datas
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce", dayfirst=True)
    df = df.dropna(subset=["Data"]).copy()
    # Valor numérico
    df["ValorNum"] = df["Valor"].apply(parse_valor)
    # Chaves/labels para cliente (evita duplicatas no select)
    df["ClienteRaw"] = df["Cliente"].astype(str)
    df["ClienteKey"] = df["ClienteRaw"].str.strip().str.lower()
    df["ClienteLabel"] = df["ClienteRaw"].str.strip().str.title()
    # Conta (forma de pagamento)
    if "Conta" not in df.columns:
        df["Conta"] = "Indefinido"
    df["Conta"] = df["Conta"].fillna("Indefinido").astype(str).str.strip().str.title()
    # Serviço
    if "Serviço" not in df.columns and "Servico" in df.columns:
        df.rename(columns={"Servico": "Serviço"}, inplace=True)
    return df

df = carregar_dados()

# ========================
# SELEÇÃO DE CLIENTE (sem duplicatas)
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
# FILTRO: FORMA DE PAGAMENTO (inclui FIADO)
# ========================
st.subheader("💳 Forma de pagamento")
formas = sorted(dados_cli_all["Conta"].dropna().unique().tolist())
# Seleciona todas por padrão
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
# RECEITA MENSAL (meses em PT-BR e ordem cronológica)
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
    # Label em PT-BR (ex.: março 2024 -> "Março 2024")
    mensal["MesAno"] = mensal["YM"].dt.to_timestamp().apply(
        lambda x: format_date(x, "MMMM yyyy", locale="pt_BR").title()
    )

    fig = px.bar(
        mensal,
        x="MesAno",
        y="Receita",
        text=mensal["Receita"].apply(lambda v: moeda(v).replace(",00", "")),
        labels={"Receita": "Receita (R$)", "MesAno": "Mês"},
        template="plotly_dark",
        title=f"📅 Receita mensal — {cliente_label}",
        height=380,
    )
    fig.update_traces(textposition="outside", cliponaxis=False, hovertemplate="%{x}<br>%{y}")
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
