import streamlit as st
import pandas as pd
import plotly.express as px
import gspread
from google.oauth2.service_account import Credentials

st.set_page_config(layout="wide", page_title="💅 Dashboard Feminino", page_icon="💅")
st.title("💅 Dashboard Feminino")

# =========================
# CONFIG
# =========================
SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"
ABA_FEM_BASE = "Base de Dados Feminino"
ABA_STATUS_FEM = "clientes_status_feminino"

@st.cache_resource
def conectar_sheets():
    info = st.secrets.get("GCP_SERVICE_ACCOUNT") or st.secrets.get("gcp_service_account")
    if not info:
        st.error("Secrets ausentes. Adicione a seção [GCP_SERVICE_ACCOUNT] no Streamlit Cloud.")
        st.stop()
    escopo = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=escopo)
    return gspread.authorize(creds).open_by_key(SHEET_ID)

def _coerce_valor(series):
    # se já for numérico (por UNFORMATTED_VALUE), mantenha
    if pd.api.types.is_numeric_dtype(series):
        return series.fillna(0).astype(float)
    # senão, tenta normalizar textos como "R$ 25,00"
    s = (series.astype(str)
                .str.replace("R$", "", regex=False)
                .str.replace(" ", "", regex=False)
                .str.replace(".", "", regex=False)   # milhar
                .str.replace(",", ".", regex=False)) # decimal
    return pd.to_numeric(s, errors="coerce").fillna(0.0)

@st.cache_data(ttl=300)
def carregar_base_feminina() -> pd.DataFrame:
    ss = conectar_sheets()
    ws = ss.worksheet(ABA_FEM_BASE)

    # Lê sem formatação para evitar "R$" e vírgula
    rows = ws.get_all_values(value_render_option="UNFORMATTED_VALUE")
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows[1:], columns=rows[0]).dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]

    # Data
    if "Data" in df.columns:
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce", dayfirst=True)
        df = df.dropna(subset=["Data"])
        df["Ano"]  = df["Data"].dt.year
        df["Mês"]  = df["Data"].dt.month
        df["Ano-Mês"] = df["Data"].dt.to_period("M").astype(str)

    # Valor numérico
    if "Valor" in df.columns:
        df["ValorNum"] = _coerce_valor(df["Valor"])
    else:
        df["ValorNum"] = 0.0

    return df

df = carregar_base_feminina()
if df.empty:
    st.warning("Sem dados na aba **Base de Dados Feminino**.")
    st.stop()

# =========================
# FILTROS
# =========================
st.sidebar.header("🎛️ Filtros")
anos = sorted(df["Ano"].dropna().unique(), reverse=True)
ano = st.sidebar.selectbox("🗓️ Ano", anos)

meses_pt = {1:"Janeiro",2:"Fevereiro",3:"Março",4:"Abril",5:"Maio",6:"Junho",7:"Julho",8:"Agosto",9:"Setembro",10:"Outubro",11:"Novembro",12:"Dezembro"}
meses_disp = sorted(df[df["Ano"]==ano]["Mês"].dropna().unique())
mes_labels = [meses_pt[m] for m in meses_disp]
meses_sel = st.sidebar.multiselect("📆 Meses (opcional)", mes_labels, default=mes_labels)

if meses_sel:
    meses_num = [k for k,v in meses_pt.items() if v in meses_sel]
    df = df[(df["Ano"]==ano) & (df["Mês"].isin(meses_num))]
else:
    df = df[df["Ano"]==ano]

# =========================
# Excluir FIADO apenas na receita
# =========================
col_conta = next((c for c in df.columns if c.lower() in ["conta","forma de pagamento","pagamento","status"]), None)
mask_fiado = df[col_conta].astype(str).str.strip().str.lower().eq("fiado") if col_conta else pd.Series(False, index=df.index)
df_receita = df[~mask_fiado].copy()

# =========================
# INDICADORES
# =========================
receita_total = float(df_receita["ValorNum"].sum())
total_atend   = len(df)

data_limite = pd.to_datetime("2025-05-11")
antes  = df[df["Data"] < data_limite]
depois = df[df["Data"] >= data_limite].drop_duplicates(subset=["Cliente","Data"])
clientes_unicos = pd.concat([antes, depois])["Cliente"].nunique()
ticket = receita_total/total_atend if total_atend else 0

c1,c2,c3,c4 = st.columns(4)
c1.metric("💰 Receita Total", f"R$ {receita_total:,.2f}".replace(",", "v").replace(".", ",").replace("v","."))
c2.metric("📅 Total de Atendimentos", total_atend)
c3.metric("🎯 Ticket Médio", f"R$ {ticket:,.2f}".replace(",", "v").replace(".", ",").replace("v","."))
c4.metric("🟢 Clientes Ativos", clientes_unicos)

# =========================
# NOVO BLOCO: Receita Mensal do ano selecionado
# =========================
st.markdown("### 📆 Receita Mensal (Ano selecionado)")
if not df_receita.empty:
    mens = (df_receita.groupby("Mês")["ValorNum"]
                      .sum()
                      .reindex(range(1,13), fill_value=0)
                      .reset_index())
    mens["MêsNome"] = mens["Mês"].map(meses_pt)

    fig_mensal = px.bar(mens, x="MêsNome", y="ValorNum", text_auto=True,
                        labels={"ValorNum":"Receita (R$)", "MêsNome":"Mês"})
    fig_mensal.update_layout(height=420, showlegend=False)
    st.plotly_chart(fig_mensal, use_container_width=True)

    mens["Receita (R$)"] = mens["ValorNum"].apply(lambda x: f"R$ {x:,.2f}".replace(",", "v").replace(".", ",").replace("v","."))
    st.dataframe(mens[["MêsNome","Receita (R$)"]], use_container_width=True)
else:
    st.info("Sem receita para o período filtrado.")

# =========================
# Receita por Funcionário
# =========================
st.markdown("### 📊 Receita por Funcionário")
if "Funcionário" in df_receita.columns:
    df_func = df_receita.groupby("Funcionário")["ValorNum"].sum().reset_index().rename(columns={"ValorNum":"Valor"})
    fig = px.bar(df_func, x="Funcionário", y="Valor", text_auto=True)
    fig.update_layout(height=400, yaxis_title="Receita (R$)", showlegend=False)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("A coluna **Funcionário** não existe na base.")

# =========================
# Receita por Tipo
# =========================
st.markdown("### 🧾 Receita por Tipo")
df_tipo = df_receita.copy()
if "Serviço" in df_tipo.columns:
    df_tipo["Tipo"] = df_tipo["Serviço"].apply(
        lambda x: "Combo" if "combo" in str(x).lower()
        else "Produto" if any(k in str(x).lower() for k in ["gel","produto"])
        else "Serviço"
    )
    df_pizza = df_tipo.groupby("Tipo")["ValorNum"].sum().reset_index().rename(columns={"ValorNum":"Valor"})
    fig2 = px.pie(df_pizza, values="Valor", names="Tipo", title="Distribuição de Receita")
    fig2.update_traces(textinfo="percent+label")
    st.plotly_chart(fig2, use_container_width=True)

# =========================
# Top 10 Clientes
# =========================
st.markdown("### 🥇 Top 10 Clientes (Feminino)")
nomes_excluir = ["boliviano","brasileiro","menino"]
cnt = df.groupby("Cliente")["Serviço"].count().rename("Qtd_Serviços") if "Serviço" in df.columns else pd.Series(dtype=int)
val = df_receita.groupby("Cliente")["ValorNum"].sum().rename("Valor") if "ValorNum" in df_receita.columns else pd.Series(dtype=float)
df_top = pd.concat([cnt,val], axis=1).reset_index().fillna(0)
df_top = df_top[~df_top["Cliente"].str.lower().isin(nomes_excluir)]
df_top = df_top.sort_values("Valor", ascending=False).head(10)
df_top["Valor Formatado"] = df_top["Valor"].apply(lambda x: f"R$ {x:,.2f}".replace(",", "v").replace(".", ",").replace("v","."))
st.dataframe(df_top[["Cliente","Qtd_Serviços","Valor Formatado"]], use_container_width=True)
