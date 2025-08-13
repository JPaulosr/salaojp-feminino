import streamlit as st
import pandas as pd
import plotly.express as px
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials

st.set_page_config(layout="wide", page_title="💅 Dashboard Feminino", page_icon="💅")
st.title("💅 Dashboard Feminino")

# =========================
# GOOGLE SHEETS
# =========================
SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"
ABA_FEM_BASE = "Base de Dados Feminino"
ABA_STATUS_FEM = "clientes_status_feminino"

@st.cache_resource
def conectar_sheets():
    # tenta duas chaves de secrets para evitar KeyError
    info = st.secrets.get("GCP_SERVICE_ACCOUNT") or st.secrets.get("gcp_service_account")
    if not info:
        st.error(
            "As credenciais não foram encontradas em `st.secrets`.\n\n"
            "Adicione nas **Secrets** do Streamlit Cloud:\n\n"
            "[GCP_SERVICE_ACCOUNT]\n"
            'type = "service_account"\n'
            "private_key_id = \"...\"\n"
            "private_key = \"\"\"-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\"\"\"\n"
            "client_email = \"...\"\n"
            "client_id = \"...\"\n"
        )
        st.stop()
    escopo = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    credenciais = Credentials.from_service_account_info(info, scopes=escopo)
    cliente = gspread.authorize(credenciais)
    return cliente.open_by_key(SHEET_ID)

def _limpar_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]

    # Valor -> número (remove R$, espaços e converte vírgula para ponto)
    if "Valor" in df.columns:
        df["ValorNum"] = (
            df["Valor"]
            .astype(str)
            .str.replace("R$", "", regex=False)
            .str.replace(".", "", regex=False)      # separador de milhar
            .str.replace(",", ".", regex=False)     # decimal BR -> ponto
            .str.strip()
        )
        df["ValorNum"] = pd.to_numeric(df["ValorNum"], errors="coerce").fillna(0)

    if "Data" in df.columns:
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce", dayfirst=True)
        df = df.dropna(subset=["Data"])
        df["Ano"] = df["Data"].dt.year
        df["Mês"] = df["Data"].dt.month
        df["Ano-Mês"] = df["Data"].dt.to_period("M").astype(str)
    return df

@st.cache_data(ttl=300)
def carregar_base_feminina() -> pd.DataFrame:
    ss = conectar_sheets()
    ws = ss.worksheet(ABA_FEM_BASE)
    df = get_as_dataframe(ws)
    return _limpar_df(df)

@st.cache_data(ttl=300)
def carregar_status_feminino() -> pd.DataFrame:
    ss = conectar_sheets()
    ws = ss.worksheet(ABA_STATUS_FEM)
    df = get_as_dataframe(ws).dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]
    if "Cliente" not in df.columns:
        possiveis = [c for c in df.columns if c.lower() in ["nome", "nome do cliente"]]
        df["Cliente"] = df[possiveis[0]] if possiveis else ""
    for col in ["Status", "Imagem", "Observação"]:
        if col not in df.columns:
            df[col] = ""
    df["Cliente_norm"] = df["Cliente"].astype(str).str.strip()
    return df

df = carregar_base_feminina()
if df.empty:
    st.warning("Sem dados na aba **Base de Dados Feminino**.")
    st.stop()

status_fem = carregar_status_feminino()

# =========================
# FILTROS
# =========================
st.sidebar.header("🎛️ Filtros")
anos = sorted(df["Ano"].dropna().unique(), reverse=True)
ano = st.sidebar.selectbox("🗓️ Ano", anos)

meses_pt = {
    1:"Janeiro",2:"Fevereiro",3:"Março",4:"Abril",5:"Maio",6:"Junho",
    7:"Julho",8:"Agosto",9:"Setembro",10:"Outubro",11:"Novembro",12:"Dezembro"
}
meses_disp = sorted(df[df["Ano"] == ano]["Mês"].dropna().unique())
mes_labels = [meses_pt[m] for m in meses_disp]
meses_sel = st.sidebar.multiselect("📆 Meses (opcional)", mes_labels, default=mes_labels)
if meses_sel:
    meses_num = [k for k,v in meses_pt.items() if v in meses_sel]
    df = df[(df["Ano"] == ano) & (df["Mês"].isin(meses_num))]
else:
    df = df[df["Ano"] == ano]

# =========================
# Excluir FIADO só na receita
# =========================
col_conta = next((c for c in df.columns if c.lower() in ["conta","forma de pagamento","pagamento","status"]), None)
mask_fiado = df[col_conta].astype(str).str.strip().str.lower().eq("fiado") if col_conta else pd.Series(False, index=df.index)
df_receita = df[~mask_fiado].copy()

# =========================
# INDICADORES
# =========================
receita_total = float(df_receita["ValorNum"].sum()) if "ValorNum" in df_receita.columns else 0.0
total_atend = len(df)

data_limite = pd.to_datetime("2025-05-11")
antes = df[df["Data"] < data_limite]
depois = df[df["Data"] >= data_limite].drop_duplicates(subset=["Cliente","Data"])
clientes_unicos = pd.concat([antes, depois])["Cliente"].nunique()
ticket = receita_total/total_atend if total_atend else 0

c1,c2,c3,c4 = st.columns(4)
c1.metric("💰 Receita Total", f"R$ {receita_total:,.2f}".replace(",", "v").replace(".", ",").replace("v","."))
c2.metric("📅 Total de Atendimentos", total_atend)
c3.metric("🎯 Ticket Médio", f"R$ {ticket:,.2f}".replace(",", "v").replace(".", ",").replace("v","."))
c4.metric("🟢 Clientes Ativos", clientes_unicos)

# =========================
# GRÁFICOS
# =========================
st.markdown("### 📊 Receita por Funcionário")
if "Funcionário" in df_receita.columns:
    df_func = df_receita.groupby("Funcionário")["ValorNum"].sum().reset_index().rename(columns={"ValorNum":"Valor"})
    fig = px.bar(df_func, x="Funcionário", y="Valor", text_auto=True)
    fig.update_layout(height=400, yaxis_title="Receita (R$)", showlegend=False)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("A coluna **Funcionário** não existe na base.")

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

st.markdown("### 🥇 Top 10 Clientes (Feminino)")
nomes_excluir = ["boliviano","brasileiro","menino"]
cnt = df.groupby("Cliente")["Serviço"].count().rename("Qtd_Serviços") if "Serviço" in df.columns else pd.Series(dtype=int)
val = df_receita.groupby("Cliente")["ValorNum"].sum().rename("Valor") if "ValorNum" in df_receita.columns else pd.Series(dtype=float)
df_top = pd.concat([cnt,val], axis=1).reset_index().fillna(0)
df_top = df_top[~df_top["Cliente"].str.lower().isin(nomes_excluir)]
df_top = df_top.sort_values("Valor", ascending=False).head(10)
df_top["Valor Formatado"] = df_top["Valor"].apply(lambda x: f"R$ {x:,.2f}".replace(",", "v").replace(".", ",").replace("v","."))
st.dataframe(df_top[["Cliente","Qtd_Serviços","Valor Formatado"]], use_container_width=True)

st.markdown("---")

# =========================
# SINCRONIZAÇÃO FEMININO -> clientes_status_feminino
# =========================
st.subheader("🔁 Sincronização de clientes (Feminino → clientes_status_feminino)")

def montar_df_upsert(df_base: pd.DataFrame, df_status: pd.DataFrame) -> pd.DataFrame:
    if "Cliente" not in df_base.columns:
        return pd.DataFrame(columns=df_status.columns)
    clientes = (
        df_base["Cliente"].dropna().astype(str).str.strip().unique().tolist()
    )
    existentes = set(df_status["Cliente_norm"].dropna().astype(str).tolist())
    novos = [c for c in clientes if c and c not in existentes]
    if not novos:
        return pd.DataFrame(columns=df_status.columns)

    cols = list(df_status.columns)
    for col in ["Cliente","Cliente_norm","Status","Imagem","Observação"]:
        if col not in cols: cols.append(col)

    linhas = []
    for c in novos:
        linha = {k:"" for k in cols}
        linha["Cliente"] = c
        linha["Cliente_norm"] = c
        linha["Status"] = "Ativo"
        linhas.append(linha)
    return pd.DataFrame(linhas)[cols]

if st.button("🚀 Sincronizar clientes (Feminino → clientes_status_feminino)"):
    try:
        base = carregar_base_feminina()
        status_atual = carregar_status_feminino()
        novos = montar_df_upsert(base, status_atual)
        if novos.empty:
            st.success("Nenhum cliente novo para adicionar.")
        else:
            ss = conectar_sheets()
            ws = ss.worksheet(ABA_STATUS_FEM)
            status_limpo = status_atual.drop(columns=["Cliente_norm"], errors="ignore")
            novos_limpo = novos.drop(columns=["Cliente_norm"], errors="ignore")
            col_final = list(status_limpo.columns)
            df_final = pd.concat([status_limpo, novos_limpo.reindex(columns=col_final)], ignore_index=True)
            ws.clear()
            set_with_dataframe(ws, df_final, include_index=False, include_column_header=True, resize=True)
            st.success(f"Clientes adicionados: {len(novos)}")
    except Exception as e:
        st.error(f"Falha na sincronização: {e}")

st.caption("Base: **Base de Dados Feminino** • Status: **clientes_status_feminino**")
