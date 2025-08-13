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

# =========================
# CONEXÃO
# =========================
@st.cache_resource
def conectar_sheets():
    info = st.secrets.get("GCP_SERVICE_ACCOUNT") or st.secrets.get("gcp_service_account")
    if not info:
        st.error("Secrets ausentes. Adicione [GCP_SERVICE_ACCOUNT] nos Secrets do Streamlit.")
        st.stop()
    escopo = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=escopo)
    return gspread.authorize(creds).open_by_key(SHEET_ID)

# =========================
# HELPERS
# =========================
def _strip2d(rows):
    out = []
    for r in rows:
        out.append([("" if c is None else str(c).strip()) for c in r])
    return out

def _descobrir_cabecalho(rows, col_alvos=("Data","Serviço","Valor")):
    """
    Acha a linha que contém as colunas esperadas (ex.: Data, Serviço, Valor).
    Retorna (idx_cab, header_list) ou (0, primeira_linha) como fallback.
    """
    for i, r in enumerate(rows[:10]):  # procura nos 10 primeiros
        if not r: 
            continue
        cabe = [c.strip() for c in r]
        achou = sum(1 for alvo in col_alvos if alvo in cabe)
        if achou >= 2:
            return i, cabe
    return 0, [c.strip() for c in rows[0]]

def _coerce_valor(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return series.fillna(0).astype(float)
    s = (series.astype(str)
               .str.replace("R$", "", regex=False)
               .str.replace(" ", "", regex=False)
               .str.replace(".", "", regex=False)   # milhar
               .str.replace(",", ".", regex=False)) # decimal
    return pd.to_numeric(s, errors="coerce").fillna(0.0)

def _parse_data_sheets(col: pd.Series) -> pd.Series:
    """
    Converte datas como:
      - 'dd/mm/aaaa' (texto)
      - número serial do Sheets (dias desde 1899-12-30)
    """
    s = col.copy()
    dt_txt = pd.to_datetime(s, errors="coerce", dayfirst=True, infer_datetime_format=True)
    s_num = pd.to_numeric(s, errors="coerce")
    dt_num = pd.to_datetime(s_num, unit="D", origin="1899-12-30")
    dt = dt_txt.combine_first(dt_num)
    return dt

@st.cache_data(ttl=300)
def carregar_base_feminina() -> pd.DataFrame:
    ss = conectar_sheets()
    ws = ss.worksheet(ABA_FEM_BASE)

    # Lê tudo cru (mantém seriais de data)
    rows_raw = ws.get_all_values(value_render_option="UNFORMATTED_VALUE")
    rows = _strip2d(rows_raw)

    if not rows or not rows[0]:
        return pd.DataFrame()

    # Descobre a linha de cabeçalho real
    i_head, header = _descobrir_cabecalho(rows)
    corpo = rows[i_head+1:] if i_head+1 < len(rows) else []

    # Remove linhas totalmente vazias
    corpo = [r for r in corpo if any(c != "" for c in r)]
    if not corpo:
        return pd.DataFrame(columns=header)

    # Ajusta largura das linhas ao tamanho do cabeçalho
    width = len(header)
    corpo = [ (r + [""]*max(0, width-len(r)))[:width] for r in corpo ]

    df = pd.DataFrame(corpo, columns=header)
    df.columns = [str(c).strip() for c in df.columns]

    # Valor numérico
    if "Valor" in df.columns:
        df["ValorNum"] = _coerce_valor(df["Valor"])
    else:
        df["ValorNum"] = 0.0

    # Data (não descarta ainda — vamos contar quantas são válidas)
    if "Data" in df.columns:
        df["DataParsed"] = _parse_data_sheets(df["Data"])
        # cria Ano/Mês só onde houver Data válida
        df["Ano"] = pd.to_datetime(df["DataParsed"], errors="coerce").dt.year
        df["Mês"] = pd.to_datetime(df["DataParsed"], errors="coerce").dt.month
    else:
        df["DataParsed"] = pd.NaT
        df["Ano"] = pd.NA
        df["Mês"] = pd.NA

    # Uniformiza nome das colunas comuns
    col_func = [c for c in df.columns if c.lower() in ["funcionário","funcionario"]]
    if col_func:
        df.rename(columns={col_func[0]: "Funcionário"}, inplace=True)

    return df

df = carregar_base_feminina()

# =========================
# DIAGNÓSTICO RÁPIDO
# =========================
with st.expander("🔎 Diagnóstico da Leitura (clique para abrir)"):
    st.write("Linhas totais lidas:", len(df))
    if "DataParsed" in df.columns:
        st.write("Com Data válida:", int(df["DataParsed"].notna().sum()))
        st.write("Sem Data válida:", int(df["DataParsed"].isna().sum()))
    st.write("Colunas:", list(df.columns))
    st.dataframe(df.head(20), use_container_width=True)

# Se não veio nada mesmo, avisa e encerra
if df.empty:
    st.warning("Sem dados na aba **Base de Dados Feminino**.")
    st.stop()

# Mantém apenas linhas com Data válida para os gráficos/filtros
df_ok = df[df["DataParsed"].notna()].copy()
if df_ok.empty:
    st.warning("A aba foi lida, mas nenhuma linha tem **Data** válida. Verifique a coluna Data.")
    st.stop()

# =========================
# FILTROS
# =========================
st.sidebar.header("🎛️ Filtros")
meses_pt = {1:"Janeiro",2:"Fevereiro",3:"Março",4:"Abril",5:"Maio",6:"Junho",
            7:"Julho",8:"Agosto",9:"Setembro",10:"Outubro",11:"Novembro",12:"Dezembro"}

anos_disp = sorted(df_ok["Ano"].dropna().astype(int).unique(), reverse=True)
ano = st.sidebar.selectbox("🗓️ Ano", anos_disp, index=0)

meses_do_ano = sorted(df_ok.loc[df_ok["Ano"]==ano, "Mês"].dropna().astype(int).unique())
mes_labels = [meses_pt[m] for m in meses_do_ano]
meses_sel = st.sidebar.multiselect("📆 Meses (opcional)", mes_labels, default=mes_labels)

if meses_sel:
    meses_num = [k for k,v in meses_pt.items() if v in meses_sel]
    base = df_ok[(df_ok["Ano"]==ano) & (df_ok["Mês"].isin(meses_num))].copy()
else:
    base = df_ok[df_ok["Ano"]==ano].copy()

if base.empty:
    st.info("Sem dados para o período filtrado.")
    st.stop()

# =========================
# Excluir FIADO só na receita
# =========================
col_conta = next((c for c in base.columns if c.lower() in ["conta","forma de pagamento","pagamento","status"]), None)
mask_fiado = base[col_conta].astype(str).str.strip().str.lower().eq("fiado") if col_conta else pd.Series(False, index=base.index)
base_rec = base[~mask_fiado].copy()

# =========================
# INDICADORES
# =========================
receita_total = float(base_rec["ValorNum"].sum())
total_atend   = len(base)
data_limite = pd.to_datetime("2025-05-11")
antes  = base[base["DataParsed"] < data_limite]
depois = base[base["DataParsed"] >= data_limite].drop_duplicates(subset=["Cliente","DataParsed"])
clientes_unicos = pd.concat([antes, depois])["Cliente"].nunique()
ticket = receita_total/total_atend if total_atend else 0.0

def brl(x: float) -> str:
    return f"R$ {x:,.2f}".replace(",", "v").replace(".", ",").replace("v",".")

c1,c2,c3,c4 = st.columns(4)
c1.metric("💰 Receita Total", brl(receita_total))
c2.metric("📅 Total de Atendimentos", int(total_atend))
c3.metric("🎯 Ticket Médio", brl(ticket))
c4.metric("🟢 Clientes Ativos", int(clientes_unicos))

# =========================
# 📆 Receita Mensal
# =========================
st.markdown("### 📆 Receita Mensal (Ano selecionado)")
mens = (base_rec.groupby("Mês")["ValorNum"].sum()
        .reindex(range(1,13), fill_value=0).reset_index())
mens["MêsNome"] = mens["Mês"].map(meses_pt)
fig_mensal = px.bar(mens, x="MêsNome", y="ValorNum", text_auto=True,
                    labels={"ValorNum":"Receita (R$)", "MêsNome":"Mês"},
                    template="plotly_dark")
fig_mensal.update_layout(height=420, showlegend=False)
st.plotly_chart(fig_mensal, use_container_width=True)
mens["Receita (R$)"] = mens["ValorNum"].apply(brl)
st.dataframe(mens[["MêsNome","Receita (R$)"]], use_container_width=True)

# =========================
# 📊 Receita por Funcionário
# =========================
st.markdown("### 📊 Receita por Funcionário")
if "Funcionário" in base_rec.columns:
    df_func = (base_rec.groupby("Funcionário")["ValorNum"].sum()
               .reset_index().rename(columns={"ValorNum":"Valor"})
               .sort_values("Valor", ascending=False))
    fig = px.bar(df_func, x="Funcionário", y="Valor", text_auto=True, template="plotly_dark")
    fig.update_layout(height=400, yaxis_title="Receita (R$)", showlegend=False)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("A coluna **Funcionário** não existe na base.")

# =========================
# 🥇 Top 10 Clientes
# =========================
st.markdown("### 🥇 Top 10 Clientes (Feminino)")
nomes_excluir = ["boliviano","brasileiro","menino"]
cnt = base.groupby("Cliente")["Serviço"].count().rename("Qtd_Serviços") if "Serviço" in base.columns else pd.Series(dtype=int)
val = base_rec.groupby("Cliente")["ValorNum"].sum().rename("Valor") if "ValorNum" in base_rec.columns else pd.Series(dtype=float)
df_top = pd.concat([cnt,val], axis=1).reset_index().fillna(0)
df_top = df_top[~df_top["Cliente"].str.lower().isin(nomes_excluir)]
df_top = df_top.sort_values("Valor", ascending=False).head(10)
df_top["Valor Formatado"] = df_top["Valor"].apply(brl)
st.dataframe(df_top[["Cliente","Qtd_Serviços","Valor Formatado"]], use_container_width=True)
