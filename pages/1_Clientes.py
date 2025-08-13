import streamlit as st
import pandas as pd
import plotly.express as px
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials

st.set_page_config(layout="wide")
st.title("🧍‍♂️ Clientes - Receita Total")

# === CONFIGURAÇÃO GOOGLE SHEETS ===
SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"
BASE_ABA = "Base de Dados"
STATUS_ABA = "clientes_status"

# === Função para conectar ao Google Sheets ===
@st.cache_resource
def conectar_sheets():
    info = st.secrets["GCP_SERVICE_ACCOUNT"]
    escopo = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credenciais = Credentials.from_service_account_info(info, scopes=escopo)
    cliente = gspread.authorize(credenciais)
    return cliente.open_by_key(SHEET_ID)

# === Carregar dados principais ===
@st.cache_data
def carregar_dados():
    planilha = conectar_sheets()
    aba = planilha.worksheet(BASE_ABA)
    df = get_as_dataframe(aba).dropna(how="all")
    df.columns = [col.strip() for col in df.columns]
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
    df = df.dropna(subset=["Data"])
    df["Ano"] = df["Data"].dt.year.astype(int)
    return df

@st.cache_data
def carregar_status():
    try:
        planilha = conectar_sheets()
        aba = planilha.worksheet(STATUS_ABA)
        df_status = get_as_dataframe(aba).dropna(how="all")
        df_status.columns = [col.strip() for col in df_status.columns]
        return df_status[["Cliente", "Status"]]
    except:
        return pd.DataFrame(columns=["Cliente", "Status"])

# === Atualizar status de clientes automaticamente ===
def atualizar_status_clientes(ultimos_status):
    try:
        planilha = conectar_sheets()
        aba_status = planilha.worksheet(STATUS_ABA)
        dados = aba_status.get_all_records()

        atualizados = 0
        for i, linha in enumerate(dados, start=2):
            nome = linha.get("Cliente", "").strip()
            status_atual = linha.get("Status", "").strip()
            status_novo = ultimos_status.get(nome)

            if status_novo and status_novo != status_atual:
                aba_status.update_cell(i, 2, status_novo)
                atualizados += 1

        return atualizados
    except Exception as e:
        st.warning(f"⚠️ Erro ao atualizar status dos clientes: {e}")
        return 0

# === Executa carregamento e atualiza status ===
df = carregar_dados()
df_status = carregar_status()

# === Filtro de RECEITA: excluir FIADO dos valores, mas manter frequência/histórico ===
col_conta = next((c for c in df.columns
                  if c.strip().lower() in ["conta", "forma de pagamento", "pagamento", "status"]), None)

if col_conta:
    serie_conta = (
        df[col_conta]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
    )
    mask_fiado = serie_conta.eq("fiado")
else:
    mask_fiado = pd.Series(False, index=df.index)

df_receita = df[~mask_fiado].copy()
df_receita["ValorNum"] = pd.to_numeric(df_receita["Valor"], errors="coerce").fillna(0)

df_fiado = df[mask_fiado].copy()
df_fiado["ValorNum"] = pd.to_numeric(df_fiado["Valor"], errors="coerce").fillna(0)

# === Lógica de atualização de status ===
hoje = pd.Timestamp.today().normalize()
limite_dias = 90

ultimos = df.groupby("Cliente")["Data"].max().reset_index()
ultimos["DiasDesde"] = (hoje - ultimos["Data"]).dt.days
ultimos["StatusNovo"] = ultimos["DiasDesde"].apply(lambda x: "Inativo" if x > limite_dias else "Ativo")

status_atualizado = dict(zip(ultimos["Cliente"], ultimos["StatusNovo"]))
qtd = atualizar_status_clientes(status_atualizado)
if qtd > 0:
    st.success(f"🔄 {qtd} cliente(s) tiveram seus status atualizados automaticamente.")

# === Indicadores ===
clientes_unicos = df["Cliente"].nunique()
contagem_status = df_status["Status"].value_counts().to_dict()
ativos = contagem_status.get("Ativo", 0)
ignorados = contagem_status.get("Ignorado", 0)
inativos = contagem_status.get("Inativo", 0)

st.markdown("### 📊 Indicadores Gerais")
col1, col2, col3, col4 = st.columns(4)
col1.metric("👥 Clientes únicos", clientes_unicos)
col2.metric("✅ Ativos", ativos)
col3.metric("🚫 Ignorados", ignorados)
col4.metric("🚩 Inativos", inativos)

# === Remove nomes genéricos ===
nomes_ignorar = ["boliviano", "brasileiro", "menino", "menino boliviano"]
normalizar = lambda s: str(s).lower().strip()
df = df[~df["Cliente"].apply(lambda x: normalizar(x) in nomes_ignorar)]
df_receita = df_receita[~df_receita["Cliente"].apply(lambda x: normalizar(x) in nomes_ignorar)]
df_fiado = df_fiado[~df_fiado["Cliente"].apply(lambda x: normalizar(x) in nomes_ignorar)]

# === Ranking geral ===
ranking = df_receita.groupby("Cliente")["ValorNum"].sum().reset_index().rename(columns={"ValorNum": "Valor"})
ranking = ranking.sort_values(by="Valor", ascending=False)
ranking["Valor Formatado"] = ranking["Valor"].apply(
    lambda x: f"R$ {x:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
)

# === Busca dinâmica ===
st.subheader("📟 Receita total por cliente")
busca = st.text_input("🔎 Filtrar por nome").lower().strip()
if busca:
    ranking_exibido = ranking[ranking["Cliente"].str.lower().str.contains(busca)]
else:
    ranking_exibido = ranking.copy()
st.dataframe(ranking_exibido[["Cliente", "Valor Formatado"]], use_container_width=True)

# === Top 5 ===
st.subheader("🏆 Top 5 Clientes por Receita")
top5 = ranking.head(5)
fig_top = px.bar(
    top5,
    x="Cliente",
    y="Valor",
    text=top5["Valor"].apply(lambda x: f"R$ {x:,.0f}".replace(",", "v").replace(".", ",").replace("v", ".")),
    labels={"Valor": "Receita (R$)"},
    color="Cliente"
)
fig_top.update_traces(textposition="outside")
fig_top.update_layout(showlegend=False, height=400, template="plotly_white")
st.plotly_chart(fig_top, use_container_width=True)

# === Comparativo ===
st.subheader("⚖️ Comparar dois clientes")
clientes_disponiveis = ranking["Cliente"].tolist()
colA, colB = st.columns(2)
c1 = colA.selectbox("👤 Cliente 1", clientes_disponiveis)
c2 = colB.selectbox("👤 Cliente 2", clientes_disponiveis, index=1 if len(clientes_disponiveis) > 1 else 0)

df_c1_val = df_receita[df_receita["Cliente"] == c1]
df_c2_val = df_receita[df_receita["Cliente"] == c2]
df_c1_hist = df[df["Cliente"] == c1]
df_c2_hist = df[df["Cliente"] == c2]

def resumo_cliente(df_val, df_hist):
    total = df_val["ValorNum"].sum()
    servicos = df_hist["Serviço"].nunique()
    media = df_val.groupby("Data")["ValorNum"].sum().mean()
    media = 0 if pd.isna(media) else media
    servicos_detalhados = df_hist["Serviço"].value_counts().rename("Quantidade")
    return pd.Series({
        "Total Receita": f"R$ {total:,.2f}".replace(",", "v").replace(".", ",").replace("v", "."),
        "Serviços Distintos": servicos,
        "Tique Médio": f"R$ {media:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
    }), servicos_detalhados

resumo1, servicos1 = resumo_cliente(df_c1_val, df_c1_hist)
resumo2, servicos2 = resumo_cliente(df_c2_val, df_c2_hist)

resumo_geral = pd.concat([resumo1.rename(c1), resumo2.rename(c2)], axis=1)
servicos_comparativo = pd.concat([servicos1.rename(c1), servicos2.rename(c2)], axis=1).fillna(0).astype(int)

st.dataframe(resumo_geral, use_container_width=True)
st.markdown("**Serviços Realizados por Tipo**")
st.dataframe(servicos_comparativo, use_container_width=True)

# === BLOCO DE FIADOS ===
st.markdown("### 💳 Fiados — Resumo e Detalhes")

colf1, colf2, colf3 = st.columns(3)
total_fiado = df_fiado["ValorNum"].sum()
clientes_fiado = df_fiado["Cliente"].nunique()
registros_fiado = len(df_fiado)
colf1.metric("💸 Total em fiado (aberto)", f"R$ {total_fiado:,.2f}".replace(",", "v").replace(".", ",").replace("v", "."))
colf2.metric("👤 Clientes com fiado", int(clientes_fiado))
colf3.metric("🧾 Registros de fiado", int(registros_fiado))

if not df_fiado.empty:
    st.markdown("**Top 10 clientes em fiado (valor em aberto)**")
    top_fiado = (
        df_fiado.groupby("Cliente")["ValorNum"]
        .sum()
        .reset_index()
        .sort_values(by="ValorNum", ascending=False)
        .head(10)
    )
    top_fiado["Valor Formatado"] = top_fiado["ValorNum"].apply(
        lambda x: f"R$ {x:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
    )
    fig_fiado = px.bar(
        top_fiado,
        x="Cliente",
        y="ValorNum",
        text=top_fiado["Valor Formatado"],
        labels={"ValorNum": "Fiado (R$)"},
        color="Cliente"
    )
    fig_fiado.update_traces(textposition="outside")
    fig_fiado.update_layout(showlegend=False, height=380, template="plotly_white")
    st.plotly_chart(fig_fiado, use_container_width=True)

    fiado_detalhe = df_fiado[["Data", "Cliente", "Serviço", "ValorNum"]].sort_values(
        by=["Cliente", "Data"], ascending=[True, False]
    )
    fiado_detalhe.rename(columns={"ValorNum": "Valor"}, inplace=True)
    fiado_detalhe["Valor"] = fiado_detalhe["Valor"].apply(
        lambda x: f"R$ {x:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
    )
    st.markdown("**Detalhamento (fiados em aberto)**")
    st.dataframe(fiado_detalhe, use_container_width=True)

    csv_bytes = fiado_detalhe.to_csv(index=False).encode("utf-8-sig")
    st.download_button("⬇️ Baixar fiados (CSV)", data=csv_bytes, file_name="fiados_em_aberto.csv", mime="text/csv")
else:
    st.info("Nenhum fiado em aberto encontrado para os filtros atuais.")

# === Navegar para detalhamento ===
st.subheader("🔍 Ver detalhamento de um cliente")
cliente_escolhido = st.selectbox("📌 Escolha um cliente", ranking["Cliente"].tolist())

if st.button("➡ Ver detalhes"):
    st.session_state["cliente"] = cliente_escolhido
    st.switch_page("pages/2_DetalhesCliente.py")
