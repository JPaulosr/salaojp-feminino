import streamlit as st
import pandas as pd
import plotly.express as px
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials
import unicodedata

st.set_page_config(layout="wide")
st.title("🧍‍♀️ Clientes (Feminino) - Receita Total")

# === CONFIGURAÇÃO GOOGLE SHEETS ===
SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"

# Nomes "alvo" (podem variar ligeiramente na planilha)
BASE_ALVOS = [
    "base de dados feminino",
    "base de dados - feminino",
    "base de dados (feminino)",
    "base de dados feminino ",
]
STATUS_ALVOS = [
    "clientes_status_feminino",
    "clientes status feminino",
    "clientes_status feminino",
    "status_feminino",
]

# -----------------------------
# Utilidades
# -----------------------------
def norm(s: str) -> str:
    """Remove acento, baixa, tira espaços repetidos."""
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    s = " ".join(s.lower().strip().split())
    return s

def parse_valor_qualquer(v):
    if pd.isna(v):
        return 0.0
    s = str(v).strip()
    if not s:
        return 0.0
    s = (s.replace("R$", "").replace("r$", "").replace(" ", "")
           .replace(".", "").replace("\u00A0", ""))
    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return float(pd.to_numeric(v, errors="coerce") or 0)

def achar_col(df, nomes):
    alvo = [n.strip().lower() for n in nomes]
    for c in df.columns:
        if c.strip().lower() in alvo:
            return c
    return None

def find_worksheet(planilha, alvos_norm):
    """Procura uma worksheet cuja versão normalizada bata com qualquer alvo."""
    wss = planilha.worksheets()
    titulos = [ws.title for ws in wss]
    titulos_norm = [norm(t) for t in titulos]

    # 1) match exato normalizado
    for ws, tnorm in zip(wss, titulos_norm):
        if tnorm in alvos_norm:
            return ws

    # 2) match por "contém" (mais tolerante)
    for ws, tnorm in zip(wss, titulos_norm):
        if any(a in tnorm for a in alvos_norm):
            return ws

    # 3) não encontrou: mostra opções
    st.error(
        "❌ Não encontrei a aba desejada.\n\n"
        "Guias disponíveis na planilha:\n- " + "\n- ".join(titulos)
    )
    st.stop()

# === Conectar Google Sheets ===
@st.cache_resource
def conectar_sheets():
    info = st.secrets["GCP_SERVICE_ACCOUNT"]
    escopo = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credenciais = Credentials.from_service_account_info(info, scopes=escopo)
    cliente = gspread.authorize(credenciais)
    return cliente.open_by_key(SHEET_ID)

# === Carregar dados principais (Feminino) ===
@st.cache_data
def carregar_dados():
    planilha = conectar_sheets()
    ws = find_worksheet(planilha, [norm(x) for x in BASE_ALVOS])
    df = get_as_dataframe(ws).dropna(how="all")
    df.columns = [col.strip() for col in df.columns]

    # Data
    if "Data" not in df.columns:
        st.error("❌ Coluna 'Data' não encontrada na aba feminina.")
        st.stop()
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce", dayfirst=True)
    df = df.dropna(subset=["Data"])
    df["Ano"] = df["Data"].dt.year.astype(int)

    # Renomeios seguros
    col_serv = achar_col(df, ["Serviço", "Servico"])
    if col_serv and col_serv != "Serviço":
        df.rename(columns={col_serv: "Serviço"}, inplace=True)

    col_valor = achar_col(df, ["Valor"])
    if col_valor and col_valor != "Valor":
        df.rename(columns={col_valor: "Valor"}, inplace=True)

    col_conta = achar_col(df, ["Conta", "Forma de pagamento", "Pagamento", "Status"])
    if col_conta and col_conta != "Conta":
        df.rename(columns={col_conta: "Conta"}, inplace=True)

    col_cli = achar_col(df, ["Cliente"])
    if col_cli and col_cli != "Cliente":
        df.rename(columns={col_cli: "Cliente"}, inplace=True)

    df["ValorNum"] = df["Valor"].apply(parse_valor_qualquer)
    return df

@st.cache_data
def carregar_status():
    try:
        planilha = conectar_sheets()
        ws = find_worksheet(planilha, [norm(x) for x in STATUS_ALVOS])
        df_status = get_as_dataframe(ws).dropna(how="all")
        df_status.columns = [col.strip() for col in df_status.columns]
        col_cli = achar_col(df_status, ["Cliente"])
        col_sta = achar_col(df_status, ["Status"])
        if not col_cli or not col_sta:
            return pd.DataFrame(columns=["Cliente", "Status"])
        out = df_status[[col_cli, col_sta]].copy()
        out.columns = ["Cliente", "Status"]
        out["Cliente"] = out["Cliente"].astype(str).str.strip()
        out["Status"] = out["Status"].astype(str).str.strip()
        return out
    except Exception as e:
        st.warning(f"⚠️ Não foi possível ler o status feminino: {e}")
        return pd.DataFrame(columns=["Cliente", "Status"])

# === Atualizar status ===
def atualizar_status_clientes(ultimos_status):
    try:
        planilha = conectar_sheets()
        ws = find_worksheet(planilha, [norm(x) for x in STATUS_ALVOS])
        dados = ws.get_all_records()
        atualizados = 0
        for i, linha in enumerate(dados, start=2):
            nome = str(linha.get("Cliente", "")).strip()
            status_atual = str(linha.get("Status", "")).strip()
            status_novo = ultimos_status.get(nome)
            if status_novo and status_novo != status_atual:
                ws.update_cell(i, 2, status_novo)  # coluna 2 = Status
                atualizados += 1
        return atualizados
    except Exception as e:
        st.warning(f"⚠️ Erro ao atualizar status (feminino): {e}")
        return 0

# === Execução ===
df = carregar_dados()
df_status = carregar_status()

# === Mascara fiado (não entra na receita) ===
if "Conta" in df.columns:
    mask_fiado = df["Conta"].fillna("").astype(str).str.strip().str.lower().eq("fiado")
else:
    mask_fiado = pd.Series(False, index=df.index)

df_receita = df[~mask_fiado].copy()
df_fiado = df[mask_fiado].copy()

# === Status por recência (90d) ===
hoje = pd.Timestamp.today().normalize()
ultimos = df.groupby("Cliente")["Data"].max().reset_index()
ultimos["DiasDesde"] = (hoje - ultimos["Data"]).dt.days
ultimos["StatusNovo"] = ultimos["DiasDesde"].apply(lambda x: "Inativo" if x > 90 else "Ativo")
qtd = atualizar_status_clientes(dict(zip(ultimos["Cliente"], ultimos["StatusNovo"])))
if qtd > 0:
    st.success(f"🔄 {qtd} cliente(s) tiveram seus status atualizados (feminino).")

# === Indicadores ===
clientes_unicos = df["Cliente"].nunique()
contagem_status = df_status["Status"].value_counts().to_dict() if not df_status.empty else {}
ativos = contagem_status.get("Ativo", 0)
ignorados = contagem_status.get("Ignorado", 0)
inativos = contagem_status.get("Inativo", 0)

st.markdown("### 📊 Indicadores Gerais (Feminino)")
c1, c2, c3, c4 = st.columns(4)
c1.metric("👥 Clientes únicas", clientes_unicos)
c2.metric("✅ Ativas", ativos)
c3.metric("🚫 Ignoradas", ignorados)
c4.metric("🚩 Inativas", inativos)

# === Limpa nomes genéricos (se houver)
ban = {"boliviano", "brasileiro", "menino", "menino boliviano"}
df = df[~df["Cliente"].astype(str).str.lower().str.strip().isin(ban)]
df_receita = df_receita[~df_receita["Cliente"].astype(str).str.lower().str.strip().isin(ban)]
df_fiado = df_fiado[~df_fiado["Cliente"].astype(str).str.lower().str.strip().isin(ban)]

# === Ranking
ranking = (df_receita.groupby("Cliente")["ValorNum"].sum()
           .reset_index().rename(columns={"ValorNum": "Valor"})
           .sort_values("Valor", ascending=False))
ranking["Valor Formatado"] = ranking["Valor"].apply(
    lambda x: f"R$ {x:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
)

st.subheader("📟 Receita total por cliente (Feminino)")
busca = st.text_input("🔎 Filtrar por nome").lower().strip()
rank_view = ranking[ranking["Cliente"].str.lower().str.contains(busca)] if busca else ranking
st.dataframe(rank_view[["Cliente", "Valor Formatado"]], use_container_width=True)

# === Top 5
st.subheader("🏆 Top 5 Clientes por Receita")
top5 = ranking.head(5)
fig_top = px.bar(
    top5, x="Cliente", y="Valor",
    text=top5["Valor"].apply(lambda x: f"R$ {x:,.0f}".replace(",", "v").replace(".", ",").replace("v", ".")),
    labels={"Valor": "Receita (R$)"},
    color="Cliente", template="plotly_white", height=400
)
fig_top.update_traces(textposition="outside", cliponaxis=False)
fig_top.update_layout(showlegend=False)
st.plotly_chart(fig_top, use_container_width=True)

# === Comparativo
st.subheader("⚖️ Comparar duas clientes")
if not ranking.empty:
    colA, colB = st.columns(2)
    c1 = colA.selectbox("👤 Cliente 1", ranking["Cliente"].tolist())
    idx2 = 1 if len(ranking) > 1 else 0
    c2 = colB.selectbox("👤 Cliente 2", ranking["Cliente"].tolist(), index=idx2)

    df_c1_val = df_receita[df_receita["Cliente"] == c1]
    df_c2_val = df_receita[df_receita["Cliente"] == c2]
    df_c1_hist = df[df["Cliente"] == c1]
    df_c2_hist = df[df["Cliente"] == c2]

    def resumo_cliente(df_val, df_hist):
        total = df_val["ValorNum"].sum()
        servicos = df_hist["Serviço"].nunique() if "Serviço" in df_hist.columns else 0
        media = df_val.groupby("Data")["ValorNum"].sum().mean()
        media = 0 if pd.isna(media) else media
        servicos_detalhados = (df_hist["Serviço"].value_counts().rename("Quantidade")
                               if "Serviço" in df_hist.columns else pd.Series(dtype=int))
        return pd.Series({
            "Total Receita": f"R$ {total:,.2f}".replace(",", "v").replace(".", ",").replace("v", "."),
            "Serviços Distintos": servicos,
            "Tique Médio": f"R$ {media:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
        }), servicos_detalhados

    r1, s1 = resumo_cliente(df_c1_val, df_c1_hist)
    r2, s2 = resumo_cliente(df_c2_val, df_c2_hist)
    st.dataframe(pd.concat([r1.rename(c1), r2.rename(c2)], axis=1), use_container_width=True)
    st.markdown("**Serviços Realizados por Tipo**")
    st.dataframe(pd.concat([s1.rename(c1), s2.rename(c2)], axis=1).fillna(0).astype(int), use_container_width=True)

# === Fiados
st.markdown("### 💳 Fiados — Resumo e Detalhes (Feminino)")
total_fiado = df_fiado["ValorNum"].sum()
clientes_fiado = df_fiado["Cliente"].nunique()
registros_fiado = len(df_fiado)
colf1, colf2, colf3 = st.columns(3)
colf1.metric("💸 Total em fiado (aberto)", f"R$ {total_fiado:,.2f}".replace(",", "v").replace(".", ",").replace("v", "."))
colf2.metric("👤 Clientes com fiado", int(clientes_fiado))
colf3.metric("🧾 Registros de fiado", int(registros_fiado))

if not df_fiado.empty:
    top_fiado = (df_fiado.groupby("Cliente")["ValorNum"].sum()
                 .reset_index().sort_values("ValorNum", ascending=False).head(10))
    top_fiado["Valor Formatado"] = top_fiado["ValorNum"].apply(
        lambda x: f"R$ {x:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
    )
    fig_fiado = px.bar(
        top_fiado, x="Cliente", y="ValorNum", text=top_fiado["Valor Formatado"],
        labels={"ValorNum": "Fiado (R$)"}, color="Cliente",
        template="plotly_white", height=380
    )
    fig_fiado.update_traces(textposition="outside", cliponaxis=False)
    fig_fiado.update_layout(showlegend=False)
    st.plotly_chart(fig_fiado, use_container_width=True)

    cols_base = ["Data", "Cliente", "Serviço", "ValorNum"] if "Serviço" in df_fiado.columns else ["Data", "Cliente", "ValorNum"]
    fiado_detalhe = (df_fiado[cols_base]
                     .sort_values(by=["Cliente", "Data"], ascending=[True, False])
                     .rename(columns={"ValorNum": "Valor"}))
    fiado_detalhe["Valor"] = fiado_detalhe["Valor"].apply(
        lambda x: f"R$ {x:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
    )
    st.markdown("**Detalhamento (fiados em aberto)**")
    st.dataframe(fiado_detalhe, use_container_width=True)

    csv_bytes = fiado_detalhe.to_csv(index=False).encode("utf-8-sig")
    st.download_button("⬇️ Baixar fiados (CSV)", data=csv_bytes,
                       file_name="fiados_em_aberto_feminino.csv", mime="text/csv")
else:
    st.info("Nenhum fiado em aberto encontrado para os filtros atuais (feminino).")

# === Navegar para detalhamento
st.subheader("🔍 Ver detalhamento de uma cliente")
if not ranking.empty:
    cliente_escolhido = st.selectbox("📌 Escolha uma cliente", ranking["Cliente"].tolist())
    if st.button("➡ Ver detalhes"):
        st.session_state["cliente"] = cliente_escolhido
        # se tiver uma página específica feminina, ajuste aqui:
        # st.switch_page("pages/2F_DetalhesCliente.py")
        st.switch_page("pages/2_DetalhesCliente.py")
