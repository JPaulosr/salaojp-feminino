import streamlit as st
import pandas as pd
import plotly.express as px
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials
import unicodedata

st.set_page_config(layout="wide")
st.title("🧍‍♀️ Clientes (Feminino) - Receita Total")

# === CONFIG GOOGLE SHEETS ===
SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"

# Possíveis nomes das guias (variações)
BASE_ALVOS = [
    "base de dados feminino", "base de dados - feminino",
    "base de dados (feminino)", "base de dados feminino "
]
STATUS_ALVOS = [
    "clientes_status_feminino", "clientes status feminino",
    "clientes_status feminino", "status_feminino"
]

# -----------------------------
# Utils
# -----------------------------
def norm(s: str) -> str:
    if s is None: return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    return " ".join(s.lower().strip().split())

def parse_valor_qualquer(v):
    """Converte 'R$ 1.234,56', '1.234,56', '25,00', '25.00', '25.0' ou número em float (sem inflar)."""
    if pd.isna(v): return 0.0
    if isinstance(v, (int, float)):
        return float(v)

    s = str(v).strip().replace("\u00A0", "")
    s = s.replace("R$", "").replace("r$", "").replace(" ", "")

    tem_virg = "," in s
    tem_ponto = "." in s

    if tem_virg and tem_ponto:
        # PT-BR: milhar '.' e decimal ','
        s = s.replace(".", "").replace(",", ".")
    elif tem_virg and not tem_ponto:
        # Só vírgula -> decimal
        s = s.replace(",", ".")
    else:
        # Só ponto (ou nenhum) -> ponto é decimal
        pass

    try:
        return float(s)
    except Exception:
        x = pd.to_numeric(s, errors="coerce")
        return float(x) if pd.notna(x) else 0.0

def achar_col(df, nomes):
    alvo = [n.strip().lower() for n in nomes]
    for c in df.columns:
        if c.strip().lower() in alvo:
            return c
    return None

def find_worksheet(planilha, alvos_norm):
    wss = planilha.worksheets()
    titulos = [ws.title for ws in wss]
    titulos_norm = [norm(t) for t in titulos]
    # 1) match exato
    for ws, tnorm in zip(wss, titulos_norm):
        if tnorm in alvos_norm:
            return ws
    # 2) contém
    for ws, tnorm in zip(wss, titulos_norm):
        if any(a in tnorm for a in alvos_norm):
            return ws
    st.error("❌ Não encontrei a aba feminina. Guias disponíveis:\n- " + "\n- ".join(titulos))
    st.stop()

def excel_col_letter(idx1_based: int) -> str:
    """Converte índice de coluna 1-based em letra (1->A, 2->B...)."""
    s = ""
    n = idx1_based
    while n > 0:
        n, r = divmod(n-1, 26)
        s = chr(65 + r) + s
    return s

# === Conexão ===
@st.cache_resource
def conectar_sheets():
    info = st.secrets["GCP_SERVICE_ACCOUNT"]
    escopo = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credenciais = Credentials.from_service_account_info(info, scopes=escopo)
    cliente = gspread.authorize(credenciais)
    return cliente.open_by_key(SHEET_ID)

# === Carregar dados Feminino ===
@st.cache_data
def carregar_dados():
    planilha = conectar_sheets()
    ws = find_worksheet(planilha, [norm(x) for x in BASE_ALVOS])
    df = get_as_dataframe(ws).dropna(how="all")
    df.columns = [col.strip() for col in df.columns]

    if "Data" not in df.columns:
        st.error("❌ Coluna 'Data' não encontrada na aba feminina."); st.stop()

    df["Data"] = pd.to_datetime(df["Data"], errors="coerce", dayfirst=True)
    df = df.dropna(subset=["Data"])
    df["Ano"] = df["Data"].dt.year.astype(int)

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
def carregar_status_df():
    """Lê a planilha de status (para indicadores na tela)."""
    try:
        planilha = conectar_sheets()
        ws = find_worksheet(planilha, [norm(x) for x in STATUS_ALVOS])
        df = get_as_dataframe(ws).dropna(how="all")
        df.columns = [c.strip() for c in df.columns]
        col_cli = achar_col(df, ["Cliente"]); col_sta = achar_col(df, ["Status"])
        if not col_cli or not col_sta:
            return pd.DataFrame(columns=["Cliente", "Status"])
        out = df[[col_cli, col_sta]].copy()
        out.columns = ["Cliente", "Status"]
        out["Cliente"] = out["Cliente"].astype(str).str.strip()
        out["Status"] = out["Status"].astype(str).str.strip()
        return out
    except Exception:
        return pd.DataFrame(columns=["Cliente", "Status"])

def atualizar_status_clientes_batch(status_map: dict) -> int:
    """
    Atualiza a coluna 'Status' da aba FEMININO em **uma única chamada**.
    status_map: {nome_cliente: "Ativo"/"Inativo"}
    Retorna quantidade de linhas alteradas.
    """
    planilha = conectar_sheets()
    ws = find_worksheet(planilha, [norm(x) for x in STATUS_ALVOS])

    vals = ws.get_all_values()   # [[Cliente, Status, ...], ...]
    if not vals:
        return 0

    header = [h.strip() for h in vals[0]]
    try:
        cli_idx0 = header.index("Cliente")         # 0-based
        sta_idx0 = header.index("Status")
    except ValueError:
        cli_idx0, sta_idx0 = 0, 1  # fallback

    linhas = vals[1:]
    novos_status = []
    alterados = 0

    for row in linhas:
        nome = (row[cli_idx0] if cli_idx0 < len(row) else "").strip()
        atual = (row[sta_idx0] if sta_idx0 < len(row) else "").strip()
        novo = status_map.get(nome, atual) or ""   # mantém se não estiver no mapa
        if novo != atual:
            alterados += 1
        novos_status.append([novo])

    if alterados == 0:
        return 0

    # Range de escrita: coluna Status da linha 2 até o fim
    col_letra = excel_col_letter(sta_idx0 + 1)  # idx 1-based
    inicio = 2
    fim = len(vals)
    rng = f"{col_letra}{inicio}:{col_letra}{fim}"

    ws.update(rng, novos_status, value_input_option="RAW")
    return alterados

# =============================
# Execução
# =============================
df = carregar_dados()
df_status = carregar_status_df()

# --- Status automático (sem botão) ---
hoje = pd.Timestamp.today().normalize()
ultimos = df.groupby("Cliente")["Data"].max().reset_index()
ultimos["DiasDesde"] = (hoje - ultimos["Data"]).dt.days
ultimos["StatusNovo"] = ultimos["DiasDesde"].apply(lambda x: "Inativo" if x > 90 else "Ativo")
status_map = dict(zip(ultimos["Cliente"], ultimos["StatusNovo"]))
try:
    _alterados = atualizar_status_clientes_batch(status_map)
    if _alterados > 0:
        st.cache_data.clear()
        df_status = carregar_status_df()
except Exception:
    pass

# =============================
# Filtro por Ano (mantém o ano atual e a última escolha)
# =============================
anos = sorted(df["Ano"].unique().tolist())
ano_atual = pd.Timestamp.today().year
# garante que o ano atual esteja na lista de opções
if ano_atual not in anos:
    anos.append(ano_atual)
    anos = sorted(anos)

opcoes_ano = ["Todos"] + anos

# inicia sessão com ano atual no primeiro acesso
if "ano_selecionado" not in st.session_state:
    st.session_state["ano_selecionado"] = ano_atual

# se a sessão tiver um valor que não existe nas opções, cai para ano atual
valor_inicial = st.session_state["ano_selecionado"]
if valor_inicial not in opcoes_ano:
    valor_inicial = ano_atual

ano_escolhido = st.selectbox(
    "📅 Selecione o ano",
    opcoes_ano,
    index=opcoes_ano.index(valor_inicial)
)
# atualiza a sessão ao mudar
st.session_state["ano_selecionado"] = ano_escolhido

# =============================
# Máscara FIADO e base filtrada por ano
# =============================
if "Conta" in df.columns:
    mask_fiado_full = df["Conta"].fillna("").astype(str).str.strip().str.lower().eq("fiado")
else:
    mask_fiado_full = pd.Series(False, index=df.index)

if ano_escolhido == "Todos":
    df_base = df.copy()
else:
    df_base = df[df["Ano"] == ano_escolhido].copy()

mask_fiado = mask_fiado_full.loc[df_base.index]
df_receita = df_base[~mask_fiado].copy()
df_fiado = df_base[mask_fiado].copy()

# =============================
# Indicadores
# =============================
clientes_unicos = df_base["Cliente"].nunique()
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

# Limpa nomes genéricos
ban = {"boliviano", "brasileiro", "menino", "menino boliviano"}
for _df in (df_base, df_receita, df_fiado):
    _df.drop(_df[_df["Cliente"].astype(str).str.lower().str.strip().isin(ban)].index, inplace=True)

# =============================
# Receita total por cliente (ano filtrado)
# =============================
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

# Top 5
st.subheader("🏆 Top 5 Clientes por Receita")
top5 = ranking.head(5)
fig_top = px.bar(
    top5, x="Cliente", y="Valor",
    text=top5["Valor"].apply(lambda x: f"R$ {x:,.0f}".replace(",", "v").replace(".", ",").replace("v", ".")),
    labels={"Valor": "Receita (R$)"}, color="Cliente", template="plotly_dark", height=400
)
fig_top.update_traces(textposition="outside", cliponaxis=False)
fig_top.update_layout(showlegend=False)
st.plotly_chart(fig_top, use_container_width=True)

# =============================
# Resultado por cliente por ANO (sempre sem fiado)
# =============================
st.subheader("🗓️ Resultado por cliente por ano (sem fiado)")

df_sem_fiado = df[~mask_fiado_full].copy()
df_sem_fiado = df_sem_fiado[~df_sem_fiado["Cliente"].astype(str).str.lower().str.strip().isin(ban)]

tabela_cliente_ano = (df_sem_fiado
    .groupby(["Cliente", "Ano"])["ValorNum"].sum()
    .reset_index()
    .sort_values(["Cliente", "Ano"]))

pivot_cliente_ano = tabela_cliente_ano.pivot(index="Cliente", columns="Ano", values="ValorNum").fillna(0.0)
pivot_fmt = pivot_cliente_ano.applymap(lambda x: f"R$ {x:,.2f}".replace(",", "v").replace(".", ",").replace("v", "."))

st.dataframe(pivot_fmt, use_container_width=True)

if not tabela_cliente_ano.empty:
    st.markdown("**Evolução por ano (selecione uma cliente):**")
    clientes_lista = sorted(tabela_cliente_ano["Cliente"].unique().tolist())
    cliente_sel = st.selectbox("👤 Cliente", clientes_lista, index=0)
    df_cli = tabela_cliente_ano[tabela_cliente_ano["Cliente"] == cliente_sel]
    fig_cli = px.bar(
        df_cli, x="Ano", y="ValorNum",
        text=df_cli["ValorNum"].apply(lambda x: f"R$ {x:,.0f}".replace(",", "v").replace(".", ",").replace("v", ".")),
        labels={"ValorNum": "Receita (R$)"}, template="plotly_dark", height=380
    )
    fig_cli.update_traces(textposition="outside", cliponaxis=False)
    st.plotly_chart(fig_cli, use_container_width=True)

# =============================
# Comparativo entre duas clientes (respeita filtro de ano)
# =============================
st.subheader("⚖️ Comparar duas clientes")
if not ranking.empty:
    colA, colB = st.columns(2)
    c1_sel = colA.selectbox("👤 Cliente 1", ranking["Cliente"].tolist())
    idx2 = 1 if len(ranking) > 1 else 0
    c2_sel = colB.selectbox("👤 Cliente 2", ranking["Cliente"].tolist(), index=idx2)

    df_c1_val = df_receita[df_receita["Cliente"] == c1_sel]
    df_c2_val = df_receita[df_receita["Cliente"] == c2_sel]
    df_c1_hist = df_base[df_base["Cliente"] == c1_sel]
    df_c2_hist = df_base[df_base["Cliente"] == c2_sel]

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
    st.dataframe(pd.concat([r1.rename(c1_sel), r2.rename(c2_sel)], axis=1), use_container_width=True)
    st.markdown("**Serviços Realizados por Tipo**")
    st.dataframe(pd.concat([s1.rename(c1_sel), s2.rename(c2_sel)].copy(), axis=1).fillna(0).astype(int), use_container_width=True)

# =============================
# Fiados (respeita filtro de ano)
# =============================
st.markdown("### 💳 Fiados — Resumo e Detalhes (Feminino)")
total_fiado = df_fiado["ValorNum"].sum()
colf1, colf2, colf3 = st.columns(3)
colf1.metric("💸 Total em fiado (aberto)", f"R$ {total_fiado:,.2f}".replace(",", "v").replace(".", ",").replace("v", "."))
colf2.metric("👤 Clientes com fiado", int(df_fiado["Cliente"].nunique()))
colf3.metric("🧾 Registros de fiado", int(len(df_fiado)))

if not df_fiado.empty:
    top_fiado = (df_fiado.groupby("Cliente")["ValorNum"].sum()
                 .reset_index().sort_values("ValorNum", ascending=False).head(10))
    top_fiado["Valor Formatado"] = top_fiado["ValorNum"].apply(
        lambda x: f"R$ {x:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
    )
    fig_fiado = px.bar(
        top_fiado, x="Cliente", y="ValorNum", text=top_fiado["Valor Formatado"],
        labels={"ValorNum": "Fiado (R$)"}, color="Cliente",
        template="plotly_dark", height=380
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
    st.dataframe(fiado_detalhe, use_container_width=True)
else:
    st.info("Nenhum fiado em aberto encontrado para os filtros atuais (feminino).")

# =============================
# Navegar para detalhamento
# =============================
st.subheader("🔍 Ver detalhamento de uma cliente")
if not ranking.empty:
    cliente_escolhido = st.selectbox("📌 Escolha uma cliente", ranking["Cliente"].tolist())
    if st.button("➡ Ver detalhes"):
        st.session_state["cliente"] = cliente_escolhido
        st.switch_page("pages/2_DetalhesCliente.py")
