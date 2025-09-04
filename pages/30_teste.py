# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import plotly.express as px
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials
import unicodedata
import requests

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

# === TELEGRAM ===
TG_TOKEN   = st.secrets["TELEGRAM"]["TOKEN"]
TG_CHAT_ID = st.secrets["TELEGRAM"]["CHAT_ID"]

# Logo padrão quando não existir foto da cliente
LOGO_PADRAO = "https://res.cloudinary.com/db8ipmete/image/upload/v1752463905/Logo_sal%C3%A3o_kz9y9c.png"

# -----------------------------
# Utils
# -----------------------------
def norm(s: str) -> str:
    if s is None:
        return ""
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
        s = s.replace(".", "").replace(",", ".")
    elif tem_virg and not tem_ponto:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        x = pd.to_numeric(s, errors="coerce")
        return float(x) if pd.notna(x) else 0.0

def achar_col(df, nomes):
    alvo = [n.strip().lower() for n in nomes]
    for c in df.columns:
        if c and c.strip().lower() in alvo:
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
    s = ""
    n = idx1_based
    while n > 0:
        n, r = divmod(n-1, 26)
        s = chr(65 + r) + s
    return s

# --- Telegram helpers ---
def tg_send(text: str, chat_id: str = None, parse_mode: str = "HTML"):
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        data = {
            "chat_id": chat_id or TG_CHAT_ID,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }
        requests.post(url, data=data, timeout=10)
    except Exception as e:
        st.warning(f"Falha ao enviar Telegram: {e}")

def tg_send_photo(photo_url: str, caption: str, chat_id: str = None, parse_mode: str = "HTML"):
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendPhoto"
        data = {
            "chat_id": chat_id or TG_CHAT_ID,
            "caption": caption[:1024],   # limite prático da legenda
            "parse_mode": parse_mode
        }
        files = None
        # envio simples com URL
        data["photo"] = photo_url or LOGO_PADRAO
        requests.post(url, data=data, files=files, timeout=15)
    except Exception as e:
        st.warning(f"Falha ao enviar foto no Telegram: {e}")

# === Conexão ===
@st.cache_resource
def conectar_sheets():
    info = st.secrets["GCP_SERVICE_ACCOUNT"]
    escopo = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credenciais = Credentials.from_service_account_info(info, scopes=escopo)
    cliente = gspread.authorize(credenciais)
    return cliente.open_by_key(SHEET_ID)

# === Carregar dados Feminino ===
@st.cache_data(ttl=300)
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

    df["Cliente"] = df["Cliente"].astype(str).str.strip()
    df["ValorNum"] = df["Valor"].apply(parse_valor_qualquer)
    return df

@st.cache_data(ttl=300)
def carregar_status_df():
    """Lê a planilha de status (para indicadores e fotos)."""
    try:
        planilha = conectar_sheets()
        ws = find_worksheet(planilha, [norm(x) for x in STATUS_ALVOS])
        df = get_as_dataframe(ws).dropna(how="all")
        df.columns = [c.strip() for c in df.columns]

        # identificar colunas núcleo
        col_cli = achar_col(df, ["Cliente"])
        col_sta = achar_col(df, ["Status"])
        if not col_cli or not col_sta:
            out = pd.DataFrame(columns=["Cliente", "Status"])
        else:
            out = df[[col_cli, col_sta]].copy()
            out.columns = ["Cliente", "Status"]

        # tentar identificar coluna de foto/link
        col_foto = achar_col(df, ["Foto", "Imagem", "Image", "URL", "Link", "FotoURL", "Foto Url", "Foto_Link"])
        if col_foto:
            out["FotoURL"] = df[col_foto].astype(str).fillna("").values
        else:
            out["FotoURL"] = ""

        out["Cliente"] = out["Cliente"].astype(str).str.strip()
        out["Status"]  = out["Status"].astype(str).str.strip()
        return out
    except Exception:
        return pd.DataFrame(columns=["Cliente", "Status", "FotoURL"])

def foto_da_cliente(nome: str, df_status: pd.DataFrame) -> str:
    """Retorna URL da foto da cliente, se existir no status; senão, LOGO_PADRAO."""
    if df_status is None or df_status.empty:
        return LOGO_PADRAO
    linha = df_status.loc[df_status["Cliente"].astype(str).str.strip() == str(nome).strip()]
    if not linha.empty:
        url = str(linha.iloc[0].get("FotoURL", "")).strip()
        return url if url else LOGO_PADRAO
    return LOGO_PADRAO

def atualizar_status_clientes_batch(status_map_norm: dict):
    """
    Atualiza a coluna 'Status' da aba FEMININO em uma única chamada,
    comparando clientes por nome normalizado (sem acento/caixa/espaços).
    Não altera linhas cujo Status atual seja 'Ignorado' (case-insensitive).

    status_map_norm: { norm(nome_cliente) : "Ativo"/"Inativo" }

    Retorna: (alterados:int, mudancas:list[dict]) onde cada item:
        {"cliente": <str>, "antes": <str>, "depois": <str>}
    """
    planilha = conectar_sheets()
    ws = find_worksheet(planilha, [norm(x) for x in STATUS_ALVOS])

    vals = ws.get_all_values()
    if not vals:
        return 0, []

    header = [h.strip() for h in vals[0]]
    try:
        cli_idx0 = header.index("Cliente")
        sta_idx0 = header.index("Status")
    except ValueError:
        cli_idx0, sta_idx0 = 0, 1  # fallback

    linhas = vals[1:]
    novos_status = []
    alterados = 0
    mudancas = []

    for row in linhas:
        nome_raw = (row[cli_idx0] if cli_idx0 < len(row) else "").strip()
        atual    = (row[sta_idx0] if sta_idx0 < len(row) else "").strip()

        # mantém "Ignorado"
        if atual.lower() == "ignorado":
            novos_status.append([atual])
            continue

        alvo_norm = norm(nome_raw)
        novo = status_map_norm.get(alvo_norm, atual) or atual

        if novo != atual:
            alterados += 1
            mudancas.append({"cliente": nome_raw, "antes": atual or "-", "depois": novo})
        novos_status.append([novo])

    if alterados == 0:
        return 0, []

    # Escreve a coluna inteira de Status de uma vez
    col_letra = excel_col_letter(sta_idx0 + 1)  # idx 1-based
    inicio = 2
    fim = len(vals)
    rng = f"{col_letra}{inicio}:{col_letra}{fim}"
    ws.update(rng, novos_status, value_input_option="RAW")

    return alterados, mudancas

# =============================
# Execução
# =============================
df = carregar_dados()
df_status = carregar_status_df()

# =============================
# 🔄 Status automático (execução automática 1x por sessão)
# =============================
st.markdown("### 🔄 Status automático de clientes (90 dias)")

# roda apenas 1x por sessão para evitar reprocesso/loops
if "_status_auto_ok_fem" not in st.session_state:
    st.session_state["_status_auto_ok_fem"] = False

if not st.session_state["_status_auto_ok_fem"]:
    try:
        df_full = df.copy()
        hoje = pd.Timestamp.today().normalize()

        # último atendimento por cliente
        ultimos = df_full.groupby("Cliente")["Data"].max().reset_index()
        ultimos["DiasDesde"] = (hoje - ultimos["Data"]).dt.days
        ultimos["StatusNovo"] = ultimos["DiasDesde"].apply(lambda x: "Inativo" if x > 90 else "Ativo")

        # dicionários auxiliares para legenda do card
        last_date_map = dict(zip(ultimos["Cliente"], ultimos["Data"]))
        days_map      = dict(zip(ultimos["Cliente"], ultimos["DiasDesde"]))

        # NÃO mexer nos "Ignorado"
        ignorados_set = set()
        if not df_status.empty and "Status" in df_status.columns and "Cliente" in df_status.columns:
            ignorados_set = set(
                df_status.loc[df_status["Status"].str.lower().eq("ignorado"), "Cliente"].astype(str)
            )

        # mapa normalizado (pula ignorados)
        status_map_norm = {}
        for _, r in ultimos.iterrows():
            if r["Cliente"] in ignorados_set:
                continue
            status_map_norm[norm(r["Cliente"])] = r["StatusNovo"]

        # aplica atualização em lote
        alterados, mudancas = atualizar_status_clientes_batch(status_map_norm)

        if alterados > 0:
            # limpa caches e recarrega status
            st.cache_data.clear()
            df_status = carregar_status_df()
            st.success(f"Status atualizado automaticamente ({alterados} linha(s) alterada(s)).")

            # --- TELEGRAM: enviar CARD com foto para CADA mudança (ambas as direções) ---
            for m in mudancas:
                nome   = m["cliente"]
                antes  = (m["antes"] or "-").strip()
                depois = (m["depois"] or "-").strip()
                foto   = foto_da_cliente(nome, df_status)

                # infos extras
                dt_ult = last_date_map.get(nome, pd.NaT)
                dias   = days_map.get(nome, None)
                if pd.isna(dt_ult) or dt_ult is pd.NaT:
                    dt_txt = "—"
                    dias_txt = "—"
                else:
                    dt_txt = pd.to_datetime(dt_ult).strftime("%d/%m/%Y")
                    dias_txt = f"{int(dias)} dia(s)" if dias is not None else "—"

                # ícone conforme direção
                if antes.lower() == "inativo" and depois.lower() == "ativo":
                    titulo = "✅ Reativada"
                elif antes.lower() == "ativo" and depois.lower() == "inativo":
                    titulo = "⛔️ Marcada como Inativa"
                else:
                    titulo = "🔁 Status atualizado"

                legenda = (
                    f"<b>{titulo}</b>\n"
                    f"👤 <b>{nome}</b>\n"
                    f"🪪 Status: <code>{antes}</code> → <b>{depois}</b>\n"
                    f"🗓 Último atendimento: <b>{dt_txt}</b>\n"
                    f"⏱ Dias desde o último: <b>{dias_txt}</b>\n"
                    f"💈 Setor: <i>Feminino</i>"
                )
                tg_send_photo(foto, legenda)

        else:
            st.info("Status já estava atualizado (ou clientes marcados como 'Ignorado').")

        st.session_state["_status_auto_ok_fem"] = True

    except Exception as e:
        st.warning(f"Não foi possível atualizar status automaticamente agora: {e}")
        st.session_state["_status_auto_ok_fem"] = True  # evita loop

# =============================
# Filtro por Ano (mantém o ano atual e a última escolha)
# =============================
anos = sorted(df["Ano"].unique().tolist())
ano_atual = pd.Timestamp.today().year
if ano_atual not in anos:
    anos.append(ano_atual)
    anos = sorted(anos)

opcoes_ano = ["Todos"] + anos

if "ano_selecionado" not in st.session_state:
    st.session_state["ano_selecionado"] = ano_atual

valor_inicial = st.session_state["ano_selecionado"]
if valor_inicial not in opcoes_ano:
    valor_inicial = ano_atual

ano_escolhido = st.selectbox(
    "📅 Selecione o ano",
    opcoes_ano,
    index=opcoes_ano.index(valor_inicial)
)
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
busca = st.text_input("🔎 Filtrar por nome").strip()
if busca:
    busca_norm = norm(busca)
    rank_view = ranking[ranking["Cliente"].apply(lambda n: busca_norm in norm(n))]
else:
    rank_view = ranking
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
