# -*- coding: utf-8 -*-
# 12_Comissoes_Daniela.py — Pagamento de comissão (linhas por DIA do atendimento)
# Regras (mantido igual ao seu fluxo atual):
# - Paga toda terça o período de terça→segunda anterior.
# - Fiado só entra quando DataPagamento <= terça do pagamento.
# - Em Despesas grava UMA LINHA POR DIA DO ATENDIMENTO (Data = data do serviço).
# - Evita duplicidades via sheet "comissoes_cache_feminino" com RefID por atendimento.
# - Arredondamento opcional para preço cheio por serviço (tabela) com tolerância.
# - Bloco extra: FIADOS A RECEBER (histórico — ainda NÃO pagos), com comissão futura.
# Ajustes pedidos:
# - Gravar despesas em "Despesas Feminino".
# - Salvar/recuperar percentuais por serviço em "config_comissoes_feminino".
# - Enviar Telegram detalhado para JPaulo e para Daniela com cada cliente+serviço+comissão e total no fim.

import streamlit as st
import pandas as pd
import gspread
import hashlib
import re
import requests
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import pytz

# =============================
# CONFIG BÁSICA
# =============================
SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"

# >>> usar a base FEMININO <<<
ABA_DADOS = "Base de Dados Feminino"
ABA_COMISSOES_CACHE = "comissoes_cache_feminino"
ABA_DESPESAS_FEM = "Despesas Feminino"          # <— NOVO alvo das despesas
ABA_CONFIG = "config_comissoes_feminino"        # <— NOVA aba para % por serviço

TZ = "America/Sao_Paulo"
FUNCIONARIA = "Daniela"  # <- alvo desta folha

# Colunas esperadas na Base de Dados
COLS_OFICIAIS = [
    "Data", "Serviço", "Valor", "Conta", "Cliente", "Combo",
    "Funcionário", "Fase", "Tipo", "Período",
    # Fiado
    "StatusFiado", "IDLancFiado", "VencimentoFiado", "DataPagamento"
]

# Colunas da aba Despesas
COLS_DESPESAS_FIX = ["Data", "Prestador", "Descrição", "Valor", "Me Pag:"]

# Percentual padrão da comissão
PERCENTUAL_PADRAO = 50.0

# Tabela de preços (valores CHEIOS por serviço) — FEMININO
VALOR_TABELA = {
    "Corte": 35.00,
    "Escova": 25.00,
    "Unha mão": 25.00,
    "Unha pé": 25.00,
    "Sobrancelhas": 25.00,
    "Designer de Henna": 30.00,
    "Manicure": 25.00,
    "Pedicure": 30.00,
    "Progressiva": 150.00,
}

# =============================
# CONEXÃO SHEETS
# =============================
@st.cache_resource
def _conn():
    info = st.secrets["GCP_SERVICE_ACCOUNT"]
    escopo = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    cred = Credentials.from_service_account_info(info, scopes=escopo)
    cli = gspread.authorize(cred)
    return cli.open_by_key(SHEET_ID)

def _ws(title: str):
    sh = _conn()
    try:
        return sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=2000, cols=50)
        return ws

def _read_df(title: str) -> pd.DataFrame:
    ws = _ws(title)
    df = get_as_dataframe(ws).fillna("")
    df.columns = [str(c).strip() for c in df.columns]
    df = df.dropna(how="all").replace({pd.NA: ""})
    return df

def _write_df(title: str, df: pd.DataFrame):
    ws = _ws(title)
    ws.clear()
    set_with_dataframe(ws, df, include_index=False, include_column_header=True)

# =============================
# CONFIG: % por serviço (persistência)
# =============================
def _read_config() -> dict:
    """Lê a aba de configuração de percentuais por serviço."""
    try:
        df = _read_df(ABA_CONFIG)
    except Exception:
        df = pd.DataFrame(columns=["Serviço", "PercentualPadrao"])
    if "Serviço" not in df.columns or "PercentualPadrao" not in df.columns:
        df = pd.DataFrame(columns=["Serviço", "PercentualPadrao"])
    df["Serviço"] = df["Serviço"].astype(str).str.strip()
    out = {}
    for _, r in df.iterrows():
        s = str(r.get("Serviço","")).strip()
        try:
            p = float(str(r.get("PercentualPadrao","")).replace(",", "."))
        except:
            p = None
        if s and p is not None:
            out[s] = p
    return out

def _write_config(perc_map: dict):
    """Grava a tabela {Serviço, PercentualPadrao} na aba de config."""
    if not perc_map:
        return
    df = pd.DataFrame(
        [{"Serviço": k, "PercentualPadrao": float(v)} for k, v in sorted(perc_map.items())]
    )
    _write_df(ABA_CONFIG, df)

# =============================
# HELPERS
# =============================
def br_now():
    return datetime.now(pytz.timezone(TZ))

def parse_br_date(s: str):
    s = (s or "").strip()
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    return None

def to_br_date(dt: datetime):
    return dt.strftime("%d/%m/%Y")

def competencia_from_data_str(data_servico_str: str) -> str:
    dt = parse_br_date(data_servico_str)
    if not dt:
        return ""
    return dt.strftime("%m/%Y")

def janela_terca_a_segunda(terca_pagto: datetime):
    # terça de pagamento paga a semana ANTERIOR (terça→segunda)
    inicio = terca_pagto - timedelta(days=7)  # terça anterior
    fim = inicio + timedelta(days=6)          # segunda
    return inicio, fim

def make_refid(row: pd.Series) -> str:
    key = "|".join([
        str(row.get("Cliente", "")).strip(),
        str(row.get("Data", "")).strip(),
        str(row.get("Serviço", "")).strip(),
        str(row.get("Valor", "")).strip(),
        str(row.get("Funcionário", "")).strip(),
        str(row.get("Combo", "")).strip(),
    ])
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]

def garantir_colunas(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    return df

def s_lower(s):
    return s.astype(str).str.strip().str.lower()

def is_cartao(conta: str) -> bool:
    c = (conta or "").strip().lower()
    padrao = r"(cart|cart[ãa]o|cr[eé]dito|d[eé]bito|maquin|pos)"
    return bool(re.search(padrao, c))

def snap_para_preco_cheio(servico: str, valor: float, tol: float, habilitado: bool) -> float:
    """
    Se habilitado, tenta 'grudar' o valor no preço CHEIO da TABELA do serviço,
    desde que esteja dentro da tolerância.
    """
    if not habilitado:
        return valor
    cheio = VALOR_TABELA.get((servico or "").strip())
    if isinstance(cheio, (int, float)) and abs(valor - float(cheio)) <= tol:
        return float(cheio)
    return valor

def format_brl(v: float) -> str:
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# =============================
# TELEGRAM
# =============================
TELEGRAM_TOKEN = st.secrets.get("TELEGRAM_TOKEN", "").strip()
CHAT_ID_JPAULO = st.secrets.get("TELEGRAM_CHAT_ID_JPAULO", "").strip()
CHAT_ID_DANIELA = st.secrets.get("TELEGRAM_CHAT_ID_DANIELA", "").strip()

def send_telegram(chat_id: str, text: str):
    if not TELEGRAM_TOKEN or not chat_id:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        requests.post(url, data=payload, timeout=15)
    except Exception as e:
        st.warning(f"Falha ao enviar Telegram: {e}")

# =============================
# UI
# =============================
st.set_page_config(layout="wide")
st.title(f"💇‍♀️ Pagamento de Comissão — {FUNCIONARIA} (1 linha por DIA do atendimento)")

# Carrega base
base = _read_df(ABA_DADOS)
base = garantir_colunas(base, COLS_OFICIAIS).copy()

# Carrega % salvos (para pré-preencher por serviço)
PERC_SALVOS = _read_config()  # dict {serviço: %}

# Inputs (linha 1)
colA, colB, colC = st.columns([1,1,1])
with colA:
    hoje = br_now()
    if hoje.weekday() == 1:  # terça
        sugestao_terca = hoje
    else:
        delta = (1 - hoje.weekday()) % 7
        if delta == 0:
            delta = 7
        sugestao_terca = (hoje + timedelta(days=delta))
    terca_pagto = st.date_input("🗓️ Terça do pagamento", value=sugestao_terca.date())
    terca_pagto = datetime.combine(terca_pagto, datetime.min.time())

with colB:
    perc_padrao = st.number_input("Percentual padrão da comissão (%)", value=PERCENTUAL_PADRAO, step=1.0)

with colC:
    incluir_produtos = st.checkbox("Incluir PRODUTOS?", value=False)

# Inputs (linha 2)
meio_pag = st.selectbox("Meio de pagamento (para DESPESAS)", ["Dinheiro", "Pix", "Cartão", "Transferência"], index=0)
descricao_padrao = st.text_input("Descrição (para DESPESAS)", value=f"Comissão {FUNCIONARIA}")

# Inputs (linha 3) — regras de cálculo
usar_tabela_cartao = st.checkbox(
    "Usar preço de TABELA para comissão quando pago no cartão",
    value=True,
    help="Ignora o valor líquido (com taxa) e comissiona pelo preço de tabela do serviço."
)
col_r1, col_r2 = st.columns([2,1])
with col_r1:
    arred_cheio = st.checkbox(
        "Arredondar para preço cheio de TABELA (tolerância abaixo)",
        value=True,
        help="Ex.: 33,00 / 34,75 / 35,10 → 35,00 (se dentro da tolerância)."
    )
with col_r2:
    tol_reais = st.number_input("Tolerância (R$)", value=2.00, step=0.50, min_value=0.0)

# ✅ Reprocessar esta terça (limpa/ignora cache desta terça)
reprocessar_terca = st.checkbox(
    "Reprocessar esta terça (regravar): ignorar/limpar cache desta terça antes de salvar",
    value=False,
    help="Marque se você apagou as linhas em Despesas e quer gravar novamente esta terça."
)

# Notificações
colN1, colN2 = st.columns(2)
with colN1:
    notificar_jpaulo = st.checkbox("Enviar Telegram para JPaulo", value=True)
with colN2:
    notificar_daniela = st.checkbox("Enviar Telegram para Daniela", value=True)

# Conjunto da FUNCIONÁRIA
dfv = base[s_lower(base["Funcionário"]) == FUNCIONARIA.lower()].copy()
if not incluir_produtos:
    dfv = dfv[s_lower(dfv["Tipo"]) == "serviço"]
dfv["_dt_serv"] = dfv["Data"].apply(parse_br_date)

# Janela terça→segunda (anterior à terça de pagamento)
ini, fim = janela_terca_a_segunda(terca_pagto)
st.info(f"Janela desta folha: **{to_br_date(ini)} a {to_br_date(fim)}** (terça→segunda)")

# 1) Itens da SEMANA NÃO FIADO
mask_semana = (
    (dfv["_dt_serv"].notna()) &
    (dfv["_dt_serv"] >= ini) &
    (dfv["_dt_serv"] <= fim) &
    ((s_lower(dfv["StatusFiado"]) == "") | (s_lower(dfv["StatusFiado"]) == "nao"))
)
semana_df = dfv[mask_semana].copy()

# 2) Fiados liberados até a terça (independe da data do serviço)
df_fiados = dfv[(s_lower(dfv["StatusFiado"]) != "") | (s_lower(dfv["IDLancFiado"]) != "")]
df_fiados["_dt_pagto"] = df_fiados["DataPagamento"].apply(parse_br_date)
fiados_liberados = df_fiados[(df_fiados["_dt_pagto"].notna()) & (df_fiados["_dt_pagto"] <= terca_pagto)].copy()

# 3) NOVO BLOCO — Fiados pendentes (histórico, ainda não pagos)
fiados_pendentes = df_fiados[(df_fiados["_dt_pagto"].isna()) | (df_fiados["_dt_pagto"] > terca_pagto)].copy()

# Cache de comissões já pagas (por RefID) — segregado por funcionária
cache = _read_df(ABA_COMISSOES_CACHE)
cache_cols = ["RefID", "Funcionario", "PagoEm", "TerçaPagamento", "ValorComissao", "Competencia", "Observacao"]
cache = garantir_colunas(cache, cache_cols)

terca_str = to_br_date(terca_pagto)
if reprocessar_terca:
    # ignora no cache SOMENTE os desta terça da Daniela (para reprocessar)
    mask_keep = ~((cache["TerçaPagamento"] == terca_str) & (s_lower(cache["Funcionario"]) == FUNCIONARIA.lower()))
    ja_pagos = set(cache[mask_keep]["RefID"].astype(str).tolist())
else:
    # considera pagos apenas os da Daniela
    ja_pagos = set(cache[s_lower(cache["Funcionario"]) == FUNCIONARIA.lower()]["RefID"].astype(str).tolist())

# Função base de cálculo
def montar_valor_base(df):
    if df.empty:
        df["Valor_num"] = []
        df["Competência"] = []
        df["Valor_base_comissao"] = []
        return df
    df["Valor_num"] = pd.to_numeric(df["Valor"], errors="coerce").fillna(0.0)
    df["Competência"] = df["Data"].apply(competencia_from_data_str)

    def _base_valor(row):
        serv = str(row.get("Serviço", "")).strip()
        conta = str(row.get("Conta", "")).strip()
        bruto = float(row.get("Valor_num", 0.0))
        if usar_tabela_cartao and is_cartao(conta):
            return float(VALOR_TABELA.get(serv, bruto))
        return snap_para_preco_cheio(serv, bruto, tol_reais, arred_cheio)

    df["Valor_base_comissao"] = df.apply(_base_valor, axis=1)
    return df

# ------- GRADE EDITÁVEL: semana e fiados liberados -------
def preparar_grid(df: pd.DataFrame, titulo: str, key_prefix: str):
    if df.empty:
        st.warning(f"Sem itens em **{titulo}**.")
        # sempre retorna 3 valores
        return pd.DataFrame(), 0.0, pd.DataFrame()

    df = df.copy()
    df["RefID"] = df.apply(make_refid, axis=1)
    df = df[~df["RefID"].isin(ja_pagos)]
    if df.empty:
        st.info(f"Todos os itens de **{titulo}** já foram pagos.")
        return pd.DataFrame(), 0.0, pd.DataFrame()

    df = montar_valor_base(df)

    st.subheader(titulo)
    st.caption("Edite a % de comissão por linha, se precisar.")

    # inclui Conta para aparecer na mensagem do Telegram
    ed_cols = ["Data", "Cliente", "Serviço", "Conta", "Valor_base_comissao", "Competência", "RefID"]
    ed = df[ed_cols].rename(columns={"Valor_base_comissao": "Valor (para comissão)"})
    # %: se houver salvo por serviço, usa; senão, o padrão
    def _perc_por_serv(srv):
        s = str(srv).strip()
        return float(PERC_SALVOS.get(s, perc_padrao))
    ed["% Comissão"] = ed["Serviço"].apply(_perc_por_serv)
    ed["Comissão (R$)"] = (ed["Valor (para comissão)"] * ed["% Comissão"] / 100.0).round(2)
    ed = ed.reset_index(drop=True)

    edited = st.data_editor(
        ed,
        key=f"editor_{key_prefix}",
        num_rows="fixed",
        column_config={
            "Valor (para comissão)": st.column_config.NumberColumn(format="R$ %.2f"),
            "% Comissão": st.column_config.NumberColumn(format="%.1f %%", min_value=0.0, max_value=100.0, step=0.5),
            "Comissão (R$)": st.column_config.NumberColumn(format="R$ %.2f"),
        },
        use_container_width=True
    )

    # Recalcula comissão se % mudou
    edited["Comissão (R$)"] = (
        pd.to_numeric(edited["Valor (para comissão)"], errors="coerce").fillna(0.0) *
        pd.to_numeric(edited["% Comissão"], errors="coerce").fillna(0.0) / 100.0
    ).round(2)

    total = float(edited["Comissão (R$)"].sum())
    merged = df.merge(edited[["RefID", "% Comissão", "Comissão (R$)", "Valor (para comissão)"]], on="RefID", how="left")
    merged["PercComissao"] = pd.to_numeric(merged["% Comissão"], errors="coerce").fillna(0.0)
    merged["ComissaoValor"] = pd.to_numeric(merged["Comissão (R$)"], errors="coerce").fillna(0.0)

    st.success(f"Total de comissão em **{titulo}**: {format_brl(total)}")
    return merged, total, edited  # merged p/ salvar, edited p/ Telegram

semana_grid, total_semana, vis_semana = preparar_grid(semana_df, "Semana (terça→segunda) — NÃO FIADO", "semana")

fiados_liberados_grid, total_fiados, vis_fiados = preparar_grid(
    fiados_liberados, "Fiados liberados (pagos até a terça)", "fiados_liberados"
)

# ------- NOVO: TABELA (somente leitura) — FIADOS A RECEBER -------
st.subheader("📌 Fiados a receber (histórico — ainda NÃO pagos)")
if fiados_pendentes.empty:
    st.info("Nenhum fiado pendente no momento.")
    total_fiados_pend = 0.0
else:
    fiados_pendentes = montar_valor_base(fiados_pendentes)
    vis = fiados_pendentes[["Data", "Cliente", "Serviço", "Valor", "Valor_base_comissao"]].rename(
        columns={"Valor_base_comissao": "Valor (para comissão)"}
    ).copy()
    vis["% Comissão"] = PERCENTUAL_PADRAO
    vis["Comissão (R$)"] = (pd.to_numeric(vis["Valor (para comissão)"], errors="coerce").fillna(0.0) * vis["% Comissão"] / 100.0).round(2)
    total_fiados_pend = float(vis["Comissão (R$)"].sum())

    st.dataframe(
        vis.sort_values(by=["Data", "Cliente"]).reset_index(drop=True),
        use_container_width=True
    )
    st.warning(f"Comissão futura (quando pagarem): **{format_brl(total_fiados_pend)}**")

# ------- RESUMO DE MÉTRICAS -------
col_m1, col_m2, col_m3, col_m4 = st.columns(4)
with col_m1:
    st.metric("Nesta terça — NÃO fiado", format_brl(total_semana))
with col_m2:
    st.metric("Nesta terça — fiados liberados", format_brl(total_fiados))
with col_m3:
    st.metric("Total desta terça", format_brl(total_semana + total_fiados))
with col_m4:
    st.metric("Fiados pendentes (futuro)", format_brl(total_fiados_pend))

# =============================
# CONFIRMAR E GRAVAR
# =============================
if st.button("✅ Registrar comissão (por DIA do atendimento) e marcar itens como pagos"):
    if (semana_grid is None or semana_grid.empty) and (fiados_liberados_grid is None or fiados_liberados_grid.empty):
        st.warning("Não há itens para pagar.")
    else:
        hoje_str = to_br_date(br_now())
        # 1) Atualiza cache item a item (para não pagar duas vezes)
        novos_cache = []
        for df_part in [semana_grid, fiados_liberados_grid]:
            if df_part is None or df_part.empty:
                continue
            for _, r in df_part.iterrows():
                novos_cache.append({
                    "RefID": r["RefID"],
                    "Funcionario": FUNCIONARIA,
                    "PagoEm": hoje_str,
                    "TerçaPagamento": to_br_date(terca_pagto),
                    "ValorComissao": f'{float(r["ComissaoValor"]):.2f}'.replace(".", ","),
                    "Competencia": r.get("Competência", ""),
                    "Observacao": f'{r.get("Cliente","")} | {r.get("Serviço","")} | {r.get("Data","")}',
                })

        cache_df = _read_df(ABA_COMISSOES_CACHE)
        cache_df = garantir_colunas(cache_df, cache_cols)

        if reprocessar_terca:
            # remove desta terça apenas da Daniela
            mask_keep = ~((cache_df["TerçaPagamento"] == to_br_date(terca_pagto)) &
                          (s_lower(cache_df["Funcionario"]) == FUNCIONARIA.lower()))
            cache_df = cache_df[mask_keep].copy()

        cache_upd = pd.concat([cache_df[cache_cols], pd.DataFrame(novos_cache)], ignore_index=True)
        _write_df(ABA_COMISSOES_CACHE, cache_upd)

        # 2) Lança em DESPESAS: UMA LINHA POR DIA DO ATENDIMENTO (na aba Despesas Feminino)
        despesas_df = _read_df(ABA_DESPESAS_FEM)
        despesas_df = garantir_colunas(despesas_df, COLS_DESPESAS_FIX)
        for c in COLS_DESPESAS_FIX:
            if c not in despesas_df.columns:
                despesas_df[c] = ""

        pagaveis = []
        for df_part in [semana_grid, fiados_liberados_grid]:
            if df_part is None or df_part.empty:
                continue
            pagaveis.append(df_part[["Data", "Competência", "ComissaoValor"]].copy())

        linhas_adicionadas = 0
        if pagaveis:
            pagos = pd.concat(pagaveis, ignore_index=True)

            def _norm_dt(s):
                s = (s or "").strip()
                for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
                    try:
                        return datetime.strptime(s, fmt)
                    except Exception:
                        pass
                return None

            pagos["_dt"] = pagos["Data"].apply(_norm_dt)
            pagos = pagos[pagos["_dt"].notna()].copy()

            por_dia = pagos.groupby(["Data", "Competência"], dropna=False)["ComissaoValor"].sum().reset_index()

            linhas = []
            for _, row in por_dia.iterrows():
                data_serv = str(row["Data"]).strip()
                comp      = str(row["Competência"]).strip()
                val       = float(row["ComissaoValor"])
                linhas.append({
                    "Data": data_serv,
                    "Prestador": FUNCIONARIA,
                    "Descrição": f"{descricao_padrao} — Comp {comp} — Pago em {hoje_str}",
                    "Valor": f'R$ {val:.2f}'.replace(".", ","),
                    "Me Pag:": meio_pag
                })

            despesas_final = pd.concat([despesas_df, pd.DataFrame(linhas)], ignore_index=True)
            colunas_finais = [c for c in COLS_DESPESAS_FIX if c in despesas_final.columns] + \
                             [c for c in despesas_final.columns if c not in COLS_DESPESAS_FIX]
            despesas_final = despesas_final[colunas_finais]
            _write_df(ABA_DESPESAS_FEM, despesas_final)
            linhas_adicionadas = len(linhas)

        # 3) Persistir % por serviço (o último % aplicado por serviço nas grades)
        perc_atualizados = dict(PERC_SALVOS)

        def _coleta_percentuais(df_vis):
            """df_vis é o 'edited' retornado do editor, contém % por linha já ajustada."""
            out = {}
            if df_vis is None or df_vis.empty:
                return out
            tmp = df_vis[["Serviço", "% Comissão"]].copy()
            tmp["Serviço"] = tmp["Serviço"].astype(str).str.strip()
            for _, r in tmp.iterrows():
                s = r.get("Serviço", "")
                try:
                    p = float(str(r.get("% Comissão","")).replace(",", "."))
                except:
                    p = None
                if s and p is not None:
                    out[s] = p
            return out

        for m in (_coleta_percentuais(vis_semana), _coleta_percentuais(vis_fiados)):
            perc_atualizados.update(m)

        _write_config(perc_atualizados)

        # 4) Telegram — mensagem detalhada (uma cópia para cada)
        def build_msg(titulo: str, vis_df: pd.DataFrame) -> str:
            if vis_df is None or vis_df.empty:
                return ""
            linhas_txt = []
            for _, r in vis_df.iterrows():
                dt = str(r.get("Data","")).strip()
                cli = str(r.get("Cliente","")).strip()
                srv = str(r.get("Serviço","")).strip()
                conta = str(r.get("Conta","")).strip()
                try:
                    comi = float(pd.to_numeric(r.get("Comissão (R$)","0"), errors="coerce") or 0.0)
                except:
                    comi = 0.0
                linhas_txt.append(f"• {dt} | {cli} — {srv} | <i>{conta}</i>\n   Comissão: <b>{format_brl(comi)}</b>")
            subtotal = float(pd.to_numeric(vis_df["Comissão (R$)"], errors="coerce").fillna(0.0).sum())
            bloco = f"<b>{titulo}</b>\n" + "\n".join(linhas_txt) + f"\n<b>Subtotal:</b> {format_brl(subtotal)}\n"
            return bloco

        mensagem = f"<b>Comissão — {FUNCIONARIA}</b>\nData: {hoje_str}\n\n"
        msg_semana = build_msg("Não fiado (pagos agora)", vis_semana)
        msg_fiado  = build_msg("Fiados liberados (pagos agora)", vis_fiados)
        if msg_semana: mensagem += msg_semana + "\n"
        if msg_fiado:  mensagem += msg_fiado  + "\n"
        total_exec = float((vis_semana["Comissão (R$)"].sum() if not vis_semana.empty else 0.0) +
                           (vis_fiados["Comissão (R$)"].sum() if not vis_fiados.empty else 0.0))
        mensagem += "<b>Total geral desta execução:</b> " + format_brl(total_exec)

        if TELEGRAM_TOKEN:
            if notificar_jpaulo and CHAT_ID_JPAULO:
                send_telegram(CHAT_ID_JPAULO, mensagem)
            if notificar_daniela and CHAT_ID_DANIELA:
                send_telegram(CHAT_ID_DANIELA, mensagem)

        st.success(
            f"🎉 Comissão registrada! {linhas_adicionadas} linha(s) adicionada(s) em **{ABA_DESPESAS_FEM}** "
            f"(uma por DIA do atendimento) e {len(novos_cache)} itens marcados no **{ABA_COMISSOES_CACHE}**. "
            f"Total desta execução: {format_brl(total_exec)}"
        )
        st.balloons()
    # fim do else
# fim do botão
