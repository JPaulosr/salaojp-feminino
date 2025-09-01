# -*- coding: utf-8 -*-
# 12_Comissoes_Daniela.py — Paga TUDO não pago, arredonda base para múltiplo de 5 (cima), 1 linha por DIA do atendimento

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
from math import ceil

# =============================
# CONFIG BÁSICA
# =============================
SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"

# Abas
ABA_DADOS = "Base de Dados Feminino"
ABA_COMISSOES_CACHE = "comissoes_cache_feminino"
ABA_DESPESAS_FEM = "Despesas Feminino"          # onde lança as despesas de comissão (1 linha por DIA)
ABA_CONFIG = "config_comissoes_feminino"        # onde persistimos % por serviço

# Identidade
TZ = "America/Sao_Paulo"
FUNCIONARIA = "Daniela"  # alvo desta folha

# Colunas esperadas na Base de Dados
COLS_OFICIAIS = [
    "Data", "Serviço", "Valor", "Conta", "Cliente", "Combo",
    "Funcionário", "Fase", "Tipo", "Período",
    # Fiado
    "StatusFiado", "IDLancFiado", "VencimentoFiado", "DataPagamento"
]

# Colunas da aba Despesas
COLS_DESPESAS_FIX = ["Data", "Prestador", "Descrição", "Valor", "Me Pag:"]

# Percentual padrão (fallback, caso não exista % salvo para o serviço)
PERCENTUAL_PADRAO = 50.0

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

def arredonda_para_cima_mult5(v: float) -> float:
    """Arredonda para cima ao múltiplo de 5 (14,25->15; 33,25->35)."""
    try:
        v = float(v)
    except:
        return 0.0
    return float(ceil(v / 5.0) * 5.0)

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
st.title(f"💇‍♀️ Comissão — {FUNCIONARIA} (paga TUDO não pago • valor-base arredondado)")

# Carrega base e % salvos
base = _read_df(ABA_DADOS)
base = garantir_colunas(base, COLS_OFICIAIS).copy()
PERC_SALVOS = _read_config()  # dict {serviço: %}

# Inputs
colA, colB, colC = st.columns([1,1,1])
with colA:
    perc_padrao = st.number_input("Percentual padrão da comissão (%)", value=PERCENTUAL_PADRAO, step=1.0, min_value=0.0, max_value=100.0)
with colB:
    incluir_produtos = st.checkbox("Incluir PRODUTOS?", value=False)
with colC:
    meio_pag = st.selectbox("Meio de pagamento (para DESPESAS)", ["Dinheiro", "Pix", "Cartão", "Transferência"], index=0)

descricao_padrao = st.text_input("Descrição (para DESPESAS)", value=f"Comissão {FUNCIONARIA}")

# Notificações
colN1, colN2 = st.columns(2)
with colN1:
    notificar_jpaulo = st.checkbox("Enviar Telegram para JPaulo", value=True)
with colN2:
    notificar_daniela = st.checkbox("Enviar Telegram para Daniela", value=True)

# Filtra só a funcionária e só serviços (se marcado)
dfv = base[s_lower(base["Funcionário"]) == FUNCIONARIA.lower()].copy()
if not incluir_produtos:
    dfv = dfv[s_lower(dfv["Tipo"]) == "serviço"]

# Parse datas e gera RefID
dfv["_dt_serv"] = dfv["Data"].apply(parse_br_date)
dfv["RefID"] = dfv.apply(make_refid, axis=1)

# Cache de pagos desta funcionária
cache = _read_df(ABA_COMISSOES_CACHE)
cache_cols = ["RefID", "Funcionario", "PagoEm", "TerçaPagamento", "ValorComissao", "Competencia", "Observacao"]
cache = garantir_colunas(cache, cache_cols)
ja_pagos = set(cache[s_lower(cache["Funcionario"]) == FUNCIONARIA.lower()]["RefID"].astype(str).tolist())

# Seleção dos itens a pagar: TUDO que nunca foi pago + fiados liberados até HOJE
hoje = br_now()
df_fiados = dfv[(s_lower(dfv["StatusFiado"]) != "") | (s_lower(dfv["IDLancFiado"]) != "")]
df_fiados["_dt_pagto"] = df_fiados["DataPagamento"].apply(parse_br_date)

nao_fiado = dfv[( (s_lower(dfv["StatusFiado"]) == "") | (s_lower(dfv["StatusFiado"]) == "nao") )].copy()
fiados_liberados = df_fiados[(df_fiados["_dt_pagto"].notna()) & (df_fiados["_dt_pagto"] <= hoje)].copy()

# Remove já pagos via RefID
if ja_pagos:
    nao_fiado = nao_fiado[~nao_fiado["RefID"].isin(ja_pagos)].copy()
    fiados_liberados = fiados_liberados[~fiados_liberados["RefID"].isin(ja_pagos)].copy()

st.info("Modo: pagando <b>TUDO</b> que ainda não foi pago — Não fiado + Fiados com DataPagamento ≤ hoje.", icon="💸")

# ====== Montagem da base (valor-base sempre arredondado ao múltiplo de 5 para cima) ======
def montar_valor_base(df: pd.DataFrame):
    if df.empty:
        df["Valor_num"] = []
        df["Competência"] = []
        df["Valor_base_comissao"] = []
        return df
    df["Valor_num"] = pd.to_numeric(df["Valor"], errors="coerce").fillna(0.0)
    df["Competência"] = df["Data"].apply(competencia_from_data_str)

    def _base_valor(row):
        bruto = float(row.get("Valor_num", 0.0))
        return arredonda_para_cima_mult5(bruto)

    df["Valor_base_comissao"] = df.apply(_base_valor, axis=1)
    return df

nao_fiado = montar_valor_base(nao_fiado)
fiados_liberados = montar_valor_base(fiados_liberados)

# ====== Editor ======
def preparar_grid(df: pd.DataFrame, titulo: str, key_prefix: str):
    if df.empty:
        st.warning(f"Sem itens em **{titulo}**.")
        return pd.DataFrame(), 0.0, pd.DataFrame()

    df = df.copy()
    ed_cols = ["Data", "Cliente", "Serviço", "Conta", "Valor_base_comissao", "Competência", "RefID"]
    ed = df[ed_cols].rename(columns={"Valor_base_comissao": "Valor (para comissão)"})

    # %: usa salvo por serviço, senão o padrão
    def _perc_por_serv(srv):
        s = str(srv).strip()
        return float(PERC_SALVOS.get(s, perc_padrao))
    ed["% Comissão"] = ed["Serviço"].apply(_perc_por_serv)

    # Comissão (calc inicial)
    ed["Comissão (R$)"] = (
        pd.to_numeric(ed["Valor (para comissão)"], errors="coerce").fillna(0.0) *
        pd.to_numeric(ed["% Comissão"], errors="coerce").fillna(0.0) / 100.0
    ).round(2)
    ed = ed.reset_index(drop=True)

    edited = st.data_editor(
        ed,
        key=f"editor_{key_prefix}",
        num_rows="fixed",
        column_config={
            "Valor (para comissão)": st.column_config.NumberColumn(format="R$ %.2f"),
            "% Comissão": st.column_config.NumberColumn(format="%.1f %%", min_value=0.0, max_value=100.0, step=0.5),
            "Comissão (R$)": st.column_config.NumberColumn(format="R$ %.2f", disabled=True),  # sempre calculada
        },
        use_container_width=True
    )

    # 1) Aplica arredondamento ao múltiplo de 5 em qualquer edição manual do valor
    edited["Valor (para comissão)"] = edited["Valor (para comissão)"].apply(
        lambda x: arredonda_para_cima_mult5(float(pd.to_numeric(x, errors="coerce") or 0.0))
    )
    # 2) Recalcula comissão
    edited["Comissão (R$)"] = (
        pd.to_numeric(edited["Valor (para comissão)"], errors="coerce").fillna(0.0) *
        pd.to_numeric(edited["% Comissão"], errors="coerce").fillna(0.0) / 100.0
    ).round(2)

    total = float(edited["Comissão (R$)"].sum())

    # Merge de volta para ter infos originais + editadas
    merged = df.merge(
        edited[["RefID", "Valor (para comissão)", "% Comissão", "Comissão (R$)"]],
        on="RefID", how="left"
    )
    merged["ValorBaseEditado"] = pd.to_numeric(merged["Valor (para comissão)"], errors="coerce").fillna(0.0)
    merged["PercComissao"] = pd.to_numeric(merged["% Comissão"], errors="coerce").fillna(0.0)
    merged["ComissaoValor"] = pd.to_numeric(merged["Comissão (R$)"], errors="coerce").fillna(0.0)

    st.success(f"Total em **{titulo}**: {format_brl(total)}")
    return merged, total, edited  # merged p/ salvar; edited p/ Telegram

grid_nao_fiado, total_nao_fiado, vis_nao_fiado = preparar_grid(nao_fiado, "Não fiado (a pagar)", "nao_fiado")
grid_fiado, total_fiado, vis_fiado = preparar_grid(fiados_liberados, "Fiados liberados (a pagar)", "fiado_lib")

total_geral = float(total_nao_fiado + total_fiado)
st.metric("💰 Total desta execução", format_brl(total_geral))

# =============================
# CONFIRMAR E GRAVAR
# =============================
if st.button("✅ Registrar comissão (por DIA do atendimento), marcar como pago e enviar Telegram"):
    if (grid_nao_fiado is None or grid_nao_fiado.empty) and (grid_fiado is None or grid_fiado.empty):
        st.warning("Não há itens para pagar.")
    else:
        hoje_str = to_br_date(br_now())

        # 1) Atualiza cache (para não pagar duas vezes)
        novos_cache = []
        for df_part in [grid_nao_fiado, grid_fiado]:
            if df_part is None or df_part.empty:
                continue
            for _, r in df_part.iterrows():
                novos_cache.append({
                    "RefID": r["RefID"],
                    "Funcionario": FUNCIONARIA,
                    "PagoEm": hoje_str,
                    "TerçaPagamento": "",  # não usamos mais a janela de terça
                    "ValorComissao": f'{float(r["ComissaoValor"]):.2f}'.replace(".", ","),
                    "Competencia": r.get("Competência", ""),
                    "Observacao": f'{r.get("Cliente","")} | {r.get("Serviço","")} | {r.get("Data","")}',
                })

        cache_df = _read_df(ABA_COMISSOES_CACHE)
        cache_df = garantir_colunas(cache_df, cache_cols)
        cache_upd = pd.concat([cache_df[cache_cols], pd.DataFrame(novos_cache)], ignore_index=True)
        _write_df(ABA_COMISSOES_CACHE, cache_upd)

        # 2) Lança em DESPESAS FEMININO: UMA LINHA POR DIA DO ATENDIMENTO
        despesas_df = _read_df(ABA_DESPESAS_FEM)
        despesas_df = garantir_colunas(despesas_df, COLS_DESPESAS_FIX)
        for c in COLS_DESPESAS_FIX:
            if c not in despesas_df.columns:
                despesas_df[c] = ""

        pagaveis = []
        for df_part in [grid_nao_fiado, grid_fiado]:
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

        # 3) Persistir % por serviço (últimos usados na grade)
        perc_atualizados = dict(PERC_SALVOS)

        def _coleta_percentuais(df_vis):
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

        for m in (_coleta_percentuais(vis_nao_fiado), _coleta_percentuais(vis_fiado)):
            perc_atualizados.update(m)

        _write_config(perc_atualizados)

        # 4) Telegram — mensagem detalhada (cliente + serviço + forma + comissão) e total
        def build_msg(titulo: str, vis_df: pd.DataFrame) -> str:
            if vis_df is None or vis_df.empty:
                return ""
            linhas_txt = []
            for _, r in vis_df.iterrows():
                dt = str(r.get("Data","")).strip()
                cli = str(r.get("Cliente","")).strip()
                srv = str(r.get("Serviço","")).strip()
                conta = str(r.get("Conta","")).strip()
                comi = float(pd.to_numeric(r.get("Comissão (R$)","0"), errors="coerce") or 0.0)
                linhas_txt.append(f"• {dt} | {cli} — {srv} | <i>{conta}</i>\n   Comissão: <b>{format_brl(comi)}</b>")
            subtotal = float(pd.to_numeric(vis_df["Comissão (R$)"], errors="coerce").fillna(0.0).sum())
            bloco = f"<b>{titulo}</b>\n" + "\n".join(linhas_txt) + f"\n<b>Subtotal:</b> {format_brl(subtotal)}\n"
            return bloco

        mensagem = f"<b>Comissão — {FUNCIONARIA}</b>\nData: {hoje_str}\n\n"
        msg1 = build_msg("Não fiado (pagos agora)", vis_nao_fiado)
        msg2 = build_msg("Fiados liberados (pagos agora)", vis_fiado)
        if msg1: mensagem += msg1 + "\n"
        if msg2: mensagem += msg2 + "\n"
        total_exec = float((vis_nao_fiado["Comissão (R$)"].sum() if not vis_nao_fiado.empty else 0.0) +
                           (vis_fiado["Comissão (R$)"].sum() if not vis_fiado.empty else 0.0))
        mensagem += "<b>Total geral desta execução:</b> " + format_brl(total_exec)

        if TELEGRAM_TOKEN:
            if notificar_jpaulo and CHAT_ID_JPAULO:
                send_telegram(CHAT_ID_JPAULO, mensagem)
            if notificar_daniela and CHAT_ID_DANIELA:
                send_telegram(CHAT_ID_DANIELA, mensagem)

        st.success(
            f"🎉 Comissão registrada! {linhas_adicionadas} linha(s) adicionada(s) em **{ABA_DESPESAS_FEM}** "
            f"(uma por DIA do atendimento) e {len(novos_cache)} item(ns) marcados no **{ABA_COMISSOES_CACHE}**. "
            f"Total desta execução: {format_brl(total_exec)}"
        )
        st.balloons()
