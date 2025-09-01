# -*- coding: utf-8 -*-
# 12_Comissoes_Daniela.py — Pagamento de comissão (PAGA TUDO que ainda não foi pago)
# Regras principais:
# - Paga TUDO que ainda não foi pago para a funcionária-alvo (independente de terça).
# - Fiado só entra quando DataPagamento <= hoje.
# - Em Despesas grava UMA LINHA POR DIA DO ATENDIMENTO (Data = data do serviço).
# - Evita duplicidades via sheet "comissoes_cache_feminino" com RefID por atendimento.
# - Mantém opção de "usar preço de tabela para cartão" e "arredondar até a tolerância".
# - Percentuais por serviço configuráveis na UI + editáveis por linha.
# - Mensagem detalhada no Telegram: conta, valor base, %, comissão por item e totais.

import streamlit as st
import pandas as pd
import gspread
import hashlib
import re
import json
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
ABA_DESPESAS = "Despesas"

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

# Percentual padrão da comissão (fallback)
PERCENTUAL_PADRAO = 50.0

# Percentuais por serviço (padrão) — você pode ajustar na UI
PERC_SERVICO_DEFAULT = {
    "Corte": 50.0,
    "Escova": 50.0,
    "Unha mão": 50.0,
    "Unha pé": 50.0,
    "Sobrancelhas": 50.0,
    "Designer de Henna": 50.0,
    "Manicure": 50.0,
    "Pedicure": 50.0,
    "Progressiva": 40.0,   # exemplo: diferente de 50%
}

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

def is_cartao(conta: str) -> bool:
    c = (conta or "").strip().lower()
    padrao = r"(cart|cart[ãa]o|cr[eé]dito|d[eé]bito|maquin|pos)"
    return bool(re.search(padrao, c))

def snap_para_preco_cheio(servico: str, valor: float, tol: float, habilitado: bool) -> float:
    if not habilitado:
        return valor
    cheio = VALOR_TABELA.get((servico or "").strip())
    if isinstance(cheio, (int, float)) and abs(valor - float(cheio)) <= tol:
        return float(cheio)
    return valor

def format_brl(v: float) -> str:
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# =============================
# UI
# =============================
st.set_page_config(layout="wide")
st.title(f"💇‍♀️ Pagamento de Comissão — {FUNCIONARIA} (Paga TUDO não pago)")

# --- Carrega base
base = _read_df(ABA_DADOS)
base = garantir_colunas(base, COLS_OFICIAIS).copy()

# --- Filtros/Opções
colA, colB, colC, colD = st.columns([1,1,1,1])
with colA:
    perc_padrao = st.number_input("Percentual padrão (%)", value=PERCENTUAL_PADRAO, step=1.0, min_value=0.0, max_value=100.0)
with colB:
    incluir_produtos = st.checkbox("Incluir PRODUTOS?", value=False)
with colC:
    usar_tabela_cartao = st.checkbox(
        "Comissionar pelo preço de TABELA quando pago no cartão",
        value=True,
        help="Ignora líquido com taxa e usa tabela do serviço para base."
    )
with colD:
    arred_cheio = st.checkbox(
        "Arredondar para preço cheio de TABELA (tolerância)",
        value=True
    )

colE, colF = st.columns([1,1])
with colE:
    tol_reais = st.number_input("Tolerância (R$)", value=2.00, step=0.50, min_value=0.0)
with colF:
    pagar_tudo_ate_hoje = st.checkbox("Pagar TUDO não pago até hoje (ignorar janela de terça)", value=True)

# --- Forma e descrição para DESPESAS
meio_pag = st.selectbox("Meio de pagamento (para DESPESAS)", ["Dinheiro", "Pix", "Cartão", "Transferência"], index=0)
descricao_padrao = st.text_input("Descrição (para DESPESAS)", value=f"Comissão {FUNCIONARIA}")

# --- Percentuais por Serviço (configuráveis)
st.subheader("Percentuais por Serviço (padrões)")
perc_serv_df = pd.DataFrame(
    [(k, v) for k, v in PERC_SERVICO_DEFAULT.items()],
    columns=["Serviço", "% Padrão"]
)
perc_serv_edit = st.data_editor(
    perc_serv_df,
    key="editor_perc_serv",
    num_rows="dynamic",
    column_config={
        "% Padrão": st.column_config.NumberColumn(format="%.1f %%", min_value=0.0, max_value=100.0, step=0.5),
    },
    use_container_width=True
)
PERC_SERVICO = {str(r["Serviço"]).strip(): float(r["% Padrão"]) for _, r in perc_serv_edit.iterrows() if str(r["Serviço"]).strip() != ""}

# --- Telegram
st.subheader("Notificação Telegram")
TELEGRAM_TOKEN = st.secrets.get("TELEGRAM_TOKEN", "").strip()
CHAT_ID_JPAULO = st.secrets.get("TELEGRAM_CHAT_ID_JPAULO", "").strip()
CHAT_ID_MEIRE  = st.secrets.get("TELEGRAM_CHAT_ID_MEIRE", "").strip()
notificar_jpaulo = st.checkbox("Enviar mensagem para JPaulo (recomendado)", value=True)
notificar_meire  = st.checkbox("Enviar mensagem para Meire", value=False)

def send_telegram(chat_id: str, text: str):
    try:
        if TELEGRAM_TOKEN and chat_id:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
            requests.post(url, data=payload, timeout=15)
    except Exception as e:
        st.warning(f"Falha ao enviar Telegram: {e}")

# --- Conjunto da FUNCIONÁRIA
dfv = base[s_lower(base["Funcionário"]) == FUNCIONARIA.lower()].copy()
if not incluir_produtos:
    dfv = dfv[s_lower(dfv["Tipo"]) == "serviço"]
dfv["_dt_serv"] = dfv["Data"].apply(parse_br_date)

# --- Fiados
df_fiados = dfv[(s_lower(dfv["StatusFiado"]) != "") | (s_lower(dfv["IDLancFiado"]) != "")]
df_fiados["_dt_pagto"] = df_fiados["DataPagamento"].apply(parse_br_date)

# --- Cache já pagos
cache = _read_df(ABA_COMISSOES_CACHE)
cache_cols = ["RefID", "Funcionario", "PagoEm", "TerçaPagamento", "ValorComissao", "Competencia", "Observacao"]
cache = garantir_colunas(cache, cache_cols)
ja_pagos = set(cache[s_lower(cache["Funcionario"]) == FUNCIONARIA.lower()]["RefID"].astype(str).tolist())

# --- Seleção dos itens pagáveis
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

hoje = br_now()

if pagar_tudo_ate_hoje:
    # 1) NÃO fiado (tudo que nunca foi pago)
    nao_fiado = dfv[((s_lower(dfv["StatusFiado"]) == "") | (s_lower(dfv["StatusFiado"]) == "nao"))].copy()
    # 2) Fiados liberados (DataPagamento <= hoje)
    fiado_lib = df_fiados[(df_fiados["_dt_pagto"].notna()) & (df_fiados["_dt_pagto"] <= hoje)].copy()
else:
    # fallback: janela de terça→segunda anterior (mantido por compatibilidade)
    # Sugestão de terça mais próxima (próxima ou atual)
    if hoje.weekday() == 1:
        terca_pagto = hoje
    else:
        delta = (1 - hoje.weekday()) % 7
        delta = 7 if delta == 0 else delta
        terca_pagto = (hoje + timedelta(days=delta))
    inicio = terca_pagto - timedelta(days=7)
    fim = inicio + timedelta(days=6)

    nao_fiado = dfv[
        (dfv["_dt_serv"].notna()) & (dfv["_dt_serv"] >= inicio) & (dfv["_dt_serv"] <= fim) &
        ((s_lower(dfv["StatusFiado"]) == "") | (s_lower(dfv["StatusFiado"]) == "nao"))
    ].copy()
    fiado_lib = df_fiados[(df_fiados["_dt_pagto"].notna()) & (df_fiados["_dt_pagto"] <= terca_pagto)].copy()

# Remove já pagos (pela chave RefID)
for df_part in (nao_fiado, fiado_lib):
    if not df_part.empty:
        df_part["RefID"] = df_part.apply(make_refid, axis=1)
        df_part = df_part[~df_part["RefID"].isin(ja_pagos)].copy()

# Reatribui (pois filtramos acima)
nao_fiado = nao_fiado if nao_fiado.empty else nao_fiado[~nao_fiado["RefID"].isin(ja_pagos)].copy()
fiado_lib = fiado_lib if fiado_lib.empty else fiado_lib[~fiado_lib["RefID"].isin(ja_pagos)].copy()

# Monta valores base
nao_fiado = montar_valor_base(nao_fiado)
fiado_lib = montar_valor_base(fiado_lib)

def prepara_editor(df: pd.DataFrame, titulo: str, key_prefix: str):
    if df.empty:
        st.info(f"Sem itens em **{titulo}**.")
        return pd.DataFrame(), 0.0

    ed = df[["Data", "Cliente", "Serviço", "Conta", "Valor_base_comissao"]].copy()
    ed = ed.rename(columns={"Valor_base_comissao": "Valor (base comissão)"})
    # Percentual sugerido: se existir no mapa por serviço, usa; senão, o padrão global
    ed["% Comissão"] = ed["Serviço"].apply(lambda s: float(PERC_SERVICO.get(str(s).strip(), perc_padrao)))
    ed["Comissão (R$)"] = (pd.to_numeric(ed["Valor (base comissão)"], errors="coerce").fillna(0.0) * ed["% Comissão"] / 100.0).round(2)
    ed["RefID"] = df["RefID"].values
    ed["Competência"] = df["Data"].apply(competencia_from_data_str)

    st.subheader(titulo)
    st.caption("Você pode ajustar a % por linha, se precisar.")
    edited = st.data_editor(
        ed,
        key=f"editor_{key_prefix}",
        num_rows="fixed",
        column_config={
            "Valor (base comissão)": st.column_config.NumberColumn(format="R$ %.2f", disabled=True),
            "% Comissão": st.column_config.NumberColumn(format="%.1f %%", min_value=0.0, max_value=100.0, step=0.5),
            "Comissão (R$)": st.column_config.NumberColumn(format="R$ %.2f", disabled=True),
        },
        use_container_width=True
    )
    # Recalcula comissão (caso % mude)
    edited["Comissão (R$)"] = (
        pd.to_numeric(edited["Valor (base comissão)"], errors="coerce").fillna(0.0) *
        pd.to_numeric(edited["% Comissão"], errors="coerce").fillna(0.0) / 100.0
    ).round(2)

    total = float(edited["Comissão (R$)"].sum())

    # Merge de volta para ter infos originais + editadas
    merged = df.merge(
        edited[["RefID", "% Comissão", "Comissão (R$)", "Competência"]],
        on="RefID", how="left"
    )
    merged["PercComissao"] = pd.to_numeric(merged["% Comissão"], errors="coerce").fillna(0.0)
    merged["ComissaoValor"] = pd.to_numeric(merged["Comissão (R$)"], errors="coerce").fillna(0.0)

    st.success(f"Total em **{titulo}**: {format_brl(total)}")
    return merged, total, edited

nao_fiado_grid, total_nao_fiado, nao_fiado_vis = prepara_editor(nao_fiado, "Não fiado (a pagar)", "nao_fiado")
fiado_grid, total_fiado, fiado_vis = prepara_editor(fiado_lib, "Fiados liberados (a pagar)", "fiado_lib")

total_geral = float(total_nao_fiado + total_fiado)
st.metric("💰 Total de comissão desta execução", format_brl(total_geral))

# =============================
# CONFIRMAR E GRAVAR
# =============================
reprocessar_mesmo_ref = st.checkbox(
    "Reprocessar (limpar do cache todos os RefID selecionados antes de gravar)",
    value=False,
    help="Use apenas se você apagou em Despesas e quer marcar os mesmos itens novamente."
)

if st.button("✅ Registrar comissão (por DIA do atendimento), marcar como pago e notificar no Telegram"):
    if (nao_fiado_grid is None or nao_fiado_grid.empty) and (fiado_grid is None or fiado_grid.empty):
        st.warning("Não há itens para pagar.")
    else:
        # 1) Atualiza cache
        cache_df = _read_df(ABA_COMISSOES_CACHE)
        cache_df = garantir_colunas(cache_df, cache_cols)

        if reprocessar_mesmo_ref:
            # Remove do cache qualquer RefID que vamos pagar agora (para reescrever)
            ids_novos = []
            for df_part in [nao_fiado_grid, fiado_grid]:
                if df_part is None or df_part.empty: 
                    continue
                ids_novos.extend(df_part["RefID"].astype(str).tolist())
            ids_novos = set(ids_novos)
            cache_df = cache_df[~cache_df["RefID"].astype(str).isin(ids_novos)].copy()

        novos_cache = []
        hoje_str = to_br_date(hoje)
        for df_part in [nao_fiado_grid, fiado_grid]:
            if df_part is None or df_part.empty:
                continue
            for _, r in df_part.iterrows():
                novos_cache.append({
                    "RefID": r["RefID"],
                    "Funcionario": FUNCIONARIA,
                    "PagoEm": hoje_str,
                    "TerçaPagamento": "",  # não estamos mais restringindo à terça
                    "ValorComissao": f'{float(r["ComissaoValor"]):.2f}'.replace(".", ","),
                    "Competencia": r.get("Competência", ""),
                    "Observacao": f'{r.get("Cliente","")} | {r.get("Serviço","")} | {r.get("Data","")}',
                })
        cache_upd = pd.concat([cache_df[cache_cols], pd.DataFrame(novos_cache)], ignore_index=True)
        _write_df(ABA_COMISSOES_CACHE, cache_upd)

        # 2) Lança em DESPESAS: UMA LINHA POR DIA DO ATENDIMENTO
        despesas_df = _read_df(ABA_DESPESAS)
        despesas_df = garantir_colunas(despesas_df, COLS_DESPESAS_FIX)
        for c in COLS_DESPESAS_FIX:
            if c not in despesas_df.columns:
                despesas_df[c] = ""

        pagaveis = []
        for df_part in [nao_fiado_grid, fiado_grid]:
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
            linhas_adicionadas = len(linhas)

            despesas_final = pd.concat([despesas_df, pd.DataFrame(linhas)], ignore_index=True)
            colunas_finais = [c for c in COLS_DESPESAS_FIX if c in despesas_final.columns] + \
                             [c for c in despesas_final.columns if c not in COLS_DESPESAS_FIX]
            despesas_final = despesas_final[colunas_finais]
            _write_df(ABA_DESPESAS, despesas_final)

        # 3) Telegram — mensagem detalhada
        def build_msg(df_vis: pd.DataFrame, titulo: str) -> str:
            if df_vis is None or df_vis.empty:
                return ""
            linhas_txt = []
            for _, r in df_vis.iterrows():
                dt = str(r.get("Data","")).strip()
                cli = str(r.get("Cliente","")).strip()
                srv = str(r.get("Serviço","")).strip()
                conta = str(r.get("Conta","")).strip()
                val_base = float(pd.to_numeric(r.get("Valor (base comissão)","0"), errors="coerce") or 0.0)
                perc = float(pd.to_numeric(r.get("% Comissão","0"), errors="coerce") or 0.0)
                comi = float(pd.to_numeric(r.get("Comissão (R$)","0"), errors="coerce") or 0.0)
                linhas_txt.append(f"• {dt} | {cli} — {srv} | <i>{conta}</i>\n   Base: {format_brl(val_base)} | %: {perc:.1f}% | Comissão: <b>{format_brl(comi)}</b>")
            subtotal = float(pd.to_numeric(df_vis["Comissão (R$)"], errors="coerce").fillna(0.0).sum())
            bloco = f"<b>{titulo}</b>\n" + "\n".join(linhas_txt) + f"\n<b>Subtotal:</b> {format_brl(subtotal)}\n"
            return bloco

        mensagem = f"<b>Comissão — {FUNCIONARIA}</b>\nData: {hoje_str}\n\n"
        mensagem += build_msg(nao_fiado_vis, "Não fiado (pagos agora)") if nao_fiado_vis is not None else ""
        mensagem += ("\n" if mensagem and not mensagem.endswith("\n\n") else "")
        mensagem += build_msg(fiado_vis, "Fiados liberados (pagos agora)") if fiado_vis is not None else ""
        mensagem += "\n<b>Total geral desta execução:</b> " + format_brl(total_geral)

        if TELEGRAM_TOKEN:
            if notificar_jpaulo and CHAT_ID_JPAULO:
                send_telegram(CHAT_ID_JPAULO, mensagem)
            if notificar_meire and CHAT_ID_MEIRE:
                send_telegram(CHAT_ID_MEIRE, mensagem)

        st.success(
            f"🎉 Comissão registrada! {linhas_adicionadas} linha(s) adicionada(s) em **{ABA_DESPESAS}** "
            f"e {len(novos_cache)} item(ns) marcados no **{ABA_COMISSOES_CACHE}**. Total: {format_brl(total_geral)}"
        )
        st.balloons()
