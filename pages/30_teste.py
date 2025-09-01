# -*- coding: utf-8 -*-
# 12_Comissoes_Daniela.py — Comissão Daniela (paga TUDO, arredonda base, envia prévia no Telegram e grava em Despesas do Salão Feminino)

import streamlit as st
import pandas as pd
import gspread
import hashlib
import re
import requests
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
from datetime import datetime
import pytz
from math import ceil

# =============================
# CONFIG BÁSICA
# =============================
SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"

ABA_DADOS            = "Base de Dados Feminino"
ABA_COMISSOES_CACHE  = "comissoes_cache_feminino"
ABA_DESPESAS_SALAO   = "Despesas do Salão Feminino"   # alvo de lançamento
ABA_CONFIG           = "config_comissoes_feminino"     # persiste % por serviço

TZ = "America/Sao_Paulo"
FUNCIONARIA = "Daniela"

COLS_OFICIAIS = [
    "Data","Serviço","Valor","Conta","Cliente","Combo",
    "Funcionário","Fase","Tipo","Período",
    "StatusFiado","IDLancFiado","VencimentoFiado","DataPagamento"
]
COLS_DESPESAS_FIX = ["Data","Prestador","Descrição","Valor","Me Pag:"]

PERCENTUAL_PADRAO = 50.0

# =============================
# TELEGRAM — mesma configuração do 11_Adicionar_Atendimento.py
# =============================
TELEGRAM_TOKEN = "8257359388:AAGayJElTPT0pQadtamVf8LoL7R6EfWzFGE"
TELEGRAM_CHAT_ID_JPAULO   = "493747253"
TELEGRAM_CHAT_ID_VINICIUS = "-1001234567890"
TELEGRAM_CHAT_ID_FEMININO = "-1002965378062"
TELEGRAM_CHAT_ID_DANIELA  = "-1003039502089"

def _get_secret(name: str, default: str | None = None) -> str | None:
    try:
        val = st.secrets.get(name)
        val = (val or "").strip()
        if val:
            return val
    except Exception:
        pass
    return (default or "").strip() or None

def _get_token() -> str | None:
    return _get_secret("TELEGRAM_TOKEN", TELEGRAM_TOKEN)

def _get_chat_id_jp() -> str | None:
    return _get_secret("TELEGRAM_CHAT_ID_JPAULO", TELEGRAM_CHAT_ID_JPAULO)

def _get_chat_id_vini() -> str | None:
    return _get_secret("TELEGRAM_CHAT_ID_VINICIUS", TELEGRAM_CHAT_ID_VINICIUS)

def _get_chat_id_fem() -> str | None:
    return _get_secret("TELEGRAM_CHAT_ID_FEMININO", TELEGRAM_CHAT_ID_FEMININO)

def _get_chat_id_dani() -> str | None:
    return _get_secret("TELEGRAM_CHAT_ID_DANIELA", TELEGRAM_CHAT_ID_DANIELA)

def _check_tg_ready(token: str | None, chat_id: str | None) -> bool:
    return bool((token or "").strip() and (chat_id or "").strip())

def tg_send(text: str, chat_id: str | None = None) -> bool:
    token = _get_token()
    chat = chat_id or _get_chat_id_jp()
    if not _check_tg_ready(token, chat):
        return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        r = requests.post(url, json=payload, timeout=30)
        js = r.json() if r.headers.get("content-type","").startswith("application/json") else {}
        return bool(r.ok and js.get("ok"))
    except Exception:
        return False

# =============================
# CONEXÃO SHEETS
# =============================
@st.cache_resource
def _conn():
    info = st.secrets["GCP_SERVICE_ACCOUNT"]
    scopes = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    cred = Credentials.from_service_account_info(info, scopes=scopes)
    cli = gspread.authorize(cred)
    return cli.open_by_key(SHEET_ID)

def _ws(title:str):
    sh=_conn()
    try:
        return sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=2000, cols=50)

def _read_df(title:str)->pd.DataFrame:
    ws=_ws(title)
    df=get_as_dataframe(ws).fillna("")
    df.columns=[str(c).strip() for c in df.columns]
    df=df.dropna(how="all").replace({pd.NA:""})
    return df

def _write_df(title:str, df:pd.DataFrame):
    ws=_ws(title); ws.clear()
    set_with_dataframe(ws, df, include_index=False, include_column_header=True)

# =============================
# CONFIG (% por serviço) — persistência
# =============================
def _read_config()->dict:
    try:
        df=_read_df(ABA_CONFIG)
    except Exception:
        df=pd.DataFrame(columns=["Serviço","PercentualPadrao"])
    if "Serviço" not in df.columns or "PercentualPadrao" not in df.columns:
        df=pd.DataFrame(columns=["Serviço","PercentualPadrao"])
    df["Serviço"]=df["Serviço"].astype(str).str.strip()
    out={}
    for _,r in df.iterrows():
        s=str(r.get("Serviço","")).strip()
        try: p=float(str(r.get("PercentualPadrao","")).replace(",",".")) 
        except: p=None
        if s and p is not None: out[s]=p
    return out

def _write_config(perc_map:dict):
    if not perc_map: return
    df=pd.DataFrame([{"Serviço":k,"PercentualPadrao":float(v)} for k,v in sorted(perc_map.items())])
    _write_df(ABA_CONFIG, df)

# =============================
# HELPERS
# =============================
def br_now(): return datetime.now(pytz.timezone(TZ))
def parse_br_date(s:str):
    s=(s or "").strip()
    for fmt in ("%d/%m/%Y","%d-%m-%Y","%Y-%m-%d"):
        try: return datetime.strptime(s, fmt)
        except: pass
    return None
def to_br_date(dt:datetime): return dt.strftime("%d/%m/%Y")
def competencia_from_data_str(s:str):
    dt=parse_br_date(s);  return dt.strftime("%m/%Y") if dt else ""
def s_lower(s): return s.astype(str).str.strip().str.lower()
def garantir_colunas(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    return df
def make_refid(row:pd.Series)->str:
    key="|".join([
        str(row.get("Cliente","")).strip(),
        str(row.get("Data","")).strip(),
        str(row.get("Serviço","")).strip(),
        str(row.get("Valor","")).strip(),
        str(row.get("Funcionário","")).strip(),
        str(row.get("Combo","")).strip(),
    ])
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
def arredonda_para_cima_mult5(v:float)->float:
    try: v=float(v)
    except: return 0.0
    return float(ceil(v/5.0)*5.0)
def format_brl(v:float)->str:
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X",".")

# =============================
# UI
# =============================
st.set_page_config(layout="wide")
st.title(f"💇‍♀️ Comissão — {FUNCIONARIA} (paga TUDO; arredonda base em múltiplos de 5)")

base=_read_df(ABA_DADOS)
base=garantir_colunas(base, COLS_OFICIAIS).copy()
PERC_SALVOS=_read_config()

colA,colB,colC=st.columns([1,1,1])
with colA:
    perc_padrao=st.number_input("Percentual padrão (%)", value=PERCENTUAL_PADRAO, step=1.0, min_value=0.0, max_value=100.0)
with colB:
    incluir_produtos=st.checkbox("Incluir PRODUTOS?", value=False)
with colC:
    meio_pag=st.selectbox("Meio de pagamento (para DESPESAS)", ["Dinheiro","Pix","Cartão","Transferência"], index=0)

descricao_padrao=st.text_input("Descrição (para DESPESAS)", value=f"Comissão {FUNCIONARIA}")

colN1, colN2 = st.columns(2)
with colN1: notificar_jpaulo  = st.checkbox("Enviar Telegram para JPaulo",  value=True)
with colN2: notificar_daniela = st.checkbox("Enviar Telegram para Daniela", value=True)

# ====== Seleção de dados (PAGA TUDO) ======
dfv=base[s_lower(base["Funcionário"])==FUNCIONARIA.lower()].copy()
if not incluir_produtos:
    dfv=dfv[s_lower(dfv["Tipo"])=="serviço"].copy()

dfv["_dt_serv"]=dfv["Data"].apply(parse_br_date)
dfv["RefID"]=dfv.apply(make_refid, axis=1)

# cache de pagos
cache=_read_df(ABA_COMISSOES_CACHE)
cache_cols=["RefID","Funcionario","PagoEm","TerçaPagamento","ValorComissao","Competencia","Observacao"]
cache=garantir_colunas(cache, cache_cols)
ja_pagos=set(cache[s_lower(cache["Funcionario"])==FUNCIONARIA.lower()]["RefID"].astype(str).tolist())

# separa fiado e não fiado
hoje=br_now()
df_fiados = dfv[(s_lower(dfv["StatusFiado"])!="") | (s_lower(dfv["IDLancFiado"])!="")]
df_fiados["_dt_pagto"]=df_fiados["DataPagamento"].apply(parse_br_date)

nao_fiado = dfv[(s_lower(dfv["StatusFiado"])=="") | (s_lower(dfv["StatusFiado"])=="nao")].copy()
fiado_lib = df_fiados[(df_fiados["_dt_pagto"].notna()) & (df_fiados["_dt_pagto"]<=hoje)].copy()
fiado_pend= df_fiados[(df_fiados["_dt_pagto"].isna())  | (df_fiados["_dt_pagto"]>hoje)].copy()

if ja_pagos:
    nao_fiado = nao_fiado[~nao_fiado["RefID"].isin(ja_pagos)].copy()
    fiado_lib = fiado_lib[~fiado_lib["RefID"].isin(ja_pagos)].copy()

st.info("Modo: pagando <b>TUDO</b> que ainda não foi pago — Não fiado + Fiados com DataPagamento ≤ hoje.", icon="💸")

# ====== Monta valor base arredondado ======
def montar_valor_base(df:pd.DataFrame)->pd.DataFrame:
    if df.empty:
        df["Valor_num"]=[]; df["Competência"]=[]; df["Valor_base_comissao"]=[]
        return df
    df=df.copy()
    df["Valor_num"]=pd.to_numeric(df["Valor"], errors="coerce").fillna(0.0)
    df["Competência"]=df["Data"].apply(competencia_from_data_str)
    df["Valor_base_comissao"]=df["Valor_num"].apply(arredonda_para_cima_mult5)
    return df

nao_fiado = montar_valor_base(nao_fiado)
fiado_lib = montar_valor_base(fiado_lib)
fiado_pend= montar_valor_base(fiado_pend)

# ====== Editor com recalculo em tempo real ======
def preparar_grid(df:pd.DataFrame, titulo:str, key_prefix:str):
    if df.empty:
        st.subheader(titulo)
        st.warning("Sem itens.")
        return pd.DataFrame(), 0.0, pd.DataFrame()

    df=df.copy()
    ed = df[["Data","Cliente","Serviço","Conta","Valor_base_comissao","Competência","RefID"]].rename(
        columns={"Valor_base_comissao":"Valor (para comissão)"}
    )
    ed["% Comissão"] = ed["Serviço"].apply(lambda s: float(PERC_SALVOS.get(str(s).strip(), perc_padrao)))
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
            "Comissão (R$)": st.column_config.NumberColumn(format="R$ %.2f", disabled=True),
        },
        use_container_width=True
    )

    # arredonda valor digitado e recalcula
    edited = edited.copy()
    edited["Valor (para comissão)"] = edited["Valor (para comissão)"].apply(
        lambda x: arredonda_para_cima_mult5(float(pd.to_numeric(x, errors="coerce") or 0.0))
    )
    edited["% Comissão"] = pd.to_numeric(edited["% Comissão"], errors="coerce").fillna(0.0)
    edited["Comissão (R$)"] = (edited["Valor (para comissão)"] * edited["% Comissão"] / 100.0).round(2)

    total=float(edited["Comissão (R$)"].sum())

    merged = df.merge(
        edited[["RefID","Valor (para comissão)","% Comissão","Comissão (R$)","Competência","Data","Cliente","Serviço","Conta"]],
        on="RefID", how="left"
    )
    merged["ValorBaseEditado"]=pd.to_numeric(merged["Valor (para comissão)"], errors="coerce").fillna(0.0)
    merged["PercComissao"]=pd.to_numeric(merged["% Comissão"], errors="coerce").fillna(0.0)
    merged["ComissaoValor"]=pd.to_numeric(merged["Comissão (R$)"], errors="coerce").fillna(0.0)

    st.success(f"Total em **{titulo}**: {format_brl(total)}")
    return merged, total, edited

grid_nao_fiado, total_nao_fiado, vis_nao_fiado = preparar_grid(nao_fiado, "Não fiado (a pagar)", "nao_fiado")
grid_fiado,     total_fiado,     vis_fiado     = preparar_grid(fiado_lib,  "Fiados liberados (a pagar)", "fiado_lib")

# ====== Fiados pendentes (sempre visível)
st.subheader("📌 Fiados pendentes (histórico — ainda NÃO pagos)")
if fiado_pend.empty:
    st.info("Nenhum fiado pendente no momento.")
    total_fiados_pend = 0.0
else:
    visp = fiado_pend[["Data","Cliente","Serviço","Conta","Valor","Valor_base_comissao","Competência"]].rename(
        columns={"Valor_base_comissao":"Valor (para comissão)"}
    )
    visp["% Comissão"] = visp["Serviço"].apply(lambda s: float(PERC_SALVOS.get(str(s).strip(), perc_padrao)))
    visp["Comissão (R$)"] = (
        pd.to_numeric(visp["Valor (para comissão)"], errors="coerce").fillna(0.0) *
        pd.to_numeric(visp["% Comissão"], errors="coerce").fillna(0.0) / 100.0
    ).round(2)
    total_fiados_pend = float(visp["Comissão (R$)"].sum())
    st.dataframe(visp.sort_values(by=["Data","Cliente"]).reset_index(drop=True), use_container_width=True)
st.warning(f"Comissão futura (quando pagarem): **{format_brl(total_fiados_pend)}**")

# ====== Resumo
col_m1,col_m2,col_m3,col_m4=st.columns(4)
with col_m1: st.metric("Não fiado (a pagar)", format_brl(total_nao_fiado))
with col_m2: st.metric("Fiados liberados (a pagar)", format_brl(total_fiado))
with col_m3: st.metric("Total desta execução", format_brl(total_nao_fiado+total_fiado))
with col_m4: st.metric("Fiados pendentes (futuro)", format_brl(total_fiados_pend))

# ====== Builder de mensagem (reuso)
def _tg_build_msg(titulo: str, vis_df: pd.DataFrame) -> str:
    if vis_df is None or vis_df.empty:
        return ""
    linhas = []
    for _, r in vis_df.iterrows():
        dt    = str(r.get("Data","")).strip()
        cli   = str(r.get("Cliente","")).strip()
        srv   = str(r.get("Serviço","")).strip()
        conta = str(r.get("Conta","")).strip()
        comi  = float(pd.to_numeric(r.get("Comissão (R$)","0"), errors="coerce") or 0.0)
        linhas.append(f"• {dt} | {cli} — {srv} | <i>{conta}</i>\n   Comissão: <b>{format_brl(comi)}</b>")
    subtotal = float(pd.to_numeric(vis_df["Comissão (R$)"], errors="coerce").fillna(0.0).sum())
    return f"<b>{titulo}</b>\n" + "\n".join(linhas) + f"\n<b>Subtotal:</b> {format_brl(subtotal)}\n"

def _tg_build_full(vis_nao_fiado: pd.DataFrame, vis_fiado: pd.DataFrame) -> tuple[str,float]:
    hoje_str = to_br_date(br_now())
    tot = float(
        (vis_nao_fiado["Comissão (R$)"].sum() if vis_nao_fiado is not None and not vis_nao_fiado.empty else 0.0) +
        (vis_fiado["Comissão (R$)"].sum()     if vis_fiado     is not None and not vis_fiado.empty     else 0.0)
    )
    msg  = f"<b>Comissão — {FUNCIONARIA}</b>\nData: {hoje_str}\n\n"
    msg += _tg_build_msg("Não fiado (pagos agora)", vis_nao_fiado)
    if msg and not msg.endswith("\n"): msg += "\n"
    msg += _tg_build_msg("Fiados liberados (pagos agora)", vis_fiado)
    msg += "\n<b>Total geral desta execução:</b> " + format_brl(tot)
    return msg, tot

# ====== Botão de PRÉVIA (sem gravar) — somente Telegram
if st.button("📤 Enviar resumo (sem gravar) — Telegram"):
    msg, tot = _tg_build_full(vis_nao_fiado, vis_fiado)
    if notificar_jpaulo and _get_chat_id_jp():    tg_send(msg, chat_id=_get_chat_id_jp())
    if notificar_daniela and _get_chat_id_dani(): tg_send(msg, chat_id=_get_chat_id_dani())
    st.success(f"Resumo enviado por Telegram. Total (prévia): {format_brl(tot)}")

# =============================
# CONFIRMAR E GRAVAR
# =============================
if st.button("✅ Registrar comissão (1 linha por DIA), marcar como pago e enviar Telegram"):
    if (grid_nao_fiado is None or grid_nao_fiado.empty) and (grid_fiado is None or grid_fiado.empty):
        st.warning("Não há itens para pagar.")
    else:
        hoje_str=to_br_date(br_now())

        # 1) Atualiza cache (anti-duplicidade)
        novos_cache=[]
        for df_part in [grid_nao_fiado, grid_fiado]:
            if df_part is None or df_part.empty: continue
            for _,r in df_part.iterrows():
                novos_cache.append({
                    "RefID": r["RefID"],
                    "Funcionario": FUNCIONARIA,
                    "PagoEm": hoje_str,
                    "TerçaPagamento": "",  # sem janela fixa
                    "ValorComissao": f'{float(r["ComissaoValor"]):.2f}'.replace(".", ","),
                    "Competencia": r.get("Competência",""),
                    "Observacao": f'{r.get("Cliente","")} | {r.get("Serviço","")} | {r.get("Data","")}',
                })
        cache_df=_read_df(ABA_COMISSOES_CACHE)
        cache_df=garantir_colunas(cache_df, cache_cols)
        cache_upd=pd.concat([cache_df[cache_cols], pd.DataFrame(novos_cache)], ignore_index=True)
        _write_df(ABA_COMISSOES_CACHE, cache_upd)

        # 2) Despesas do Salão Feminino (1 linha por DIA do atendimento)
        despesas_df=_read_df(ABA_DESPESAS_SALAO)
        despesas_df=garantir_colunas(despesas_df, COLS_DESPESAS_FIX)
        for c in COLS_DESPESAS_FIX:
            if c not in despesas_df.columns: despesas_df[c]=""

        pagaveis=[]
        for df_part in [grid_nao_fiado, grid_fiado]:
            if df_part is None or df_part.empty: continue
            pagaveis.append(df_part[["Data","Competência","ComissaoValor"]].copy())

        linhas_adicionadas=0
        if pagaveis:
            pagos=pd.concat(pagaveis, ignore_index=True)

            def _norm_dt(s):
                s=(s or "").strip()
                for fmt in ("%d/%m/%Y","%d-%m-%Y","%Y-%m-%d"):
                    try: return datetime.strptime(s, fmt)
                    except: pass
                return None

            pagos["_dt"]=pagos["Data"].apply(_norm_dt)
            pagos=pagos[pagos["_dt"].notna()].copy()

            por_dia=pagos.groupby(["Data","Competência"], dropna=False)["ComissaoValor"].sum().reset_index()

            linhas=[]
            for _,row in por_dia.iterrows():
                data_serv=str(row["Data"]).strip()
                comp=str(row["Competência"]).strip()
                val=float(row["ComissaoValor"])
                linhas.append({
                    "Data": data_serv,
                    "Prestador": FUNCIONARIA,
                    "Descrição": f"{descricao_padrao} — Comp {comp} — Pago em {hoje_str}",
                    "Valor": f'R$ {val:.2f}'.replace(".", ","),
                    "Me Pag:": meio_pag
                })
            despesas_final=pd.concat([despesas_df, pd.DataFrame(linhas)], ignore_index=True)
            colunas_finais=[c for c in COLS_DESPESAS_FIX if c in despesas_final.columns] + \
                           [c for c in despesas_final.columns if c not in COLS_DESPESAS_FIX]
            despesas_final=despesas_final[colunas_finais]
            _write_df(ABA_DESPESAS_SALAO, despesas_final)
            linhas_adicionadas=len(linhas)

        # 3) Persiste últimos % por serviço
        perc_atualizados=dict(PERC_SALVOS)
        def _coleta_percentuais(vis_df):
            out={}
            if vis_df is None or vis_df.empty: return out
            for _,r in vis_df.iterrows():
                s=str(r.get("Serviço","")).strip()
                try: p=float(str(r.get("% Comissão","")).replace(",", "."))
                except: p=None
                if s and p is not None: out[s]=p
            return out
        for m in (_coleta_percentuais(vis_nao_fiado), _coleta_percentuais(vis_fiado)):
            perc_atualizados.update(m)
        _write_config(perc_atualizados)

        # 4) Telegram final (mesma mensagem da prévia)
        msg, tot = _tg_build_full(vis_nao_fiado, vis_fiado)
        if notificar_jpaulo and _get_chat_id_jp():    tg_send(msg, chat_id=_get_chat_id_jp())
        if notificar_daniela and _get_chat_id_dani(): tg_send(msg, chat_id=_get_chat_id_dani())

        st.success(
            f"🎉 Comissão registrada! {linhas_adicionadas} linha(s) em **{ABA_DESPESAS_SALAO}** "
            f"e {len(novos_cache)} item(ns) no **{ABA_COMISSOES_CACHE}**. Total: {format_brl(tot)}"
        )
        st.balloons()
