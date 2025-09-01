# 11_Adicionar_Atendimento.py — FEMININO (ajustado BRUTO + só JP + sem total atendimentos)
# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from gspread.utils import rowcol_to_a1
from datetime import datetime
import pytz
import unicodedata
import requests
from collections import Counter

# =========================
# CONFIG
# =========================
SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"

ABA_DADOS = "Base de Dados Feminino"
STATUS_ABA = "clientes_status_feminino"

TZ = "America/Sao_Paulo"
REL_MULT = 1.5
DATA_FMT = "%d/%m/%Y"

COLS_OFICIAIS = [
    "Data", "Serviço", "Valor", "Conta", "Cliente", "Combo",
    "Funcionário", "Fase", "Tipo", "Período"
]
COLS_FIADO = ["StatusFiado", "IDLancFiado", "VencimentoFiado", "DataPagamento"]

COLS_PAG_EXTRAS = [
    "ValorBrutoRecebido", "ValorLiquidoRecebido",
    "TaxaCartaoValor", "TaxaCartaoPct",
    "FormaPagDetalhe", "PagamentoID"
]

FUNCIONARIOS_FEM = ["Daniela", "Meire"]

# =========================
# TELEGRAM
# =========================
TELEGRAM_TOKEN = "8257359388:AAGayJElTPT0pQadtamVf8LoL7R6EfWzFGE"
TELEGRAM_CHAT_ID_JPAULO = "493747253"

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

def tg_send_photo(photo_url: str, caption: str, chat_id: str | None = None) -> bool:
    token = _get_token()
    chat = chat_id or _get_chat_id_jp()
    if not _check_tg_ready(token, chat):
        return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendPhoto"
        payload = {"chat_id": chat, "photo": photo_url, "caption": caption, "parse_mode": "HTML"}
        r = requests.post(url, data=payload, timeout=30)
        js = r.json() if r.headers.get("content-type","").startswith("application/json") else {}
        if r.ok and js.get("ok"):
            return True
        return tg_send(caption, chat_id=chat)
    except Exception:
        return tg_send(caption, chat_id=chat)

# =========================
# UTILS
# =========================
def _norm(s: str) -> str:
    s = (s or "").strip().casefold()
    s = unicodedata.normalize("NFD", s)
    return "".join(ch for ch in s if unicodedata.category(ch) != "Mn")

def _canon(s: str) -> str:
    s = _norm(s)
    return "".join(ch for ch in s if ch.isalnum())

def _norm_key(s: str) -> str:
    return unicodedata.normalize("NFKC", str(s).strip()).casefold()

def _keyify(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode("ascii")
    return "".join(ch if ch.isalnum() else "_" for ch in s.strip())

def _cap_first(s: str) -> str:
    return (str(s).strip().lower().capitalize()) if s is not None else ""

def now_br():
    return datetime.now(pytz.timezone(TZ)).strftime("%d/%m/%Y %H:%M:%S")

def _fmt_brl(v: float) -> str:
    try: v = float(v)
    except Exception: v = 0.0
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def gerar_pag_id(prefixo="A"):
    return f"{prefixo}-{datetime.now(pytz.timezone(TZ)).strftime('%Y%m%d%H%M%S%f')[:-3]}"

def is_nao_cartao(conta: str) -> bool:
    s = unicodedata.normalize("NFKD", (conta or "")).encode("ascii","ignore").decode("ascii").lower()
    tokens = {"pix","dinheiro","carteira","cash","especie","espécie","transfer","transferencia","transferência","ted","doc"}
    return any(t in s for t in tokens)

def contains_cartao(s: str) -> bool:
    MAQ = {"cart","cartao","cartão","credito","crédito","debito","débito","maquina","maquininha","pos"}
    x = unicodedata.normalize("NFKD", (s or "")).encode("ascii","ignore").decode("ascii").lower().replace(" ","")
    return any(k in x for k in MAQ)

def default_card_flag(conta: str) -> bool:
    if is_nao_cartao(conta): return False
    return contains_cartao(conta)

# =========================
# SHEETS
# =========================
@st.cache_resource
def conectar_sheets():
    info = st.secrets["GCP_SERVICE_ACCOUNT"]
    escopo = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    credenciais = Credentials.from_service_account_info(info, scopes=escopo)
    cliente = gspread.authorize(credenciais)
    return cliente.open_by_key(SHEET_ID)

def carregar_base():
    aba = conectar_sheets().worksheet(ABA_DADOS)
    df = get_as_dataframe(aba).dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]
    for c in [*COLS_OFICIAIS, *COLS_FIADO, *COLS_PAG_EXTRAS]:
        if c not in df.columns:
            df[c] = ""
    return df, aba

def salvar_base(df_final: pd.DataFrame):
    aba = conectar_sheets().worksheet(ABA_DADOS)
    colunas_alvo = list(dict.fromkeys([*COLS_OFICIAIS, *COLS_FIADO, *COLS_PAG_EXTRAS]))
    for c in colunas_alvo:
        if c not in df_final.columns:
            df_final[c] = ""
    df_final = df_final[colunas_alvo]
    aba.clear()
    set_with_dataframe(aba, df_final, include_index=False, include_column_header=True)

# =========================
# FOTOS
# =========================
FOTO_COL_CANDIDATES = ["link_foto","foto","imagem","url_foto","foto_link","link","image"]

@st.cache_data(show_spinner=False, ttl=120)
def carregar_fotos_mapa():
    try:
        sh = conectar_sheets()
        if STATUS_ABA not in [w.title for w in sh.worksheets()]:
            return {}
        ws = sh.worksheet(STATUS_ABA)
        df = get_as_dataframe(ws).fillna("")
        df.columns = [str(c).strip() for c in df.columns]
        cli_col = "Cliente"
        foto_col = "Foto"
        if cli_col in df.columns and foto_col in df.columns:
            tmp = df[[cli_col, foto_col]].copy()
            tmp["k"] = tmp["Cliente"].astype(str).map(_norm)
            return {r["k"]: str(r["Foto"]).strip() for _, r in tmp.iterrows() if str(r["Foto"]).strip()}
        return {}
    except Exception:
        return {}

def get_foto_url(nome: str) -> str | None:
    fotos = carregar_fotos_mapa()
    return fotos.get(_norm(nome))

# =========================
# CARDS
# =========================
def _resumo_do_dia(df_all: pd.DataFrame, cliente: str, data_str: str):
    d = df_all[(df_all["Cliente"].astype(str).str.strip()==cliente) & (df_all["Data"].astype(str).str.strip()==data_str)].copy()
    d["Valor"] = pd.to_numeric(d["Valor"], errors="coerce").fillna(0.0)
    servicos = [str(s).strip() for s in d["Serviço"].fillna("").tolist() if str(s).strip()]
    valor_total = float(d["Valor"].sum()) if not d.empty else 0.0
    is_combo = len(servicos)>1 or (d["Combo"].fillna("").str.strip()!="").any()
    label = " + ".join(servicos)+(" (Combo)" if is_combo else " (Simples)") if servicos else "-"
    periodo_vals = [p for p in d["Período"].astype(str).str.strip().tolist() if p]
    periodo_label = max(set(periodo_vals), key=periodo_vals.count) if periodo_vals else "-"
    conta_vals = [p for p in d["Conta"].astype(str).str.strip().tolist() if p]
    conta_label = max(set(conta_vals), key=conta_vals.count) if conta_vals else "-"
    return label, valor_total, is_combo, servicos, periodo_label, conta_label

def _totais_bruto_liquido(df_all: pd.DataFrame, cliente: str, data_str: str) -> tuple[float,float]:
    d = df_all[(df_all["Cliente"].astype(str).str.strip()==cliente) & (df_all["Data"].astype(str).str.strip()==data_str)].copy()
    if d.empty: return 0.0,0.0
    d["Valor"] = pd.to_numeric(d.get("Valor",0), errors="coerce").fillna(0.0)
    d["ValorBrutoRecebido"] = pd.to_numeric(d.get("ValorBrutoRecebido",0), errors="coerce").fillna(0.0)
    bruto_comp = d.apply(lambda r: float(r["ValorBrutoRecebido"]) if float(r["ValorBrutoRecebido"])>0 else float(r["Valor"]), axis=1)
    return float(bruto_comp.sum()), float(d["Valor"].sum())

def make_card_caption_v2(df_all, cliente, data_str, funcionario, servico_label, valor_total,
    periodo_label, conta_label, pct_func: float|None=None, append_sections:list[str]|None=None, pct_base:float|None=None):
    valor_str = _fmt_brl(valor_total)
    base = (
        "📌 <b>Atendimento registrado</b>\n"
        f"👤 Cliente: <b>{cliente}</b>\n"
        f"🗓️ Data: <b>{data_str}</b>\n"
        f"🕒 Período: <b>{periodo_label or '-'}</b>\n"
        f"💳 Forma de pagamento: <b>{conta_label or '-'}</b>\n"
        f"✂️ Serviço: <b>{servico_label}</b>\n"
        f"💰 Valor total: <b>{valor_str}</b>\n"
        f"👩‍🦰 Atendido por: <b>{funcionario}</b>"
    )
    if pct_func is not None:
        base_para_pct = float(pct_base if pct_base is not None else valor_total)
        valor_pct = base_para_pct * (float(pct_func)/100.0)
        if funcionario=="Daniela":
            base += f"\n🧾 <b>Daniela recebe:</b> <b>{_fmt_brl(valor_pct)}</b> ({float(pct_func):.0f}%)"
        else:
            base += f"\n🧾 Comissão {funcionario} ({float(pct_func):.0f}%): <b>{_fmt_brl(valor_pct)}</b>"
    if append_sections:
        base += "\n\n" + "\n\n".join([s for s in append_sections if s and s.strip()])
    return base

def enviar_card(df_all, cliente, funcionario, data_str, servico=None, valor=None, combo=None, pct_func: float|None=None):
    if servico is None or valor is None:
        servico_label, valor_total, _, _, periodo_label, conta_label = _resumo_do_dia(df_all, cliente, data_str)
    else:
        is_combo = bool(combo and str(combo).strip())
        servico_label = (f"{servico} (Combo)" if is_combo and "+" in str(servico)
                         else f"{servico} (Simples)" if not is_combo else f"{servico} (Combo)")
        valor_total = float(valor)
        _, _, _, _, periodo_label, conta_label = _resumo_do_dia(df_all, cliente, data_str)
    bruto_total,_ = _totais_bruto_liquido(df_all, cliente, data_str)
    caption_jp = make_card_caption_v2(df_all, cliente, data_str, funcionario, servico_label,
        valor_total, periodo_label, conta_label, pct_func=pct_func, pct_base=bruto_total)
    foto = get_foto_url(cliente)
    chat_jp = _get_chat_id_jp()
    if foto: tg_send_photo(foto, caption_jp, chat_id=chat_jp)
    else: tg_send(caption_jp, chat_id=chat_jp)

# =========================
# UI — (resto igual do seu código para adicionar atendimento)
# =========================
st.set_page_config(layout="wide", page_title="Adicionar Atendimento (Feminino)", page_icon="💇‍♀️")
st.title("📅 Adicionar Atendimento (Feminino)")

# aqui segue o resto do fluxo de formulário (combos, simples, salvar, etc.)
# sem alterações além de chamar enviar_card() que já está ajustado acima
