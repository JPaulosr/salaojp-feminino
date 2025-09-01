# 11_Adicionar_Atendimento.py — VERSÃO FEMININO COMPLETA (atualizada)
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

# >>> Abas FEMININO <<<
ABA_DADOS = "Base de Dados Feminino"
STATUS_ABA = "clientes_status_feminino"

FOTO_COL_CANDIDATES = ["link_foto", "foto", "imagem", "url_foto", "foto_link", "link", "image"]

TZ = "America/Sao_Paulo"
REL_MULT = 1.5
DATA_FMT = "%d/%m/%Y"

COLS_OFICIAIS = [
    "Data", "Serviço", "Valor", "Conta", "Cliente", "Combo",
    "Funcionário", "Fase", "Tipo", "Período"
]
COLS_FIADO = ["StatusFiado", "IDLancFiado", "VencimentoFiado", "DataPagamento"]

# Extras para pagamento com cartão
COLS_PAG_EXTRAS = [
    "ValorBrutoRecebido", "ValorLiquidoRecebido",
    "TaxaCartaoValor", "TaxaCartaoPct",
    "FormaPagDetalhe", "PagamentoID"
]

FUNCIONARIOS_FEM = ["Daniela", "Meire"]

# =========================
# TELEGRAM IDs
# =========================
TELEGRAM_TOKEN = "8257359388:AAGayJElTPT0pQadtamVf8LoL7R6EfWzFGE"
TELEGRAM_CHAT_ID_JPAULO = "493747253"
TELEGRAM_CHAT_ID_VINICIUS = "-1001234567890"
TELEGRAM_CHAT_ID_FEMININO = "-1002965378062"
TELEGRAM_CHAT_ID_DANIELA = "-1003039502089"

# =========================
# UTILS
# =========================
def _norm(s: str) -> str:
    s = (s or "").strip().casefold()
    s = unicodedata.normalize("NFD", s)
    return "".join(ch for ch in s if unicodedata.category(ch) != "Mn")

def _fmt_brl(v: float) -> str:
    try:
        v = float(v)
    except Exception:
        v = 0.0
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def gerar_pag_id(prefixo="A"):
    return f"{prefixo}-{datetime.now(pytz.timezone(TZ)).strftime('%Y%m%d%H%M%S%f')[:-3]}"

def _calc_payout_daniela(valor_total: float, pct: float | None) -> str:
    if pct is None:
        return ""
    try:
        pctf = max(0.0, min(100.0, float(pct)))
    except Exception:
        pctf = 0.0
    valor_receber = round(float(valor_total or 0.0) * (pctf / 100.0), 2)
    return f"💰 Daniela recebe: <b>{_fmt_brl(valor_receber)}</b> ({pctf:.0f}%)"

# =========================
# TELEGRAM – envio
# =========================
def tg_send(text: str, chat_id: str) -> bool:
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        r = requests.post(url, json=payload, timeout=30)
        return r.ok
    except Exception:
        return False

def tg_send_photo(photo_url: str, caption: str, chat_id: str) -> bool:
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
        payload = {"chat_id": chat_id, "photo": photo_url, "caption": caption, "parse_mode": "HTML"}
        r = requests.post(url, data=payload, timeout=30)
        if r.ok: return True
        return tg_send(caption, chat_id=chat_id)
    except Exception:
        return tg_send(caption, chat_id=chat_id)

# =========================
# CARDS
# =========================
def make_card_caption(df_all, cliente, data_str, funcionario, servico_label, valor_total, periodo_label,
                      pct_daniela=None, append_sections=None, conta_pag: str | None = None):
    valor_str = _fmt_brl(valor_total)
    forma = (conta_pag or "-")
    base = (
        "📌 <b>Atendimento registrado</b>\n"
        f"👤 Cliente: <b>{cliente}</b>\n"
        f"🗓️ Data: <b>{data_str}</b>\n"
        f"🕒 Período: <b>{periodo_label}</b>\n"
        f"💳 Forma de pagamento: <b>{forma}</b>\n"
        f"✂️ Serviço: <b>{servico_label}</b>\n"
        f"💰 Valor total: <b>{valor_str}</b>\n"
        f"👩‍🦰 Atendido por: <b>{funcionario}</b>"
    )
    if funcionario == "Daniela" and pct_daniela is not None:
        base += "\n" + _calc_payout_daniela(valor_total, pct_daniela)
    if append_sections:
        base += "\n\n" + "\n\n".join([s for s in append_sections if s and s.strip()])
    return base

def _conta_do_dia(df_all: pd.DataFrame, cliente: str, data_str: str) -> str | None:
    d = df_all[
        (df_all["Cliente"].astype(str).str.strip()==cliente) &
        (df_all["Data"].astype(str).str.strip()==data_str)
    ]
    if d.empty or "Conta" not in d.columns: return None
    s = d["Conta"].astype(str).str.strip()
    s = s[s!=""]
    return s.mode().iat[0] if not s.empty else None

def enviar_card(df_all, cliente, funcionario, data_str, servico=None, valor=None, combo=None,
                pct_daniela=None, conta_pag: str | None = None):
    servico_label = servico or "-"
    valor_total = float(valor or 0.0)
    periodo_label = "-"

    if not conta_pag:
        conta_pag = _conta_do_dia(df_all, cliente, data_str)

    extras = []  # ex: cartão
    caption = make_card_caption(df_all, cliente, data_str, funcionario, servico_label, valor_total, periodo_label,
                                pct_daniela=(pct_daniela if funcionario=="Daniela" else None),
                                append_sections=extras, conta_pag=conta_pag)

    foto = None

    # Roteamento
    if funcionario == "Vinicius":
        tg_send_photo(foto, caption, chat_id=TELEGRAM_CHAT_ID_VINICIUS)
        tg_send_photo(foto, caption, chat_id=TELEGRAM_CHAT_ID_JPAULO)
    elif funcionario == "Daniela":
        tg_send_photo(foto, caption, chat_id=TELEGRAM_CHAT_ID_DANIELA)
        tg_send_photo(foto, caption, chat_id=TELEGRAM_CHAT_ID_JPAULO)
    elif funcionario == "Meire":
        tg_send_photo(foto, caption, chat_id=TELEGRAM_CHAT_ID_FEMININO)
        tg_send_photo(foto, caption, chat_id=TELEGRAM_CHAT_ID_JPAULO)
    else:
        tg_send_photo(foto, caption, chat_id=TELEGRAM_CHAT_ID_JPAULO)

# =========================
# (restante do código do formulário continua igual)
# basta garantir que ao chamar enviar_card, você passe:
#    conta_pag=conta
#    pct_daniela=pct_daniela (se funcionário == Daniela)
# =========================
