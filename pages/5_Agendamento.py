# -*- coding: utf-8 -*-
# 11_Adicionar_Atendimento_Feminino.py — Clientes da "Base de Dados Feminino" + Notificação (Canal Feminino + JPaulo)

import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe
from datetime import datetime
import pytz, unicodedata, requests

# =========================
# CONFIG
# =========================
SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"

ABA_DADOS_FEM = "Base de Dados Feminino"       # <-- usa esta aba para listar clientes e salvar
STATUS_ABA_FEM = "clientes_status_feminino"    # opcional (para fotos), se existir

TZ          = "America/Sao_Paulo"
DATA_FMT_BR = "%d/%m/%Y"
HORA_FMT    = "%H:%M:%S"

FUNCIONARIAS_FEMININO = {"Meire", "Daniela"}   # padrão Meire

# Telegram (mantenha em st.secrets em produção)
TELEGRAM_TOKEN            = st.secrets.get("TELEGRAM_TOKEN", "8257359388:AAGayJElTPT0pQadtamVf8LoL7R6EfWzFGE")
TELEGRAM_CHAT_ID_JPAULO   = st.secrets.get("TELEGRAM_CHAT_ID_JPAULO", "493747253")
TELEGRAM_CHAT_ID_FEMININO = st.secrets.get("TELEGRAM_CHAT_ID_FEMININO", "-1002965378062")

# =========================
# GOOGLE SHEETS
# =========================
@st.cache_resource
def conectar_sheets():
    if "GCP_SERVICE_ACCOUNT" not in st.secrets:
        st.stop()  # configure st.secrets["GCP_SERVICE_ACCOUNT"]
    info = dict(st.secrets["GCP_SERVICE_ACCOUNT"])
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"]
    )
    return gspread.authorize(creds)

def abrir_aba(gc, key: str, aba: str):
    sh = gc.open_by_key(key)
    try:
        return sh.worksheet(aba)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=aba, rows=1000, cols=30)

def _norm(s: str) -> str:
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    return "".join(c for c in s if not unicodedata.combining(c))

@st.cache_data(ttl=300)
def listar_clientes_da_base_fem(gc) -> list[str]:
    """Lista única (ordenada) de clientes, somente da 'Base de Dados Feminino'."""
    ws = abrir_aba(gc, SHEET_ID, ABA_DADOS_FEM)
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0).dropna(how="all")
    if df.empty or "Cliente" not in df.columns:
        return []
    vistos, saida = set(), []
    for nome in df["Cliente"].dropna().astype(str):
        nm = nome.strip()
        if not nm:
            continue
        chave = _norm(nm)
        if chave not in vistos:
            vistos.add(chave)
            saida.append(nm)
    return sorted(saida, key=lambda x: x.casefold())

def append_respeitando_cabecalho(ws, row_dict: dict):
    """Append sem limpar a aba; respeita o cabeçalho existente."""
    headers = ws.row_values(1)
    if not headers:
        # cria cabeçalho com as chaves informadas na primeira gravação
        headers = list(row_dict.keys())
        ws.update('A1', [headers])
    valores = [row_dict.get(col, "") for col in headers]
    ws.append_row(valores, value_input_option="USER_ENTERED")

# =========================
# TELEGRAM
# =========================
def _send_telegram_message(chat_id: str, html_text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": chat_id, "text": html_text, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        requests.post(url, data=data, timeout=12)
    except Exception as e:
        st.warning(f"Falha ao enviar mensagem para {chat_id}: {e}")

def enviar_card(destinos: list, cliente: str, servico: str, valor: float,
                data_br: str, hora_ini: str | None, funcionario: str):
    servico_display = servico[:1].upper() + servico[1:] if servico else "-"
    hora_txt = hora_ini if (hora_ini and len(hora_ini) >= 5) else "-"
    valor_txt = f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    card = (
        "✨ <b>Novo Atendimento</b>\n"
        f"👤 <b>Cliente:</b> {cliente}\n"
        f"💇‍♀️ <b>Serviço:</b> {servico_display}\n"
        f"💲 <b>Valor:</b> {valor_txt}\n"
        f"📅 <b>Data:</b> {data_br}\n"
        f"⏰ <b>Horário:</b> {hora_txt}\n"
        f"🧑‍🎤 <b>Funcionário:</b> {funcionario}"
    )
    for chat_id in destinos:
        _send_telegram_message(chat_id, card)

# =========================
# APP
# =========================
st.set_page_config(page_title="Adicionar Atendimento — Feminino", layout="wide")
st.title("🖊️ Adicionar Atendimento — Feminino")

gc = conectar_sheets()
ws_fem = abrir_aba(gc, SHEET_ID, ABA_DADOS_FEM)

# ---- Formulário ----
col1, col2 = st.columns([1,1])
with col1:
    data_input = st.date_input("Data", value=datetime.now(pytz.timezone(TZ)).date())
with col2:
    funcionario = st.selectbox("Funcionário", options=["Meire", "Daniela"], index=0)

# Clientes (somente da Base de Dados Feminino)
clientes_lista = ["➕ Digitar novo nome"]
try:
    base_clientes = listar_clientes_da_base_fem(gc)
    if base_clientes:
        clientes_lista += base_clientes
except Exception:
    pass

nome_sel = st.selectbox("Nome da Cliente", options=clientes_lista, index=1 if len(clientes_lista) > 1 else 0)
if nome_sel == "➕ Digitar novo nome":
    cliente = st.text_input("Novo nome da cliente").strip()
else:
    cliente = nome_sel.strip()

col3, col4 = st.columns([1,1])
with col3:
    tipo = st.selectbox("Tipo", options=["Serviço", "Produto"], index=0)
with col4:
    conta = st.text_input("Forma de Pagamento", placeholder="Dinheiro / Cartão / Pix").strip()

col5, col6 = st.columns([1,1])
with col5:
    servico = st.text_input("Serviço", placeholder="progressiva / corte+escova").strip()
with col6:
    valor = st.number_input("Valor (R$)", min_value=0.0, step=1.0, format="%.2f")

col7, col8 = st.columns([1,1])
with col7:
    hora_inicio = st.text_input("Hora Início (HH:MM:SS)", value="")
with col8:
    hora_saida  = st.text_input("Hora Saída (HH:MM:SS)", value="")

combo = st.text_input("Combo (opcional)", placeholder="corte+escova").strip()

col_btn = st.columns([1,1,6])
with col_btn[0]:
    salvar = st.button("💾 Salvar atendimento", type="primary")
with col_btn[1]:
    if st.button("🧹 Limpar formulário"):
        st.experimental_rerun()

def validar_hora(h: str) -> str | None:
    if not h:
        return None
    try:
        datetime.strptime(h, HORA_FMT)
        return h
    except Exception:
        return None

if salvar:
    if not cliente or not servico or valor <= 0:
        st.error("Preencha Cliente, Serviço e Valor corretamente.")
        st.stop()

    data_br = data_input.strftime(DATA_FMT_BR)

    # Monta linha (campos padrão + compatíveis com a sua planilha)
    row = {
        "Data": data_br,
        "Serviço": servico,                          # mantém como digitado (minúsculo e/ou '+')
        "Valor": valor,
        "Conta": conta,
        "Cliente": cliente,
        "Combo": combo,
        "Funcionário": funcionario,
        "Fase": "Dono + funcionário",
        "Tipo": tipo,
        # Em muitas abas femininas o campo "Período" é Manhã/Tarde/Noite; se quiser, adicionamos um select disso
        "Período": "",
        "Hora Chegada": "",
        "Hora Início": validar_hora(hora_inicio) or "",
        "Hora Saída": validar_hora(hora_saida) or "",
        "Hora Saída do Salão": ""
    }

    # Append respeitando o cabeçalho existente (não limpa a aba e mantém colunas como StatusFiado, etc.)
    try:
        append_respeitando_cabecalho(ws_fem, row)
        st.success("Atendimento salvo na planilha (Base de Dados Feminino) ✅")
    except Exception as e:
        st.error(f"Erro ao salvar na planilha: {e}")
        st.stop()

    # Notificações — sempre JPaulo; e canal feminino se for Meire/Daniela
    destinos = [TELEGRAM_CHAT_ID_JPAULO]
    if funcionario in FUNCIONARIAS_FEMININO:
        destinos.insert(0, TELEGRAM_CHAT_ID_FEMININO)

    try:
        enviar_card(destinos, cliente, servico, valor, data_br, validar_hora(hora_inicio), funcionario)
        st.success("Notificações enviadas (Canal Feminino/JPaulo) ✅")
    except Exception as e:
        st.warning(f"Registro salvo, mas falhou o envio do Telegram: {e}")

    # Atualiza lista de clientes e limpa campos
    st.cache_data.clear()
    st.rerun()
