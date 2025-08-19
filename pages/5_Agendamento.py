# -*- coding: utf-8 -*-
# 11_Adicionar_Atendimento.py — Registro + Notificações (Feminino + JPaulo)
# - Cliente com select (autocomplete) e opção "➕ Cadastrar novo cliente"
# - Se novo, opcionalmente adiciona em clientes_status
# - Registro na "Base de Dados"
# - Notificação só para Canal Feminino (funcionárias femininas) e JPaulo
# - Se houver link de foto em clientes_status, envia como foto

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

# =========================
# CONFIG
# =========================
SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"
ABA_DADOS = "Base de Dados"
STATUS_ABA = "clientes_status"
FOTO_COL_CANDIDATES = ["link_foto", "foto", "imagem", "url_foto", "foto_link", "link", "image"]

# Timezone/formatos
TZ = "America/Sao_Paulo"
DATA_FMT_BR = "%d/%m/%Y"
HORA_FMT = "%H:%M:%S"

# Funcionárias do salão feminino (ajuste os nomes conforme sua planilha)
FUNCIONARIAS_FEMININO = {"Daniela", "Equipe Feminina", "Feminino", "Outro_Feminino"}

# --- Telegram (mantenha em st.secrets em produção) ---
TELEGRAM_TOKEN = st.secrets.get("TELEGRAM_TOKEN", "8257359388:AAGayJElTPT0pQadtamVf8LoL7R6EfWzFGE")
TELEGRAM_CHAT_ID_JPAULO = st.secrets.get("TELEGRAM_CHAT_ID_JPAULO", "493747253")
TELEGRAM_CHAT_ID_FEMININO = st.secrets.get("TELEGRAM_CHAT_ID_FEMININO", "-1002965378062")

# =========================
# GOOGLE SHEETS
# =========================
@st.cache_resource
def conectar_sheets():
    if "GCP_SERVICE_ACCOUNT" in st.secrets:
        info = dict(st.secrets["GCP_SERVICE_ACCOUNT"])
        creds = Credentials.from_service_account_info(
            info,
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"]
        )
    else:
        st.stop()  # configure st.secrets["GCP_SERVICE_ACCOUNT"]
    gc = gspread.authorize(creds)
    return gc

def abrir_aba(gc, sheet_id: str, aba_nome: str):
    sh = gc.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(aba_nome)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=aba_nome, rows=1000, cols=30)
    return ws

def _norm(s: str) -> str:
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join([c for c in s if not unicodedata.combining(c)])
    return s

@st.cache_data(ttl=300)
def listar_clientes_existentes(gc) -> list[str]:
    """Lista única de clientes vindos da Base e clientes_status (ordenada)."""
    nomes = set()
    # Base de Dados
    ws_base = abrir_aba(gc, SHEET_ID, ABA_DADOS)
    df_base = get_as_dataframe(ws_base, evaluate_formulas=True, header=0).dropna(how="all")
    if not df_base.empty and "Cliente" in df_base.columns:
        for v in df_base["Cliente"].dropna().astype(str):
            vv = v.strip()
            if vv:
                nomes.add(vv)
    # clientes_status
    ws_status = abrir_aba(gc, SHEET_ID, STATUS_ABA)
    df_status = get_as_dataframe(ws_status, evaluate_formulas=True, header=0).dropna(how="all")
    if not df_status.empty and "Cliente" in df_status.columns:
        for v in df_status["Cliente"].dropna().astype(str):
            vv = v.strip()
            if vv:
                nomes.add(vv)
    # Ordena de forma amigável (casefold)
    return sorted(nomes, key=lambda x: x.casefold())

def carregar_fotos_mapa(gc) -> dict:
    """Retorna {nome_normalizado: url_foto} a partir da aba clientes_status."""
    ws = abrir_aba(gc, SHEET_ID, STATUS_ABA)
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0).dropna(how="all")
    if df.empty:
        return {}
    # Detecta coluna de foto
    foto_col = None
    cols_lower = {c.lower(): c for c in df.columns if isinstance(c, str)}
    for cand in FOTO_COL_CANDIDATES:
        if cand in cols_lower:
            foto_col = cols_lower[cand]
            break
    if "Cliente" not in df.columns or not foto_col:
        return {}
    fotos = {}
    for _, row in df.iterrows():
        cli = str(row.get("Cliente", "")).strip()
        url = str(row.get(foto_col, "")).strip()
        if cli and url and url.startswith(("http://", "https://")):
            fotos[_norm(cli)] = url
    return fotos

def garantir_cliente_no_status(gc, nome: str):
    """Garante que o cliente exista em clientes_status (sem duplicar)."""
    ws = abrir_aba(gc, SHEET_ID, STATUS_ABA)
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
    if df is None or df.empty:
        df = pd.DataFrame(columns=["Cliente", "status", "link_foto"])
    # Verifica se já existe (normalização simples por trim/case)
    if "Cliente" in df.columns:
        ja_existe = df["Cliente"].astype(str).str.strip().str.casefold().eq(nome.strip().casefold()).any()
        if ja_existe:
            return
    # Monta nova linha respeitando colunas existentes
    nova = {c: "" for c in df.columns}
    if "Cliente" in df.columns:
        nova["Cliente"] = nome.strip()
    if "status" in df.columns:
        nova["status"] = "Ativo"
    if "Status" in df.columns:
        nova["Status"] = "Ativo"
    df_out = pd.concat([df, pd.DataFrame([nova])], ignore_index=True)
    ws.clear()
    set_with_dataframe(ws, df_out)

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

def _send_telegram_photo(chat_id: str, photo_url: str, caption_html: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    data = {"chat_id": chat_id, "photo": photo_url, "caption": caption_html, "parse_mode": "HTML"}
    try:
        requests.post(url, data=data, timeout=12)
    except Exception as e:
        st.warning(f"Falha ao enviar foto para {chat_id}: {e}")

def enviar_card_atendimento(destinos: list, cliente: str, servico: str, valor: float,
                            data_br: str, hora_ini: str | None, funcionario: str,
                            foto_url: str | None):
    # Serviço exibido com primeira letra maiúscula (base segue como digitado)
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
        if foto_url:
            _send_telegram_photo(chat_id, foto_url, card)
        else:
            _send_telegram_message(chat_id, card)

# =========================
# APP
# =========================
st.set_page_config(page_title="Adicionar Atendimento — Salão JP", layout="wide")
st.title("➕ Adicionar Atendimento (Salão JP)")
st.caption("Selecione um cliente existente ou cadastre um novo. Notificações: Canal Feminino (se aplicável) + JPaulo.")

gc = conectar_sheets()
ws_base = abrir_aba(gc, SHEET_ID, ABA_DADOS)

# ------- Formulário -------
col1, col2, col3, col4 = st.columns(4)
with col1:
    data_input = st.date_input("Data", value=datetime.now(pytz.timezone(TZ)).date())
with col2:
    hora_chegada = st.text_input("Hora Chegada (HH:MM:SS)", value="")
with col3:
    hora_inicio = st.text_input("Hora Início (HH:MM:SS)", value="")
with col4:
    hora_saida = st.text_input("Hora Saída (HH:MM:SS)", value="")

# Cliente (select + novo)
clientes_opcoes = ["➕ Cadastrar novo cliente"]
try:
    clientes_opcoes += listar_clientes_existentes(gc)
except Exception:
    pass  # se der erro, fica só com a opção de novo

col5, col6, col7, col8 = st.columns(4)
with col5:
    escolha_cliente = st.selectbox("Cliente (digite para buscar)", options=clientes_opcoes, index=1 if len(clientes_opcoes) > 1 else 0)
    if escolha_cliente == "➕ Cadastrar novo cliente":
        novo_cliente = st.text_input("Novo cliente", placeholder="Nome completo").strip()
        add_no_status = st.checkbox("Adicionar ao cadastro de clientes (clientes_status)", value=True)
    else:
        novo_cliente = ""
        add_no_status = False

with col6:
    servico = st.text_input("Serviço", placeholder="Ex.: progressiva ou corte+escova").strip()
with col7:
    valor = st.number_input("Valor (R$)", min_value=0.0, step=1.0, format="%.2f")
with col8:
    conta = st.text_input("Forma de Pagamento", placeholder="Ex.: Dinheiro / Cartão / Pix").strip()

col9, col10, col11, col12 = st.columns(4)
with col9:
    combo = st.text_input("Combo (opcional)", placeholder="Ex.: corte+barba").strip()
with col10:
    funcionario = st.text_input("Funcionário", placeholder="Ex.: Daniela / Vinicius / JPaulo").strip()
with col11:
    fase = st.text_input("Fase", value="Dono + funcionário").strip()
with col12:
    tipo = st.text_input("Tipo", value="Serviço").strip()

btn_salvar = st.button("💾 Salvar atendimento", type="primary")

def validar_hora(h: str) -> str | None:
    if not h:
        return None
    try:
        datetime.strptime(h, HORA_FMT)
        return h
    except Exception:
        return None

if btn_salvar:
    # Determina cliente final
    cliente_final = (novo_cliente if escolha_cliente == "➕ Cadastrar novo cliente" else escolha_cliente).strip()

    if not cliente_final or not servico or valor <= 0:
        st.error("Preencha Cliente, Serviço e Valor corretamente.")
        st.stop()

    # Monta registro
    data_br = data_input.strftime(DATA_FMT_BR)
    row = {
        "Data": data_br,
        "Serviço": servico,              # mantém como digitado (minúsculo e/ou '+')
        "Valor": valor,
        "Conta": conta,
        "Cliente": cliente_final,
        "Combo": combo,
        "Funcionário": funcionario,
        "Fase": fase,
        "Tipo": tipo,
        "Período": data_input.strftime("%m/%Y"),
        "Hora Chegada": validar_hora(hora_chegada) or "",
        "Hora Início": validar_hora(hora_inicio) or "",
        "Hora Saída": validar_hora(hora_saida) or "",
        "Hora Saída do Salão": ""
    }

    # Append na Base
    try:
        df_base = get_as_dataframe(ws_base, evaluate_formulas=True, header=0)
        if df_base is None or df_base.empty:
            df_base = pd.DataFrame(columns=list(row.keys()))
        df_out = pd.concat([df_base, pd.DataFrame([row])], ignore_index=True)
        ws_base.clear()
        set_with_dataframe(ws_base, df_out)
        st.success("Atendimento salvo na planilha ✅")
    except Exception as e:
        st.error(f"Erro ao salvar na planilha: {e}")
        st.stop()

    # Se novo cliente, adiciona no cadastro (opcional)
    if escolha_cliente == "➕ Cadastrar novo cliente" and add_no_status:
        try:
            garantir_cliente_no_status(gc, cliente_final)
        except Exception as e:
            st.warning(f"Atendimento ok, mas não foi possível cadastrar cliente em clientes_status: {e}")

    # Foto do cliente (se houver)
    fotos = carregar_fotos_mapa(gc)
    foto_url = fotos.get(_norm(cliente_final))

    # Destinos de notificação
    destinos = [TELEGRAM_CHAT_ID_JPAULO]  # sempre você
    if funcionario in FUNCIONARIAS_FEMININO:
        destinos.insert(0, TELEGRAM_CHAT_ID_FEMININO)  # também canal feminino

    # Envia card
    try:
        enviar_card_atendimento(
            destinos=destinos,
            cliente=cliente_final,
            servico=servico,
            valor=valor,
            data_br=data_br,
            hora_ini=validar_hora(hora_inicio),
            funcionario=funcionario,
            foto_url=foto_url
        )
        st.success("Notificações enviadas (Feminino/JPaulo) ✅")
    except Exception as e:
        st.warning(f"Registro salvo, mas houve falha ao notificar: {e}")

    # Limpa cache de lista de clientes e recarrega a página
    st.cache_data.clear()
    st.rerun()
