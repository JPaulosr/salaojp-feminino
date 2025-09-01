# -*- coding: utf-8 -*-
# pages/3_Adicionar_Atendimento.py
# ------------------------------------------------------------
# Adicionar atendimento (Feminino) com:
# - Conexão Google Sheets via Service Account nos secrets
# - carregar_base() -> (df, ws)
# - Serviço simples OU Combo (com edição de valores por item)
# - Forma de pagamento (Conta)
# - Comissão Daniela (%) -> mensagem Telegram com total e comissão
# - Envio opcional ao Telegram (JPaulo / Daniela / canal Feminino)
# ------------------------------------------------------------

import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe
from gspread.utils import rowcol_to_a1
from datetime import datetime, date
import requests
import re

st.set_page_config(page_title="➕ Adicionar Atendimento (Feminino)",
                   page_icon="➕", layout="wide")
st.title("➕ Adicionar Atendimento – Feminino")

# =========================
# CONFIG
# =========================
SHEET_ID  = st.secrets.get("SHEET_ID", "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE").strip()
ABA_DADOS = st.secrets.get("ABA_FEM_BASE", "Base de Dados Feminino").strip()

TZ = "America/Sao_Paulo"
DATA_FMT = "%d/%m/%Y"

COLS = [
    "Data", "Serviço", "Valor", "Conta", "Cliente", "Combo",
    "Funcionário", "Fase", "Tipo", "Hora Chegada", "Hora Início",
    "Hora Saída", "Hora Saída do Salão"
]

# Telegram (opcionais)
TELEGRAM_TOKEN            = st.secrets.get("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID_JPAULO   = st.secrets.get("TELEGRAM_CHAT_ID_JPAULO", "").strip()
TELEGRAM_CHAT_ID_DANIELA  = st.secrets.get("TELEGRAM_CHAT_ID_DANIELA", "").strip()
TELEGRAM_CHAT_ID_FEMININO = st.secrets.get("TELEGRAM_CHAT_ID_FEMININO", "").strip()


# =========================
# CONEXÃO GOOGLE SHEETS
# =========================
@st.cache_resource(show_spinner=False)
def _gs_client():
    sa_info = st.secrets.get("GCP_SERVICE_ACCOUNT") or st.secrets.get("gcp_service_account")
    if not sa_info:
        st.error("⚠️ Credenciais ausentes: adicione 'GCP_SERVICE_ACCOUNT' nos Secrets.")
        st.stop()
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
    return gspread.authorize(creds)

@st.cache_resource(show_spinner=False)
def _open_sheet(sheet_id: str):
    gc = _gs_client()
    return gc.open_by_key(sheet_id)

# ❌ sem cache aqui (evita UnhashableParamError)
def _read_worksheet_as_df(sh, aba_nome: str):
    ws = sh.worksheet(aba_nome)
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
    if df is None or df.empty:
        df = pd.DataFrame(columns=COLS)
    else:
        df = df.dropna(how="all")
        df = df.loc[:, ~df.columns.str.contains("^Unnamed", na=False)]
    return df, ws

def carregar_base():
    sh = _open_sheet(SHEET_ID)
    df, ws = _read_worksheet_as_df(sh, ABA_DADOS)

    rename_map = {"Servico": "Serviço", "Funcionario": "Funcionário", "Forma de Pagamento": "Conta"}
    df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)

    for c in COLS:
        if c not in df.columns:
            df[c] = pd.Series(dtype="object")

    if "Data" in df.columns:
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce", dayfirst=True)
    if "Valor" in df.columns:
        df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce")

    for col in ["Cliente", "Serviço", "Funcionário", "Conta", "Combo", "Fase", "Tipo"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    return df, ws


# =========================
# UTILITÁRIOS
# =========================
def _format_dt(d: date) -> str:
    return pd.to_datetime(d).strftime(DATA_FMT) if d else ""

def _validar_hora(h: str) -> bool:
    return not h or bool(re.fullmatch(r"\d{2}:\d{2}:\d{2}", h))

def _send_telegram(texto: str, chat_id: str) -> None:
    if not TELEGRAM_TOKEN or not chat_id:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": chat_id, "text": texto, "parse_mode": "HTML"}, timeout=10)
    except:
        pass

def _append_rows(ws, rows: list):
    sheet_values = ws.get_all_values()
    last_row = len(sheet_values)
    start_row = max(last_row + 1, 2)
    rng = f"{rowcol_to_a1(start_row, 1)}:{rowcol_to_a1(start_row+len(rows)-1, len(COLS))}"
    ws.update(rng, rows, value_input_option="USER_ENTERED")


# =========================
# CARREGAR BASE
# =========================
with st.spinner("Carregando base..."):
    df_existente, ws = carregar_base()

clientes = sorted([c for c in df_existente["Cliente"].dropna().unique() if c.strip()])
servicos = sorted([s for s in df_existente["Serviço"].dropna().unique() if s.strip()])
combos   = sorted([c for c in df_existente["Combo"].dropna().unique() if c.strip()])
contas   = sorted([c for c in df_existente["Conta"].dropna().unique() if c.strip()])
funcs    = sorted([f for f in df_existente["Funcionário"].dropna().unique() if f.strip()])

if not contas:
    contas = ["Carteira", "Pix", "Cartão Débito", "Cartão Crédito"]
if not funcs:
    funcs = ["Daniela"]


# =========================
# FORMULÁRIO
# =========================
st.subheader("🧾 Registro de Atendimento")

colA, colB, colC = st.columns(3)
with colA:
    data_atd = st.date_input("Data", value=date.today())
with colB:
    cliente = st.selectbox("Cliente", options=[""]+clientes)
with colC:
    conta = st.selectbox("Conta (forma de pagamento)", options=contas)

col1, col2 = st.columns(2)
with col1:
    tipo_registro = st.radio("Tipo de registro", ["Serviço simples", "Combo"], horizontal=True)
with col2:
    funcionario = st.selectbox("Funcionário", options=funcs)

col_horas = st.expander("⏱️ Horários (opcional – HH:MM:SS)")
with col_horas:
    hora_chegada = st.text_input("Hora Chegada", "")
    hora_inicio  = st.text_input("Hora Início", "")
    hora_saida   = st.text_input("Hora Saída da Cadeira", "")
    hora_final   = st.text_input("Hora Saída do Salão", "")

comissao_pct = st.number_input("Comissão Daniela (%)", 0.0, 100.0, 0.0, 0.5)
fase = st.selectbox("Fase", ["Dono + funcionário", "Dono (sozinho)", "Autônomo (prestador)"])

st.markdown("---")

itens_registro, total_registro = [], 0.0

if tipo_registro == "Serviço simples":
    servico_sel = st.selectbox("Serviço", options=[""]+servicos)
    valor_padrao = 0.0
    if servico_sel and servico_sel in df_existente["Serviço"].values:
        ult = df_existente[df_existente["Serviço"]==servico_sel]["Valor"].dropna()
        if not ult.empty:
            valor_padrao = float(ult.iloc[-1])
    valor_servico = st.number_input("Valor (R$)", 0.0, 10000.0, valor_padrao, 1.0)
    if servico_sel:
        itens_registro = [{"Serviço": servico_sel, "Valor": valor_servico}]
        total_registro = valor_servico
else:
    combo_sel = st.selectbox("Combo", options=[""]+combos)
    combo_custom = st.text_input("Novo combo (opcional)", "")
    combo_final = combo_custom or combo_sel
    itens_combo = []
    if combo_final:
        for p in [p.strip() for p in combo_final.split("+") if p.strip()]:
            val = 0.0
            ult = df_existente[df_existente["Serviço"]==p]["Valor"].dropna()
            if not ult.empty: val = float(ult.iloc[-1])
            v = st.number_input(f"Valor {p}", 0.0, 10000.0, val, 1.0, key=f"val_{p}")
            itens_combo.append({"Serviço": p, "Valor": v})
    itens_registro, total_registro = itens_combo, sum([i["Valor"] for i in itens_combo])

st.info(f"**Total:** R$ {total_registro:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
valor_comissao = (comissao_pct/100)*total_registro if comissao_pct else 0.0
if comissao_pct:
    st.success(f"Comissão Daniela ({comissao_pct:.1f}%): R$ {valor_comissao:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

# =========================
# SALVAR
# =========================
if st.button("💾 Salvar atendimento", type="primary", use_container_width=True):
    if not cliente or not itens_registro:
        st.error("Preencha cliente e serviços.")
    else:
        linhas = []
        combo_txt = combo_final if tipo_registro=="Combo" else ""
        for it in itens_registro:
            linhas.append([
                _format_dt(data_atd), it["Serviço"], it["Valor"], conta, cliente,
                combo_txt, funcionario, fase, "Serviço",
                hora_chegada, hora_inicio, hora_saida, hora_final
            ])
        _append_rows(ws, linhas)

        # --- Telegram
        data_txt = _format_dt(data_atd)
        itens_str = "\n".join([f"• {i['Serviço']}: R$ {i['Valor']:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") for i in itens_registro])
        msg = (f"💅 <b>Atendimento Feminino</b>\n📅 {data_txt}\n👤 {cliente}\n👩‍🦰 {funcionario}\n💳 {conta}\n"
               f"{'🧩 Combo: '+combo_txt+'\n' if combo_txt else ''}🧾 Itens:\n{itens_str}\n—\n"
               f"💰 Total: R$ {total_registro:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        if comissao_pct:
            msg += f"\n💼 Comissão Daniela ({comissao_pct:.1f}%): R$ {valor_comissao:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        _send_telegram(msg, TELEGRAM_CHAT_ID_JPAULO)
        if comissao_pct and TELEGRAM_CHAT_ID_DANIELA:
            _send_telegram(msg, TELEGRAM_CHAT_ID_DANIELA)
        if TELEGRAM_CHAT_ID_FEMININO:
            _send_telegram(f"💅 Atendimento: {cliente} | {conta} | R$ {total_registro:,.2f}", TELEGRAM_CHAT_ID_FEMININO)

        st.success("✅ Atendimento salvo com sucesso!")
        st.balloons()
        st.rerun()
