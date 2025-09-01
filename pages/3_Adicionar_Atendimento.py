# -*- coding: utf-8 -*-
# pages/3_Adicionar_Atendimento.py
# ------------------------------------------------------------
# Adicionar atendimento (Feminino) com:
# - Conexão Google Sheets via Service Account nos secrets
# - carregar_base() -> (df, ws)  [corrige o NameError]
# - Serviço simples OU Combo (com edição de valores por item)
# - Forma de pagamento (Conta)
# - Comissão Daniela (%) -> mensagem Telegram com total e comissão
# - Envio opcional ao Telegram (JPaulo / Daniela / canal Feminino)
# ------------------------------------------------------------

import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from gspread.utils import rowcol_to_a1
from datetime import datetime, date
import pytz
import requests
import re

st.set_page_config(page_title="➕ Adicionar Atendimento (Feminino)",
                   page_icon="➕", layout="wide")
st.title("➕ Adicionar Atendimento – Feminino")

# =========================
# CONFIG
# =========================
# Planilha e aba
SHEET_ID  = st.secrets.get("SHEET_ID", "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE").strip()
ABA_DADOS = st.secrets.get("ABA_FEM_BASE", "Base de Dados Feminino").strip()

# Timezone/formatos
TZ = "America/Sao_Paulo"
DATA_FMT = "%d/%m/%Y"
HORA_FMT = "%H:%M:%S"

# Campos oficiais/esperados na aba:
COLS = [
    "Data", "Serviço", "Valor", "Conta", "Cliente", "Combo",
    "Funcionário", "Fase", "Tipo", "Hora Chegada", "Hora Início",
    "Hora Saída", "Hora Saída do Salão"
]

# Telegram (opcionais – se ausentes, apenas não envia)
TELEGRAM_TOKEN              = st.secrets.get("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID_JPAULO     = st.secrets.get("TELEGRAM_CHAT_ID_JPAULO", "").strip()
TELEGRAM_CHAT_ID_DANIELA    = st.secrets.get("TELEGRAM_CHAT_ID_DANIELA", "").strip()
TELEGRAM_CHAT_ID_FEMININO   = st.secrets.get("TELEGRAM_CHAT_ID_FEMININO", "").strip()
# Observação: não envia para Meire, conforme pedido


# =========================
# CONEXÃO COM GOOGLE SHEETS
# =========================
@st.cache_resource(show_spinner=False)
def _gs_client():
    sa_info = st.secrets.get("GCP_SERVICE_ACCOUNT") or st.secrets.get("gcp_service_account")
    if not sa_info:
        st.error("⚠️ Credenciais ausentes: adicione 'GCP_SERVICE_ACCOUNT' (JSON) nos Secrets.")
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
    try:
        return gc.open_by_key(sheet_id)
    except Exception as e:
        st.error(f"Não consegui abrir a planilha (SHEET_ID={sheet_id}). Detalhes: {e}")
        st.stop()

@st.cache_data(show_spinner=False, ttl=60)
def _read_worksheet_as_df(sh, aba_nome: str):
    try:
        ws = sh.worksheet(aba_nome)
    except Exception as e:
        st.error(f"Aba '{aba_nome}' não encontrada. Detalhes: {e}")
        st.stop()
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
    if df is None or df.empty:
        df = pd.DataFrame(columns=COLS)
    else:
        df = df.dropna(how="all")
        df = df.loc[:, ~df.columns.str.contains("^Unnamed", na=False)]
    return df, ws

def carregar_base():
    """
    Retorna (df, worksheet) da aba principal.
    Compatível com: df_existente, _ = carregar_base()
    """
    sh = _open_sheet(SHEET_ID)
    df, ws = _read_worksheet_as_df(sh, ABA_DADOS)

    # Normalizações de nomes
    rename_map = {
        "Servico": "Serviço",
        "Funcionario": "Funcionário",
        "Forma de Pagamento": "Conta",
    }
    df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)

    # Garante todas as colunas oficiais
    for c in COLS:
        if c not in df.columns:
            df[c] = pd.Series(dtype="object")

    # Tipos
    if "Data" in df.columns:
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce", dayfirst=True)
    if "Valor" in df.columns:
        df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce")

    # Limpa strings
    for col in ["Cliente", "Serviço", "Funcionário", "Conta", "Combo", "Fase", "Tipo"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    return df, ws


# =========================
# UTILITÁRIOS
# =========================
def _format_dt(d: date) -> str:
    if pd.isna(d):
        return ""
    return pd.to_datetime(d).strftime(DATA_FMT)

def _validar_hora(h: str) -> bool:
    if not h:
        return True
    return bool(re.fullmatch(r"\d{2}:\d{2}:\d{2}", h))

def _coalesce(x, default=""):
    return x if (x is not None and str(x).strip() != "nan") else default

def _send_telegram(texto: str, chat_id: str) -> None:
    if not TELEGRAM_TOKEN or not chat_id:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": chat_id, "text": texto, "parse_mode": "HTML"}, timeout=10)
    except Exception:
        pass  # não quebra o fluxo do app


def _append_rows(ws, rows: list):
    """
    rows: lista de listas, cada sublista na ordem das COLS.
    Se a planilha estiver com header na primeira linha, insere ao final.
    """
    # Descobre última linha não-vazia
    sheet_values = ws.get_all_values()
    last_row = len(sheet_values)
    start_row = max(last_row + 1, 2)  # preserva header na linha 1
    start_col = 1
    end_row = start_row + len(rows) - 1
    end_col = len(COLS)
    rng = f"{rowcol_to_a1(start_row, start_col)}:{rowcol_to_a1(end_row, end_col)}"
    ws.update(rng, rows, value_input_option="USER_ENTERED")


# =========================
# CARREGAMENTO INICIAL
# =========================
with st.spinner("Carregando base..."):
    df_existente, ws = carregar_base()

# Listas para selects (com fallback seguro)
clientes = sorted([c for c in df_existente["Cliente"].dropna().unique() if str(c).strip()], key=str.lower)
servicos = sorted([s for s in df_existente["Serviço"].dropna().unique() if str(s).strip()], key=str.lower)
combos   = sorted([c for c in df_existente["Combo"].dropna().unique() if str(c).strip()], key=str.lower)
contas   = sorted([c for c in df_existente["Conta"].dropna().unique() if str(c).strip()], key=str.lower)
funcs    = sorted([f for f in df_existente["Funcionário"].dropna().unique() if str(f).strip()], key=str.lower)

# Defaults amigáveis
if not contas:
    contas = ["Carteira", "Pix", "Cartão Débito", "Cartão Crédito"]
if not funcs:
    funcs = ["Daniela", "Equipe Feminino"]


# =========================
# FORMULÁRIO
# =========================
st.subheader("🧾 Registro de Atendimento")

colA, colB, colC = st.columns(3)
with colA:
    data_atd = st.date_input("Data", value=date.today())
with colB:
    cliente = st.selectbox("Cliente", options=[""] + clientes, index=0, help="Digite para buscar. Pode digitar um nome novo.")
with colC:
    conta = st.selectbox("Conta (forma de pagamento)", options=contas, index=0)

col1, col2 = st.columns(2, gap="large")
with col1:
    tipo_registro = st.radio("Tipo de registro", ["Serviço simples", "Combo"], horizontal=True)

with col2:
    funcionario = st.selectbox("Funcionário", options=funcs, index=0)

col_horas = st.expander("⏱️ Horários (opcional – formato HH:MM:SS)")
with col_horas:
    colh1, colh2, colh3, colh4 = st.columns(4)
    hora_chegada = colh1.text_input("Hora Chegada", value="")
    hora_inicio  = colh2.text_input("Hora Início", value="")
    hora_saida   = colh3.text_input("Hora Saída da Cadeira", value="")
    hora_final   = colh4.text_input("Hora Saída do Salão", value="")

# Comissão Daniela (%)
col_com = st.container()
with col_com:
    colp1, colp2 = st.columns([1,1])
    comissao_pct = colp1.number_input("Comissão Daniela (%)", min_value=0.0, max_value=100.0, value=0.0, step=0.5,
                                      help="Se preencher, a mensagem do Telegram mostrará o total e o valor da comissão.")
    fase = colp2.selectbox("Fase", options=["Dono + funcionário", "Dono (sozinho)", "Autônomo (prestador)"], index=0)

st.markdown("---")

itens_registro = []
total_registro = 0.0

if tipo_registro == "Serviço simples":
    colS1, colS2 = st.columns([2,1])
    with colS1:
        servico_sel = st.selectbox("Serviço", options=[""] + servicos, index=0)
    with colS2:
        # Sugere último valor usado para este serviço
        valor_default = 0.0
        if servico_sel and servico_sel in df_existente["Serviço"].values:
            ult = df_existente[df_existente["Serviço"] == servico_sel]["Valor"].dropna()
            if not ult.empty:
                valor_default = float(ult.iloc[-1])
        valor_servico = st.number_input("Valor (R$)", min_value=0.0, value=float(valor_default), step=1.0)

    # Monta um único item
    if servico_sel:
        itens_registro.append({"Serviço": servico_sel, "Valor": float(valor_servico)})
        total_registro = float(valor_servico)

else:
    st.markdown("**Combo selecionado**: escolha um dos combos existentes ou digite um novo (separe serviços por `+`).")
    colC1, colC2 = st.columns([2,2])
    with colC1:
        combo_sel = st.selectbox("Combo", options=[""] + combos, index=0, help="Ex.: 'escova+manicure'")

    with colC2:
        combo_custom = st.text_input("Novo combo (opcional)", value="",
                                     placeholder="Ex.: progressiva+escova")

    combo_final = (combo_custom or combo_sel or "").strip()
    # explode combo
    itens_combo = []
    if combo_final:
        partes = [p.strip() for p in combo_final.split("+") if p.strip()]
        for p in partes:
            # sugere último valor do serviço p
            valor_padrao = 0.0
            ult = df_existente[df_existente["Serviço"] == p]["Valor"].dropna()
            if not ult.empty:
                valor_padrao = float(ult.iloc[-1])
            itens_combo.append({"Serviço": p, "Valor": valor_padrao})

    st.write("### Itens do combo")
    editar = []
    for i, it in enumerate(itens_combo):
        c1, c2 = st.columns([3,1])
        with c1:
            srv = st.text_input(f"Serviço #{i+1}", value=it["Serviço"])
        with c2:
            val = st.number_input(f"Valor R$ #{i+1}", min_value=0.0, value=float(it["Valor"]), step=1.0, key=f"val_combo_{i}")
        editar.append({"Serviço": srv.strip(), "Valor": float(val)})

    editar = [e for e in editar if e["Serviço"]]
    itens_registro = editar
    total_registro = float(sum(e["Valor"] for e in editar))
    combo_final = combo_final if combo_final else ""

# Preview do total e comissão
st.info(f"**Total do atendimento:** R$ {total_registro:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
valor_comissao = (comissao_pct / 100.0) * total_registro if comissao_pct and total_registro else 0.0
if comissao_pct > 0:
    st.success(f"Comissão Daniela ({comissao_pct:.1f}%): R$ {valor_comissao:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

st.markdown("---")

# =========================
# SALVAR
# =========================
btn = st.button("💾 Salvar atendimento", type="primary", use_container_width=True)

def _montar_linha_base(servico:str, valor:float, combo_texto:str) -> list:
    """Retorna uma linha completa na ordem das COLS."""
    data_txt = _format_dt(data_atd)
    return [
        data_txt,                      # Data
        servico,                       # Serviço
        valor,                         # Valor
        conta,                         # Conta
        cliente.strip() if cliente else "",  # Cliente
        combo_texto,                   # Combo
        funcionario,                   # Funcionário
        fase,                          # Fase
        "Serviço",                     # Tipo
        hora_chegada,                  # Hora Chegada
        hora_inicio,                   # Hora Início
        hora_saida,                    # Hora Saída
        hora_final                     # Hora Saída do Salão
    ]

def _validar_form() -> bool:
    # Data ok
    if not data_atd:
        st.error("Informe a Data.")
        return False
    # Cliente
    if not cliente or not str(cliente).strip():
        st.error("Informe o Cliente.")
        return False
    # Conta
    if not conta or not str(conta).strip():
        st.error("Informe a Conta (forma de pagamento).")
        return False
    # Serviços
    if not itens_registro:
        st.error("Informe ao menos um serviço.")
        return False
    # Horários (se preenchidos, validar formato)
    for campo, valor in [("Hora Chegada", hora_chegada), ("Hora Início", hora_inicio),
                         ("Hora Saída", hora_saida), ("Hora Saída do Salão", hora_final)]:
        if valor and not _validar_hora(valor):
            st.error(f"{campo} inválida. Use HH:MM:SS.")
            return False
    return True


if btn:
    if _validar_form():
        linhas = []
        combo_texto = ""
        if tipo_registro == "Combo":
            # usa texto digitado ou selecionado; se vazio, monta a partir da lista
            if not combo_final and itens_registro:
                combo_texto = "+".join([e["Serviço"] for e in itens_registro])
            else:
                combo_texto = combo_final
        else:
            combo_texto = ""

        # Gera linhas
        for item in itens_registro:
            linhas.append(_montar_linha_base(item["Serviço"], item["Valor"], combo_texto if tipo_registro=="Combo" else ""))

        try:
            _append_rows(ws, linhas)
        except Exception as e:
            st.error(f"Falha ao salvar no Sheets: {e}")
            st.stop()

        # Mensagens Telegram (opcionais)
        data_txt = _format_dt(data_atd)
        forma_pg = conta
        cliente_txt = cliente.strip() if cliente else ""
        funcionario_txt = funcionario

        # Mensagem detalhada (JPaulo)
        itens_str = "\n".join([f"• {it['Serviço']}: R$ {it['Valor']:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                               for it in itens_registro])
        mensagem_jp = (
            f"💅 <b>Atendimento Feminino registrado</b>\n"
            f"📅 {data_txt}\n"
            f"👤 Cliente: {cliente_txt}\n"
            f"👩‍🦰 Funcionário: {funcionario_txt}\n"
            f"💳 Forma de pagamento: {forma_pg}\n"
            f"{'🧩 Combo: ' + combo_texto + '\n' if tipo_registro=='Combo' and combo_texto else ''}"
            f"🧾 Itens:\n{itens_str}\n"
            f"—\n"
            f"💰 <b>Total:</b> R$ {total_registro:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        )
        if comissao_pct > 0:
            mensagem_jp += (
                f"\n💼 Comissão Daniela ({comissao_pct:.1f}%): "
                f"R$ {valor_comissao:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            )

        _send_telegram(mensagem_jp, TELEGRAM_CHAT_ID_JPAULO)

        # Mensagem Daniela (se houver comissão)
        if comissao_pct > 0 and TELEGRAM_CHAT_ID_DANIELA:
            mensagem_daniela = (
                f"💅 <b>Resumo do atendimento</b>\n"
                f"📅 {data_txt}\n"
                f"👤 Cliente: {cliente_txt}\n"
                f"💳 Pagamento: {forma_pg}\n"
                f"💰 Total: R$ {total_registro:,.2f}\n"
                f"💼 Sua comissão ({comissao_pct:.1f}%): R$ {valor_comissao:,.2f}"
            ).replace(",", "X").replace(".", ",").replace("X", ".")
            _send_telegram(mensagem_daniela, TELEGRAM_CHAT_ID_DANIELA)

        # Mensagem canal Feminino (resumo)
        if TELEGRAM_CHAT_ID_FEMININO:
            mensagem_channel = (
                f"💅 Atendimento registrado\n"
                f"📅 {data_txt} | 👤 {cliente_txt}\n"
                f"💳 {forma_pg} | 💰 Total: R$ {total_registro:,.2f}"
            ).replace(",", "X").replace(".", ",").replace("X", ".")
            _send_telegram(mensagem_channel, TELEGRAM_CHAT_ID_FEMININO)

        st.success("✅ Atendimento salvo com sucesso!")
        st.balloons()
        st.rerun()
