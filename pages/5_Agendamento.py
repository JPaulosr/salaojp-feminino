# -*- coding: utf-8 -*-
# 14_Agendamento.py — Agenda com notificações no Telegram + confirmação em lote
# Requisitos:
# - st.secrets["gcp_service_account"] (JSON da Service Account)
# - Permissões de edição na planilha
# - TELEGRAM_TOKEN e CHAT_IDs abaixo (pode mover para st.secrets se preferir)

import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from gspread.utils import rowcol_to_a1
from datetime import datetime, date, time as dt_time
import pytz
import unicodedata
import requests
import random
import string

# =========================
# CONFIG (ajuste conforme seu app)
# =========================
SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"

ABA_DADOS_FEM = "Base de Dados Feminino"      # sua base feminina
ABA_STATUS_FEM = "clientes_status_feminino"   # opcional, se existir
ABA_AGENDAMENTO = "Agendamento"               # nova/atual aba de agenda

TZ = "America/Sao_Paulo"
DATA_FMT = "%d/%m/%Y"
HORA_FMT = "%H:%M:%S"

# Telegram (use estes IDs já usados no projeto)
TELEGRAM_TOKEN = "8257359388:AAGayJElTPT0pQadtamVf8LoL7R6EfWzFGE"
CHAT_ID_JPAULO = "493747253"
CHAT_ID_FEMININO = "-1002965378062"  # Canal Salão JP Feminino

# Funcionário padrão (feminino)
FUNCIONARIOS_FEM = ["Meire", "Daniela"]
FUNCIONARIO_PADRAO = "Meire"

# =========================
# Utils
# =========================
def tz_now():
    return datetime.now(pytz.timezone(TZ))

def norm(s: str) -> str:
    if not isinstance(s, str): return ""
    s = s.strip().lower()
    s = "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))
    return s

def periodo_por_hora(hh: int) -> str:
    if 5 <= hh < 12: return "Manhã"
    if 12 <= hh < 18: return "Tarde"
    return "Noite"

def novo_id(prefix="AG"):
    base = tz_now().strftime("%Y%m%d%H%M%S")
    rand = "".join(random.choices(string.ascii_uppercase + string.digits, k=4))
    return f"{prefix}-{base}-{rand}"

def send_telegram(text: str):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        for chat_id in (CHAT_ID_FEMININO, CHAT_ID_JPAULO):
            payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
            requests.post(url, json=payload, timeout=10)
    except Exception as e:
        st.warning(f"Falha ao notificar no Telegram: {e}")

# =========================
# Conexão Sheets
# =========================
@st.cache_resource(show_spinner=False)
def conectar_sheets():
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ])
    return gspread.authorize(creds)

gc = conectar_sheets()
sh = gc.open_by_key(SHEET_ID)

def abrir_ws(nome_aba: str):
    try:
        return sh.worksheet(nome_aba)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=nome_aba, rows=1000, cols=30)

# =========================
# Estruturas e carregamento
# =========================
COLS_AGENDA = [
    "IDAgenda", "Data", "Hora", "Cliente", "Serviço", "Valor", "Conta",
    "Funcionário", "Combo", "Observação", "Status", "Criado_em", "Atendido_em"
]

def garantir_estrutura_agenda():
    ws = abrir_ws(ABA_AGENDAMENTO)
    df = get_as_dataframe(ws, evaluate_formulas=False, header=0)
    if df.empty or list(df.columns) != COLS_AGENDA:
        # reescreve cabeçalho (preserva dados se existirem e colunas baterem)
        ws.clear()
        ws.update(rowcol_to_a1(1, 1), [COLS_AGENDA])

garantir_estrutura_agenda()

def carregar_df(aba: str) -> pd.DataFrame:
    ws = abrir_ws(aba)
    df = get_as_dataframe(ws, evaluate_formulas=False, header=0)
    df = df.dropna(how="all")
    # Ajusta colunas vazias
    if aba == ABA_AGENDAMENTO and not df.empty:
        faltantes = [c for c in COLS_AGENDA if c not in df.columns]
        for c in faltantes: df[c] = ""
        df = df[COLS_AGENDA]
    return df

def salvar_df(aba: str, df: pd.DataFrame):
    ws = abrir_ws(aba)
    if df.empty:
        ws.clear()
        ws.update(rowcol_to_a1(1, 1), [list(df.columns)])
        return
    ws.clear()
    set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)

def clientes_existentes() -> list:
    nomes = set()
    # Base feminina
    try:
        df = carregar_df(ABA_DADOS_FEM)
        if "Cliente" in df.columns:
            for x in df["Cliente"].dropna().astype(str):
                nomes.add(x.strip())
    except Exception:
        pass
    # clientes_status_feminino (se existir)
    try:
        df2 = carregar_df(ABA_STATUS_FEM)
        for col in df2.columns:
            if norm(col) in ("cliente","nome","nome_cliente"):
                for x in df2[col].dropna().astype(str):
                    nomes.add(x.strip())
                break
    except Exception:
        pass
    return sorted(nomes, key=lambda s: norm(s))

# =========================
# UI
# =========================
st.title("📅 Agendamento (Feminino)")

acao = st.radio(
    "Ações:",
    ["➕ Agendar", "✅ Confirmar atendimentos", "🗂️ Em aberto & exportação"],
    horizontal=False,
)

# ---------- 1) AGENDAR ----------
if acao.startswith("➕"):
    st.subheader("Novo agendamento")

    # Data & hora
    colA, colB, colC = st.columns([1,1,2])
    data_ag = colA.date_input("Data", value=date.today())
    hora_ag = colB.time_input("Hora", value=dt_time(9, 0, 0), step=300)
    funcionario = colC.selectbox("Funcionário", options=FUNCIONARIOS_FEM, index=FUNCIONARIOS_FEM.index(FUNCIONARIO_PADRAO))

    # Cliente (autocomplete com existentes + campo para novo)
    clientes = clientes_existentes()
    cli_opcoes = ["(digite novo cliente)"] + clientes
    cliente_sel = st.selectbox("Cliente", cli_opcoes, index=1 if clientes else 0)
    cliente_txt = ""
    if cliente_sel == "(digite novo cliente)":
        cliente_txt = st.text_input("Novo cliente")
    cliente_final = (cliente_txt or cliente_sel).strip()

    col1, col2 = st.columns([2,1])
    servico = col1.text_input("Serviço", placeholder="Ex.: Escova, Unha pé+mão, Progressiva…")
    valor = col2.text_input("Valor (R$)", placeholder="Ex.: 35,00")
    col3, col4 = st.columns([1,1])
    conta = col3.text_input("Conta / Forma de pagamento", value="Carteira")
    combo = col4.text_input("Combo (opcional)", placeholder="Ex.: corte+barba")

    obs = st.text_area("Observação (opcional)", placeholder="Preferências, referências, etc.")

    if st.button("Agendar e notificar", type="primary", use_container_width=True):
        if not cliente_final:
            st.error("Informe o cliente.")
        elif not servico:
            st.error("Informe o serviço.")
        else:
            garantir_estrutura_agenda()
            df_ag = carregar_df(ABA_AGENDAMENTO)

            ida = novo_id("AG")
            criado_em = tz_now().strftime(f"{DATA_FMT} {HORA_FMT}")
            linha = {
                "IDAgenda": ida,
                "Data": data_ag.strftime(DATA_FMT),
                "Hora": hora_ag.strftime(HORA_FMT),
                "Cliente": cliente_final,
                "Serviço": servico,
                "Valor": str(valor).replace(",", ".").strip(),
                "Conta": conta,
                "Funcionário": funcionario,
                "Combo": combo,
                "Observação": obs,
                "Status": "Agendado",
                "Criado_em": criado_em,
                "Atendido_em": ""
            }
            df_ag = pd.concat([df_ag, pd.DataFrame([linha])], ignore_index=True)
            salvar_df(ABA_AGENDAMENTO, df_ag)

            # Telegram
            msg = (
                "📅 <b>Novo agendamento</b>\n"
                f"👤 <b>Cliente:</b> {cliente_final}\n"
                f"🧴 <b>Serviço:</b> {servico}\n"
                f"💳 <b>Conta:</b> {conta}\n"
                f"💲 <b>Valor:</b> {valor}\n"
                f"🧑‍💼 <b>Funcionário:</b> {funcionario}\n"
                f"🗓️ <b>Data/Hora:</b> {linha['Data']} {linha['Hora']}\n"
                f"📝 <b>Obs.:</b> {obs or '-'}\n"
                f"🏷️ <b>ID:</b> {ida}"
            )
            send_telegram(msg)
            st.success("Agendado e notificado com sucesso ✅")

# ---------- 2) CONFIRMAR ----------
elif acao.startswith("✅"):
    st.subheader("Confirmar atendimentos (lote)")

    df_ag = carregar_df(ABA_AGENDAMENTO)
    if df_ag.empty or not (df_ag["Status"] == "Agendado").any():
        st.info("Nenhum agendamento em aberto.")
    else:
        # Apenas abertos
        em_aberto = df_ag[df_ag["Status"] == "Agendado"].copy()
        # Campos editáveis para confirmação
        em_aberto["Selecionar"] = False
        # Conversão segura de valor
        def fix_val(v):
            s = str(v).strip().replace(",", ".")
            try:
                return round(float(s), 2)
            except:
                return ""

        em_aberto["Valor"] = em_aberto["Valor"].apply(fix_val)

        st.caption("Edite o que for necessário antes de confirmar.")
        edit = st.data_editor(
            em_aberto,
            column_config={
                "Selecionar": st.column_config.CheckboxColumn("Selecionar", help="Marque para confirmar"),
                "Valor": st.column_config.NumberColumn("Valor (R$)", step=0.5, format="%.2f"),
                "Serviço": st.column_config.TextColumn("Serviço"),
                "Conta": st.column_config.TextColumn("Conta"),
                "Combo": st.column_config.TextColumn("Combo"),
                "Observação": st.column_config.TextColumn("Observação"),
            },
            disabled=["IDAgenda", "Data", "Hora", "Cliente", "Funcionário", "Status", "Criado_em", "Atendido_em"],
            use_container_width=True,
            height=420,
            key="editor_confirm"
        )

        colx, coly = st.columns([1,1])
        marcar_todos = colx.checkbox("Marcar todos visíveis")
        if marcar_todos:
            edit["Selecionar"] = True
        btn = coly.button("Confirmar selecionados e lançar na Base", type="primary", use_container_width=True)

        if btn:
            selecionar = edit[edit["Selecionar"] == True]
            if selecionar.empty:
                st.warning("Selecione pelo menos um agendamento.")
            else:
                # Carrega base feminina para append
                df_base = carregar_df(ABA_DADOS_FEM)
                cols_base = list(df_base.columns) if not df_base.empty else [
                    "Data","Serviço","Valor","Conta","Cliente","Combo","Funcionário",
                    "Fase","Tipo","Período","StatusFiado","IDLancFiado","VencimentoFiado",
                    "DataPagamento","Fiado_Vencimento","Fiado_Status","Quitado_em","Observação"
                ]
                if df_base.empty:
                    df_base = pd.DataFrame(columns=cols_base)

                registros_novos = []
                ids_atendidos = []

                for _, row in selecionar.iterrows():
                    # Monta lançamento na Base
                    data_txt = str(row["Data"])
                    hora_txt = str(row["Hora"])
                    try:
                        hh = int(hora_txt.split(":")[0])
                    except:
                        hh = 9
                    periodo = periodo_por_hora(hh)

                    servico = str(row["Serviço"]).strip()
                    # Serviço com primeira letra maiúscula (sem mudar combo minúsculo)
                    if servico:
                        servico = servico[:1].upper() + servico[1:]

                    valor_str = str(row["Valor"]).replace(",", ".").strip()
                    try:
                        valor_float = float(valor_str)
                    except:
                        valor_float = 0.0

                    novo = {
                        "Data": data_txt,
                        "Serviço": servico,
                        "Valor": valor_float,
                        "Conta": str(row["Conta"]).strip() or "Carteira",
                        "Cliente": str(row["Cliente"]).strip(),
                        "Combo": str(row["Combo"]).strip(),
                        "Funcionário": str(row["Funcionário"]).strip() or FUNCIONARIO_PADRAO,
                        "Fase": "Dono + funcionário",
                        "Tipo": "Serviço",
                        "Período": periodo,
                        # campos de fiado vazios/compatíveis
                        "StatusFiado": "",
                        "IDLancFiado": "",
                        "VencimentoFiado": "",
                        "DataPagamento": "",
                        "Fiado_Vencimento": "",
                        "Fiado_Status": "",
                        "Quitado_em": "",
                        "Observação": str(row["Observação"]).strip(),
                    }
                    # Garante todas as colunas da base
                    for c in cols_base:
                        if c not in novo:
                            novo[c] = ""

                    registros_novos.append(novo)
                    ids_atendidos.append(row["IDAgenda"])

                # Append na Base
                df_base = pd.concat([df_base, pd.DataFrame(registros_novos)], ignore_index=True)
                salvar_df(ABA_DADOS_FEM, df_base)

                # Atualiza status na Agenda
                df_ag = carregar_df(ABA_AGENDAMENTO)
                agora_txt = tz_now().strftime(f"{DATA_FMT} {HORA_FMT}")
                df_ag.loc[df_ag["IDAgenda"].isin(ids_atendidos), "Status"] = "Atendido"
                df_ag.loc[df_ag["IDAgenda"].isin(ids_atendidos), "Atendido_em"] = agora_txt
                salvar_df(ABA_AGENDAMENTO, df_ag)

                # Telegram (resumo)
                qtd = len(ids_atendidos)
                msg = f"✅ <b>Atendimentos confirmados</b>\n🗂️ {qtd} registro(s) lançado(s) na Base de Dados Feminino."
                send_telegram(msg)

                st.success(f"{qtd} atendimento(s) confirmados e lançados na base.")

# ---------- 3) EM ABERTO ----------
else:
    st.subheader("Agendamentos em aberto")
    df_ag = carregar_df(ABA_AGENDAMENTO)
    if df_ag.empty:
        st.info("Nenhum agendamento cadastrado.")
    else:
        abertos = df_ag[df_ag["Status"] == "Agendado"].copy()
        if abertos.empty:
            st.success("Sem agendamentos em aberto 🎉")
        else:
            # Ordena por data/hora
            def dt_key(r):
                try:
                    d = datetime.strptime(str(r["Data"]), DATA_FMT)
                    h = datetime.strptime(str(r["Hora"]), HORA_FMT).time()
                    return datetime.combine(d.date(), h)
                except:
                    return datetime.max
            abertos["__ord"] = abertos.apply(dt_key, axis=1)
            abertos = abertos.sort_values("__ord").drop(columns="__ord")

            st.dataframe(
                abertos[["IDAgenda","Data","Hora","Cliente","Serviço","Valor","Funcionário","Conta","Combo","Observação"]],
                use_container_width=True,
                hide_index=True,
            )

            # Export
            csv = abertos.to_csv(index=False).encode("utf-8-sig")
            st.download_button("Baixar CSV", data=csv, file_name="agendamentos_em_aberto.csv", mime="text/csv")
