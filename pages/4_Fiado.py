# pages/12_Fiado_Meire.py
# --------------------------------------------------------------
# Controle de Fiado — Meire (Feminino)
#   ✓ Usa service account de st.secrets["GCP_SERVICE_ACCOUNT"] OU ["gcp_service_account"]
#   ✓ Se não houver credenciais: leitura via CSV (sem escrita)
# --------------------------------------------------------------

import streamlit as st
import pandas as pd
from datetime import date
from io import BytesIO
import pytz
import urllib.parse

# gspread e auth (opcionais; só serão usados se houver credenciais)
try:
    import gspread
    from gspread_dataframe import get_as_dataframe, set_with_dataframe
    from google.oauth2.service_account import Credentials
except Exception:
    gspread = None
    Credentials = None

st.set_page_config(page_title="Fiado (Meire - Feminino)", page_icon="💳", layout="wide")
st.title("💳 Controle de Fiado — Registro da Meire (Feminino)")
BR_TZ = pytz.timezone("America/Sao_Paulo")

# ID padrão (sua planilha principal)
SHEET_ID_PADRAO = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"
SHEET_ID_MEIRE = st.secrets.get("SHEET_ID_MEIRE", SHEET_ID_PADRAO)
PLANILHA_URL_MEIRE = f"https://docs.google.com/spreadsheets/d/{SHEET_ID_MEIRE}/edit"

ABA_FEMININO_ALVOS = [
    "Base de Dados Feminino",
    "base de dados feminino",
    "base de dados - feminino",
    "Base Feminino",
    "Base de Dados Fem",
]

# ===== NOVO: compatível com [GCP_SERVICE_ACCOUNT] e [gcp_service_account]
def _get_service_account_from_secrets():
    """Retorna o dict do JSON da service account a partir dos secrets, aceitando
    tanto 'GCP_SERVICE_ACCOUNT' (maiúsculas) quanto 'gcp_service_account' (minúsculas)."""
    svc = st.secrets.get("gcp_service_account")
    if not svc:
        svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    return svc

def _tem_auth():
    return _get_service_account_from_secrets() is not None and Credentials is not None and gspread is not None

# ===== Helpers comuns
COLS_OBRIGATORIAS = [
    "Data", "Serviço", "Valor", "Conta", "Cliente", "Combo",
    "Funcionário", "Fase", "Tipo", "Período",
    "Fiado_Vencimento", "Fiado_Status", "Quitado_em", "Observação"
]

def moeda_to_float(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return 0.0
    s = str(v).strip()
    if s == "":
        return 0.0
    s = s.replace("R$", "").replace(" ", "")
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0

def normalizar_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.loc[:, ~df.columns.astype(str).str.contains("^Unnamed")].copy()
    for c in COLS_OBRIGATORIAS:
        if c not in df.columns:
            df[c] = "" if c != "Valor" else 0.0
    if "Valor" in df.columns:
        df["Valor"] = df["Valor"].apply(moeda_to_float)
    if "Data" in df.columns:
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce").dt.date
    if "Fiado_Vencimento" in df.columns:
        df["Fiado_Vencimento"] = pd.to_datetime(df["Fiado_Vencimento"], errors="coerce").dt.date
    if "Quitado_em" in df.columns:
        df["Quitado_em"] = pd.to_datetime(df["Quitado_em"], errors="coerce").dt.date
    if len(df):
        empty_mask = df.fillna("").astype(str).apply(lambda r: "".join(r.values), axis=1) == ""
        df = df.loc[~empty_mask].copy()
    return df

# ===== Caminho 1: com autenticação (le/escreve)
def abrir_planilha_com_auth(spreadsheet_id: str):
    svc = _get_service_account_from_secrets()
    if not svc:
        st.error("Credenciais não encontradas em st.secrets['gcp_service_account'] nem ['GCP_SERVICE_ACCOUNT'].")
        st.stop()
    creds = Credentials.from_service_account_info(
        svc,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    client = gspread.authorize(creds)
    sh = client.open_by_key(spreadsheet_id)
    abas = [ws.title for ws in sh.worksheets()]
    alvo = None
    for possivel in ABA_FEMININO_ALVOS:
        if any(aba.strip().lower() == possivel.strip().lower() for aba in abas):
            alvo = possivel
            break
    if alvo is None:
        alvo = abas[0]
        st.warning(f"Não encontrei uma aba Feminino esperada. Usando a primeira aba: {alvo}")
    ws = sh.worksheet(alvo)
    return sh, ws, abas, alvo

def carregar_df_auth(ws) -> pd.DataFrame:
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0, dtype=str)
    return normalizar_df(df)

def salvar_df_auth(ws, df: pd.DataFrame):
    cols = list(dict.fromkeys(list(df.columns) + COLS_OBRIGATORIAS))
    df = df.reindex(columns=cols)
    set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)

# ===== Caminho 2: sem autenticação (leitura CSV)
def _csv_from_sheet(sheet_name: str) -> pd.DataFrame | None:
    quoted = urllib.parse.quote(sheet_name, safe="")
    url_csv = f"https://docs.google.com/spreadsheets/d/{SHEET_ID_MEIRE}/gviz/tq?tqx=out:csv&sheet={quoted}"
    try:
        df = pd.read_csv(url_csv)
        if len(df.columns) == 1 and df.columns[0].startswith("<!DOCTYPE html"):
            return None
        return df
    except Exception:
        return None

def carregar_df_sem_auth() -> tuple[pd.DataFrame, str]:
    for nome in ABA_FEMININO_ALVOS:
        df = _csv_from_sheet(nome)
        if df is not None:
            return normalizar_df(df), nome
    df = _csv_from_sheet("Base%20de%20Dados")  # último esforço
    if df is not None:
        return normalizar_df(df), "Base de Dados"
    st.error("Não consegui ler a planilha via CSV público. Verifique se a planilha está compartilhada (leitura).")
    st.stop()

# ===== Carregar dados
HAS_AUTH = _tem_auth()
if not HAS_AUTH:
    st.warning("Sem credenciais: modo leitura via CSV (sem escrita). Para editar, mantenha seus secrets como [GCP_SERVICE_ACCOUNT].")

if HAS_AUTH:
    sh, ws, abas_disp, aba_usada = abrir_planilha_com_auth(SHEET_ID_MEIRE)
    st.success(f"Conectado com autenticação. Aba: **{aba_usada}**")
    st.caption(f"Abas disponíveis: {', '.join(abas_disp)}")
    df_base = carregar_df_auth(ws)
    EDITAVEL = True
else:
    df_base, aba_usada = carregar_df_sem_auth()
    st.info(f"Conectado em **modo leitura** via CSV. Aba: **{aba_usada}**")
    EDITAVEL = False

# ===== Opções dinâmicas
clientes_opts = sorted([c for c in df_base["Cliente"].dropna().unique() if str(c).strip()])
servicos_opts = sorted([s for s in df_base["Serviço"].dropna().unique() if str(s).strip()])
periodos_opts = sorted([p for p in df_base.get("Período", pd.Series([])).dropna().unique() if str(p).strip()])
formas_pagamento = sorted(
    [c for c in df_base.get("Conta", pd.Series([])).dropna().unique() if str(c).strip()]
    + ["Carteira", "Pix", "Nubank", "Dinheiro", "Cartão"]
)

# ===== Sidebar
st.sidebar.header("Ações")
modo = st.sidebar.radio(
    "Escolha:",
    ["➕ Lançar fiado", "💵 Registrar pagamento", "📄 Em aberto & exportação"],
    index=0
)

# ===== Lançar fiado
if modo.startswith("➕"):
    st.subheader("Lançar fiado — cria UMA linha por serviço (Conta='Fiado')")
    if not EDITAVEL:
        st.warning("Modo leitura: lançar fiado desativado. Configure as credenciais em [GCP_SERVICE_ACCOUNT] ou [gcp_service_account].")

    colA, colB = st.columns([1,1])
    with colA:
        data_atend = st.date_input("Data do atendimento", value=date.today(), disabled=not EDITAVEL)
        cliente_sel = st.selectbox("Cliente (selecione)", options=["—"] + clientes_opts, index=0, disabled=not EDITAVEL)
        cliente_digitado = st.text_input("Ou digite o nome do cliente", "", disabled=not EDITAVEL)
        combo_txt = st.text_input("Combo (use 'serv1+serv2')", value="", disabled=not EDITAVEL)
        servico_sel = st.selectbox("Ou selecione um serviço (se não usar combo)", options=["—"] + servicos_opts, index=0, disabled=not EDITAVEL)
        valor_unico = st.text_input("Valor (R$) — no combo aplica no 1º serviço", value="", disabled=not EDITAVEL)
    with colB:
        venc_opc = st.date_input("Vencimento (opcional)", value=None, format="YYYY/MM/DD", disabled=not EDITAVEL)
        fase = st.selectbox("Fase", options=["Dono (sozinha)", "Autônomo (prestador)", "Dono + funcionário"], index=0, disabled=not EDITAVEL)
        tipo = st.selectbox("Tipo", options=["Serviço", "Produto"], index=0, disabled=not EDITAVEL)
        periodo = st.selectbox("Período (opcional)", options=["—"] + periodos_opts, index=0, disabled=not EDITAVEL)
        observ = st.text_area("Observação (opcional)", "", disabled=not EDITAVEL)

    funcionario = "Meire"; conta = "Fiado"

    if st.button("Salvar fiado", type="primary", disabled=not EDITAVEL):
        cliente = cliente_sel if cliente_sel != "—" else cliente_digitado.strip()
        if not cliente:
            st.error("Informe o cliente (selecione ou digite)."); st.stop()

        linhas = []
        if combo_txt.strip():
            partes = [p.strip() for p in combo_txt.split("+") if p.strip()]
            if not partes:
                st.error("Combo informado está vazio depois de separar por '+'."); st.stop()
            valores = [moeda_to_float(valor_unico)] + [0.0]*(len(partes)-1)
            for i, srv in enumerate(partes):
                linhas.append({"Serviço": srv, "Valor": valores[i], "Combo": combo_txt})
        else:
            if servico_sel == "—":
                st.error("Informe um combo OU selecione um serviço."); st.stop()
            linhas.append({"Serviço": servico_sel, "Valor": moeda_to_float(valor_unico), "Combo": ""})

        novos = []
        for L in linhas:
            novos.append({
                "Data": data_atend, "Serviço": L["Serviço"], "Valor": L["Valor"], "Conta": conta,
                "Cliente": cliente, "Combo": L["Combo"], "Funcionário": funcionario,
                "Fase": fase, "Tipo": tipo, "Período": (periodo if periodo != "—" else ""),
                "Fiado_Vencimento": venc_opc, "Fiado_Status": "Em aberto", "Quitado_em": "", "Observação": observ
            })
        df_novos = pd.DataFrame(novos)
        df_final = pd.concat([df_base, df_novos], ignore_index=True)
        salvar_df_auth(ws, df_final)
        st.success(f"Fiado lançado com sucesso para **{cliente}** ({len(df_novos)} linha(s)).")
        st.balloons()

# ===== Registrar pagamento
elif modo.startswith("💵"):
    st.subheader("Registrar pagamento (quitar por competência)")
    if not EDITAVEL:
        st.warning("Modo leitura: quitação desativada. Configure as credenciais em [GCP_SERVICE_ACCOUNT] ou [gcp_service_account].")

    col1, col2, col3 = st.columns([1,1,1])
    with col1:
        cliente_pg = st.selectbox("Cliente", options=["—"] + clientes_opts, index=0, disabled=not EDITAVEL)
        forma = st.selectbox("Forma de pagamento", options=formas_pagamento, index=0, disabled=not EDITAVEL)
    with col2:
        data_quit = st.date_input("Data do pagamento (Quitado em)", value=date.today(), disabled=not EDITAVEL)
        somente_servicos = st.checkbox("Somente 'Serviço' (ignorar 'Produto')", value=True, disabled=not EDITAVEL)
    with col3:
        filtro_vencidos = st.checkbox("Apenas vencidos (Fiado_Vencimento < hoje)", value=False, disabled=not EDITAVEL)
        incluir_sem_venc = st.checkbox("Incluir fiados sem vencimento", value=True, disabled=not EDITAVEL)

    if cliente_pg != "—":
        mask = (df_base["Cliente"].astype(str) == cliente_pg) & (df_base["Conta"].astype(str).str.lower() == "fiado") & \
               (df_base["Fiado_Status"].astype(str).str.lower().isin(["", "em aberto"]))
        if somente_servicos and "Tipo" in df_base.columns:
            mask &= (df_base["Tipo"].astype(str).str.lower() == "serviço")

        hoje = date.today()
        if filtro_vencidos:
            venc = pd.to_datetime(df_base["Fiado_Vencimento"], errors="coerce").dt.date
            mask &= (venc.notna() & (venc < hoje))
            if incluir_sem_venc:
                mask |= ((df_base["Cliente"].astype(str) == cliente_pg) &
                         (df_base["Conta"].astype(str).str.lower() == "fiado") &
                         (df_base["Fiado_Status"].astype(str).str.lower().isin(["", "em aberto"])) &
                         venc.isna())

        df_aberto = df_base[mask].copy()
        st.markdown(f"**Fiados em aberto para {cliente_pg}: {len(df_aberto)}**")
        if len(df_aberto) == 0:
            st.info("Nenhuma linha com os filtros atuais.")
        else:
            cols_show = ["Data","Serviço","Valor","Fiado_Vencimento","Período","Observação"]
            cols_show = [c for c in cols_show if c in df_aberto.columns]
            st.dataframe(df_aberto[cols_show].reset_index(drop=True), use_container_width=True)

            if st.button("Registrar pagamento (quitar todos filtrados)", type="primary", disabled=not EDITAVEL):
                df_edit = df_base.copy()
                idxs = df_aberto.index
                for idx in idxs:
                    df_edit.loc[idx, "Fiado_Status"] = "Pago"
                    df_edit.loc[idx, "Quitado_em"] = data_quit
                    df_edit.loc[idx, "Conta"] = forma
                salvar_df_auth(ws, df_edit)
                st.success(f"Quitado com sucesso: {len(idxs)} linha(s) de {cliente_pg}.")
                st.balloons()

# ===== Em aberto & Exportação
else:
    st.subheader("Fiados em aberto")

    df_em_aberto = df_base[
        (df_base["Conta"].astype(str).str.lower() == "fiado") &
        (df_base["Fiado_Status"].astype(str).str.lower().isin(["", "em aberto"]))
    ].copy()

    colf1, colf2 = st.columns([1,3])
    with colf1:
        cliente_f = st.selectbox("Filtrar por cliente (opcional)", options=["—"] + sorted(df_em_aberto["Cliente"].dropna().unique().tolist()))
        periodo_f = st.selectbox("Filtrar por período (opcional)", options=["—"] + sorted(df_em_aberto["Período"].dropna().unique().tolist()))
    with colf2:
        venc_ate = st.date_input("Vencimento até (opcional)", value=None)

    if cliente_f != "—":
        df_em_aberto = df_em_aberto[df_em_aberto["Cliente"] == cliente_f]
    if periodo_f != "—":
        df_em_aberto = df_em_aberto[df_em_aberto["Período"] == periodo_f]
    if venc_ate:
        vencs = pd.to_datetime(df_em_aberto["Fiado_Vencimento"], errors="coerce").dt.date
        df_em_aberto = df_em_aberto[vencs.notna() & (vencs <= venc_ate)]

    total_aberto = df_em_aberto["Valor"].apply(moeda_to_float).sum() if len(df_em_aberto) else 0.0
    st.metric("Total em aberto (R$)", f"{total_aberto:,.2f}".replace(",", "v").replace(".", ",").replace("v", "."))

    cols_show = ["Data","Cliente","Serviço","Valor","Fiado_Vencimento","Período","Observação"]
    cols_show = [c for c in cols_show if c in df_em_aberto.columns]
    st.dataframe(
        df_em_aberto[cols_show].sort_values(by=["Cliente","Data"], ascending=[True, True]).reset_index(drop=True),
        use_container_width=True
    )

    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df_em_aberto.to_excel(writer, index=False, sheet_name="Fiado_Em_Aberto")
    st.download_button(
        "📥 Baixar Excel (em aberto)",
        data=buf.getvalue(),
        file_name=f"fiado_em_aberto_{date.today()}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

st.caption(f"{'Edição habilitada (auth)' if HAS_AUTH else 'Somente leitura (CSV)'} · Planilha: {PLANILHA_URL_MEIRE} · Aba: {aba_usada}")
