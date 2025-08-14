# pages/12_Fiado_Meire.py
# --------------------------------------------------------------
# Controle de Fiado — Meire (Feminino)
# - Compatível com secrets: [GCP_SERVICE_ACCOUNT] ou [gcp_service_account]
# - Auth (le/escreve) ou CSV (somente leitura)
# - Datas sempre em DD/MM/YYYY (texto) e colunas auxiliares *_dt para filtros
# - Padrão de colunas: StatusFiado, IDLancFiado, VencimentoFiado, DataPagamento
#   + mantém colunas antigas equivalentes para compat (Fiado_Status, Fiado_Vencimento, Quitado_em)
# - Gera IDLancFiado no formato L-YYYYMMDDHHMMSS-XXX (XXX = 3 dígitos)
# --------------------------------------------------------------

import streamlit as st
import pandas as pd
from datetime import date, datetime
from io import BytesIO
import pytz, random, urllib.parse

# gspread e auth (só usados se houver credenciais)
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

# -------- Planilha alvo
SHEET_ID_PADRAO = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"
SHEET_ID_MEIRE = st.secrets.get("SHEET_ID_MEIRE", SHEET_ID_PADRAO)
PLANILHA_URL_MEIRE = f"https://docs.google.com/spreadsheets/d/{SHEET_ID_MEIRE}/edit"

# Abas possíveis
ABA_FEMININO_ALVOS = [
    "Base de Dados Feminino",
    "base de dados feminino",
    "base de dados - feminino",
    "Base Feminino",
    "Base de Dados Fem",
]

# -------- Credenciais (aceita os dois nomes)
def _get_service_account_from_secrets():
    svc = st.secrets.get("gcp_service_account")
    if not svc:
        svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    return svc

def _tem_auth():
    return _get_service_account_from_secrets() is not None and Credentials is not None and gspread is not None

# -------- Colunas
COLS_OBRIGATORIAS = [
    # padrão comum da sua base
    "Data","Serviço","Valor","Conta","Cliente","Combo","Funcionário","Fase","Tipo","Período",
    # fiado (novo padrão)
    "StatusFiado","IDLancFiado","VencimentoFiado","DataPagamento",
    # fiado (antigo, mantido p/ compat)
    "Fiado_Status","Fiado_Vencimento","Quitado_em",
    # observação
    "Observação",
]

# -------- Helpers
def moeda_to_float(v):
    if v is None or (isinstance(v, float) and pd.isna(v)): return 0.0
    s = str(v).strip()
    if s == "": return 0.0
    s = s.replace("R$", "").replace(" ", "").replace(".", "").replace(",", ".")
    try: return float(s)
    except Exception: return 0.0

def _parse_date_br(x):
    try: return pd.to_datetime(x, dayfirst=True, errors="coerce")
    except Exception: return pd.NaT

def _fmt_ddmmyyyy(x):
    if x is None or (isinstance(x, float) and pd.isna(x)): return ""
    try:
        import datetime as _dt
        if isinstance(x, _dt.date): return x.strftime("%d/%m/%Y")
    except Exception: pass
    try:
        ts = pd.to_datetime(x, dayfirst=True, errors="coerce")
        if pd.isna(ts): return str(x)
        return ts.strftime("%d/%m/%Y")
    except Exception:
        return str(x)

def _gen_lanc_id(prefix="L"):
    now = datetime.now(BR_TZ)
    return f"{prefix}-{now.strftime('%Y%m%d%H%M%S')}-{random.randint(100,999)}"

def normalizar_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.loc[:, ~df.columns.astype(str).str.contains("^Unnamed")].copy()
    # garantir colunas
    for c in COLS_OBRIGATORIAS:
        if c not in df.columns:
            # valores default
            df[c] = "" if c not in ["Valor"] else 0.0

    # sincronizar nomes antigos/novos (sem sobrescrever o que já existe)
    # Status
    if df["StatusFiado"].eq("").all() and not df["Fiado_Status"].eq("").all():
        df["StatusFiado"] = df["Fiado_Status"]
    elif df["Fiado_Status"].eq("").all() and not df["StatusFiado"].eq("").all():
        df["Fiado_Status"] = df["StatusFiado"]
    # Vencimento
    if df["VencimentoFiado"].eq("").all() and not df["Fiado_Vencimento"].eq("").all():
        df["VencimentoFiado"] = df["Fiado_Vencimento"]
    elif df["Fiado_Vencimento"].eq("").all() and not df["VencimentoFiado"].eq("").all():
        df["Fiado_Vencimento"] = df["VencimentoFiado"]
    # Pagamento
    if df["DataPagamento"].eq("").all() and not df["Quitado_em"].eq("").all():
        df["DataPagamento"] = df["Quitado_em"]
    elif df["Quitado_em"].eq("").all() and not df["DataPagamento"].eq("").all():
        df["Quitado_em"] = df["DataPagamento"]

    # valor
    df["Valor"] = df["Valor"].apply(moeda_to_float)

    # colunas auxiliares para filtros
    df["Data_dt"]  = df["Data"].apply(_parse_date_br)
    df["Venc_dt"]  = df["VencimentoFiado"].apply(_parse_date_br)
    df["Quitado_dt"] = df["DataPagamento"].apply(_parse_date_br)

    # não remover linhas que tenham Data preenchida
    if len(df):
        def _is_row_empty(row):
            r = row.fillna("")
            if str(r.get("Data","")).strip() != "": return False
            cols_check = [c for c in df.columns if not c.endswith("_dt")]
            return "".join(str(r[c]) for c in cols_check).strip() == ""
        mask_empty = df.apply(_is_row_empty, axis=1)
        df = df.loc[~mask_empty].copy()
    return df

# -------- Auth path
def abrir_planilha_com_auth(spreadsheet_id: str):
    svc = _get_service_account_from_secrets()
    if not svc:
        st.error("Credenciais não encontradas (GCP_SERVICE_ACCOUNT/gcp_service_account)."); st.stop()
    creds = Credentials.from_service_account_info(
        svc, scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    )
    client = gspread.authorize(creds)
    sh = client.open_by_key(spreadsheet_id)
    abas = [ws.title for ws in sh.worksheets()]
    alvo = None
    for possivel in ABA_FEMININO_ALVOS:
        if any(aba.strip().lower()==possivel.strip().lower() for aba in abas):
            alvo = possivel; break
    if alvo is None:
        alvo = abas[0]; st.warning(f"Aba Feminino não encontrada. Usando: {alvo}")
    ws = sh.worksheet(alvo)
    return sh, ws, abas, alvo

def carregar_df_auth(ws): 
    return normalizar_df(get_as_dataframe(ws, evaluate_formulas=True, header=0, dtype=str))

def salvar_df_auth(ws, df: pd.DataFrame):
    df = df.copy()
    # datas em texto BR
    for col in ["Data","VencimentoFiado","Fiado_Vencimento","DataPagamento","Quitado_em"]:
        if col in df.columns: df[col] = df[col].apply(_fmt_ddmmyyyy)
    # tirar auxiliares
    df = df.loc[:, [c for c in df.columns if not c.endswith("_dt")]]
    # garantir colunas
    cols = list(dict.fromkeys(list(df.columns) + COLS_OBRIGATORIAS))
    df = df.reindex(columns=cols)
    set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)

# -------- CSV path (somente leitura)
def _csv_from_sheet(sheet_name: str) -> pd.DataFrame|None:
    quoted = urllib.parse.quote(sheet_name, safe="")
    url_csv = f"https://docs.google.com/spreadsheets/d/{SHEET_ID_MEIRE}/gviz/tq?tqx=out:csv&sheet={quoted}"
    try:
        df = pd.read_csv(url_csv)
        if len(df.columns)==1 and str(df.columns[0]).startswith("<!DOCTYPE html"): return None
        return df
    except Exception:
        return None

def carregar_df_sem_auth():
    for nome in ABA_FEMININO_ALVOS:
        df = _csv_from_sheet(nome)
        if df is not None: return normalizar_df(df), nome
    df = _csv_from_sheet("Base%20de%20Dados")
    if df is not None: return normalizar_df(df), "Base de Dados"
    st.error("Não consegui ler a planilha via CSV. Compartilhe com link (leitura)."); st.stop()

# -------- Load
HAS_AUTH = _tem_auth()
if not HAS_AUTH:
    st.warning("Sem credenciais: modo leitura via CSV (sem escrita).")

if HAS_AUTH:
    sh, ws, abas_disp, aba_usada = abrir_planilha_com_auth(SHEET_ID_MEIRE)
    st.success(f"Conectado (auth). Aba: **{aba_usada}**"); st.caption(", ".join(abas_disp))
    df_base = carregar_df_auth(ws); EDITAVEL = True
else:
    df_base, aba_usada = carregar_df_sem_auth()
    st.info(f"Conectado (somente leitura CSV). Aba: **{aba_usada}**")
    EDITAVEL = False

# -------- Opções dinâmicas
clientes_opts = sorted([c for c in df_base["Cliente"].dropna().unique() if str(c).strip()])
servicos_opts = sorted([s for s in df_base["Serviço"].dropna().unique() if str(s).strip()])
periodos_opts = sorted([p for p in df_base.get("Período", pd.Series([])).dropna().unique() if str(p).strip()])
formas_pagamento = sorted([c for c in df_base.get("Conta", pd.Series([])).dropna().unique() if str(c).strip()]
                          + ["Carteira","Pix","Nubank","Nubank CNPJ","Dinheiro","Cartão"])

# -------- Sidebar
st.sidebar.header("Ações")
modo = st.sidebar.radio("Escolha:", ["➕ Lançar fiado","💵 Registrar pagamento","📄 Em aberto & exportação"], index=0)

# -------- Lançar fiado
if modo.startswith("➕"):
    st.subheader("Lançar fiado — cria UMA linha por serviço (Conta='Fiado')")
    if not EDITAVEL: st.warning("Modo leitura: lançar fiado desativado.")
    colA,colB = st.columns([1,1])
    with colA:
        data_atend = st.date_input("Data do atendimento", value=date.today(), disabled=not EDITAVEL)
        cliente_sel = st.selectbox("Cliente (selecione)", ["—"] + clientes_opts, index=0, disabled=not EDITAVEL)
        cliente_digitado = st.text_input("Ou digite o nome do cliente", "", disabled=not EDITAVEL)
        combo_txt = st.text_input("Combo (use 'serv1+serv2')", "", disabled=not EDITAVEL)
        servico_sel = st.selectbox("Ou selecione um serviço", ["—"] + servicos_opts, index=0, disabled=not EDITAVEL)
        valor_unico = st.text_input("Valor (R$) — no combo aplica no 1º", "", disabled=not EDITAVEL)
    with colB:
        venc_opc = st.date_input("Vencimento (opcional)", value=None, format="YYYY/MM/DD", disabled=not EDITAVEL)
        fase = st.selectbox("Fase", ["Dono (sozinha)","Autônomo (prestador)","Dono + funcionário"], index=0, disabled=not EDITAVEL)
        tipo = st.selectbox("Tipo", ["Serviço","Produto"], index=0, disabled=not EDITAVEL)
        periodo = st.selectbox("Período (opcional)", ["—"] + periodos_opts, index=0, disabled=not EDITAVEL)
        observ = st.text_area("Observação (opcional)", "", disabled=not EDITAVEL)

    funcionario, conta = "Meire","Fiado"

    if st.button("Salvar fiado", type="primary", disabled=not EDITAVEL):
        cliente = cliente_sel if cliente_sel != "—" else cliente_digitado.strip()
        if not cliente: st.error("Informe o cliente."); st.stop()

        # monta serviços
        linhas=[]
        if combo_txt.strip():
            partes=[p.strip() for p in combo_txt.split("+") if p.strip()]
            if not partes: st.error("Combo vazio após separar por '+'."); st.stop()
            valores=[moeda_to_float(valor_unico)] + [0.0]*(len(partes)-1)
            for i,srv in enumerate(partes):
                linhas.append({"Serviço":srv,"Valor":valores[i],"Combo":combo_txt})
        else:
            if servico_sel=="—": st.error("Informe um combo OU um serviço."); st.stop()
            linhas.append({"Serviço":servico_sel,"Valor":moeda_to_float(valor_unico),"Combo":""})

        novos=[]
        for L in linhas:
            _id = _gen_lanc_id("L")
            novos.append({
                # comuns
                "Data": data_atend.strftime("%d/%m/%Y"),
                "Serviço": L["Serviço"],
                "Valor": L["Valor"],
                "Conta": conta,
                "Cliente": cliente,
                "Combo": L["Combo"],
                "Funcionário": funcionario,
                "Fase": fase,
                "Tipo": tipo,
                "Período": (periodo if periodo != "—" else ""),
                "Observação": observ,
                # fiado (novo padrão)
                "StatusFiado": "Em aberto",
                "IDLancFiado": _id,
                "VencimentoFiado": (venc_opc.strftime("%d/%m/%Y") if venc_opc else ""),
                "DataPagamento": "",
                # compat antigo
                "Fiado_Status": "Em aberto",
                "Fiado_Vencimento": (venc_opc.strftime("%d/%m/%Y") if venc_opc else ""),
                "Quitado_em": "",
            })
        df_novos = pd.DataFrame(novos)

        df_final = pd.concat([df_base, df_novos], ignore_index=True)
        salvar_df_auth(ws, df_final)
        st.success(f"Lançado com sucesso para **{cliente}** ({len(df_novos)} linha(s)).")
        st.balloons()

# -------- Registrar pagamento
elif modo.startswith("💵"):
    st.subheader("Registrar pagamento (quitar por competência)")
    if not EDITAVEL: st.warning("Modo leitura: quitação desativada.")

    col1,col2,col3 = st.columns([1,1,1])
    with col1:
        cliente_pg = st.selectbox("Cliente", ["—"] + clientes_opts, index=0, disabled=not EDITAVEL)
        forma = st.selectbox("Forma de pagamento", formas_pagamento, index=0, disabled=not EDITAVEL)
    with col2:
        data_quit = st.date_input("Data do pagamento", value=date.today(), disabled=not EDITAVEL)
        somente_servicos = st.checkbox("Somente 'Serviço'", True, disabled=not EDITAVEL)
    with col3:
        filtro_vencidos = st.checkbox("Apenas vencidos", False, disabled=not EDITAVEL)
        incluir_sem_venc = st.checkbox("Incluir sem vencimento", True, disabled=not EDITAVEL)

    if cliente_pg != "—":
        mask = (df_base["Cliente"].astype(str)==cliente_pg) & (df_base["Conta"].str.lower()=="fiado") & \
               (df_base["StatusFiado"].astype(str).str.lower().isin(["","em aberto"]))
        if somente_servicos and "Tipo" in df_base.columns:
            mask &= (df_base["Tipo"].astype(str).str.lower()=="serviço")
        hoje = date.today()
        if filtro_vencidos:
            venc = df_base.get("Venc_dt")
            mask &= (venc.notna() & (venc.dt.date < hoje))
            if incluir_sem_venc:
                mask |= ((df_base["Cliente"].astype(str)==cliente_pg) &
                         (df_base["Conta"].str.lower()=="fiado") &
                         (df_base["StatusFiado"].astype(str).str.lower().isin(["","em aberto"])) &
                         (venc.isna()))
        df_aberto = df_base[mask].copy()
        st.markdown(f"**Fiados em aberto para {cliente_pg}: {len(df_aberto)}**")
        if len(df_aberto)==0:
            st.info("Nenhuma linha com os filtros atuais.")
        else:
            cols_show = ["Data","Serviço","Valor","VencimentoFiado","Período","IDLancFiado","Observação"]
            cols_show = [c for c in cols_show if c in df_aberto.columns]
            st.dataframe(df_aberto[cols_show].reset_index(drop=True), use_container_width=True)

            if st.button("Registrar pagamento (quitar todos filtrados)", type="primary", disabled=not EDITAVEL):
                df_edit = df_base.copy()
                idxs = df_aberto.index
                for idx in idxs:
                    df_edit.loc[idx, "StatusFiado"] = "Pago"
                    df_edit.loc[idx, "Fiado_Status"] = "Pago"      # compat
                    df_edit.loc[idx, "DataPagamento"] = data_quit.strftime("%d/%m/%Y")
                    df_edit.loc[idx, "Quitado_em"]   = data_quit.strftime("%d/%m/%Y")  # compat
                    df_edit.loc[idx, "Conta"] = forma
                salvar_df_auth(ws, df_edit)
                st.success(f"Quitado com sucesso: {len(idxs)} linha(s) de {cliente_pg}.")
                st.balloons()

# -------- Em aberto & exportação
else:
    st.subheader("Fiados em aberto")
    df_em_aberto = df_base[
        (df_base["Conta"].astype(str).str.lower()=="fiado") &
        (df_base["StatusFiado"].astype(str).str.lower().isin(["","em aberto"]))
    ].copy()

    colf1,colf2 = st.columns([1,3])
    with colf1:
        cliente_f = st.selectbox("Filtrar por cliente", ["—"] + sorted(df_em_aberto["Cliente"].dropna().unique().tolist()))
        periodo_f = st.selectbox("Filtrar por período", ["—"] + sorted(df_em_aberto["Período"].dropna().unique().tolist()))
    with colf2:
        venc_ate = st.date_input("Vencimento até (opcional)", value=None)

    if cliente_f != "—": df_em_aberto = df_em_aberto[df_em_aberto["Cliente"]==cliente_f]
    if periodo_f != "—": df_em_aberto = df_em_aberto[df_em_aberto["Período"]==periodo_f]
    if venc_ate:
        vencs = df_em_aberto.get("Venc_dt")
        df_em_aberto = df_em_aberto[vencs.notna() & (vencs.dt.date <= venc_ate)]

    total_aberto = df_em_aberto["Valor"].apply(moeda_to_float).sum() if len(df_em_aberto) else 0.0
    st.metric("Total em aberto (R$)", f"{total_aberto:,.2f}".replace(",", "v").replace(".", ",").replace("v","."))

    cols_show = ["Data","Cliente","Serviço","Valor","VencimentoFiado","Período","IDLancFiado","Observação"]
    cols_show = [c for c in cols_show if c in df_em_aberto.columns]
    st.dataframe(
        df_em_aberto[cols_show].sort_values(by=["Cliente","Data_dt"], ascending=[True,True]).reset_index(drop=True),
        use_container_width=True
    )

    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df_em_aberto.to_excel(writer, index=False, sheet_name="Fiado_Em_Aberto")
    st.download_button("📥 Baixar Excel (em aberto)", data=buf.getvalue(),
        file_name=f"fiado_em_aberto_{date.today()}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

st.caption(f"{'Edição habilitada (auth)' if HAS_AUTH else 'Somente leitura (CSV)'} · Planilha: {PLANILHA_URL_MEIRE} · Aba: {aba_usada}")
