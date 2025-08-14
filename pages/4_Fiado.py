# pages/12_Fiado_Meire.py
# --------------------------------------------------------------
# Controle de Fiado — Registro da Meire (Feminino)
# - Lançar fiado (uma linha por serviço, conta='Fiado')
# - Registrar pagamento por competência (atualiza as linhas, não cria novas)
# - Listar em aberto + exportação
# --------------------------------------------------------------

import streamlit as st
import pandas as pd
from datetime import date, datetime
from io import BytesIO
import pytz

# gspread
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials

# ============== CONFIG GERAL ==============
st.set_page_config(page_title="Fiado (Meire - Feminino)", page_icon="💳", layout="wide")
st.title("💳 Controle de Fiado — Registro da Meire (Feminino)")

BR_TZ = pytz.timezone("America/Sao_Paulo")

# Aba(s) possíveis do feminino
ABA_FEMININO_ALVOS = [
    "Base de Dados Feminino",
    "base de dados - feminino",
    "base de dados feminino",
    "Base de Dados Fem",
    "Base Feminino",
]

# ID padrão da planilha (a mesma do salão)
SHEET_ID_PADRAO = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"

# Tente pegar dos secrets; se não houver, use o padrão
SHEET_ID_MEIRE = st.secrets.get("SHEET_ID_MEIRE", SHEET_ID_PADRAO)
PLANILHA_URL_MEIRE = f"https://docs.google.com/spreadsheets/d/{SHEET_ID_MEIRE}/edit"

# ============== AUTENTICAÇÃO ==============
def get_gspread_client():
    # Tenta com Service Account. Se não estiver configurado, mostra aviso claro.
    try:
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"]
        )
        client = gspread.authorize(creds)
        return client, None
    except Exception as e:
        return None, f"Service Account não configurada em st.secrets['gcp_service_account']: {e}"

# ============== HELPERS ==============
COLS_OBRIGATORIAS = [
    "Data", "Serviço", "Valor", "Conta", "Cliente", "Combo",
    "Funcionário", "Fase", "Tipo", "Período",
    # Campos de fiado
    "Fiado_Vencimento", "Fiado_Status", "Quitado_em", "Observação"
]

def moeda_to_float(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return 0.0
    s = str(v).strip()
    if s == "":
        return 0.0
    # Trata formatos: "R$ 1.234,56" | "1234,56" | "1234.56"
    s = s.replace("R$", "").replace(" ", "")
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0

@st.cache_data(ttl=180)
def abrir_planilha(spreadsheet_id: str):
    cli, err = get_gspread_client()
    if err:
        st.warning("**Usando ID/URL padrão definidos no código** (SHEET_ID_MEIRE/PLANILHA_URL_MEIRE não encontrados em secrets).")
    if cli is None and err:
        st.error(err)
        st.stop()

    try:
        sh = cli.open_by_key(spreadsheet_id)
        return sh
    except Exception as e:
        st.error(f"Não consegui abrir a planilha (ID: {spreadsheet_id}). Erro: {e}")
        st.stop()

def localizar_aba_feminino(sh):
    abas = [ws.title for ws in sh.worksheets()]
    alvo = None
    for possivel in ABA_FEMININO_ALVOS:
        for aba in abas:
            if aba.strip().lower() == possivel.strip().lower():
                alvo = aba
                break
        if alvo:
            break
    if alvo is None:
        st.error(f"Não encontrei a aba do Feminino. Abas disponíveis: {', '.join(abas)}")
        st.stop()
    return alvo, abas

def carregar_df(ws):
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0, dtype=str)
    # remove colunas vazias "Unnamed"
    df = df.loc[:, ~df.columns.astype(str).str.contains("^Unnamed")]
    # normaliza tipos básicos
    if "Valor" in df.columns:
        df["Valor"] = df["Valor"].apply(moeda_to_float)
    # garante colunas obrigatórias
    for c in COLS_OBRIGATORIAS:
        if c not in df.columns:
            df[c] = "" if c not in ["Valor"] else 0.0
    # normaliza datas
    if "Data" in df.columns:
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce").dt.date
    if "Fiado_Vencimento" in df.columns:
        df["Fiado_Vencimento"] = pd.to_datetime(df["Fiado_Vencimento"], errors="coerce").dt.date
    if "Quitado_em" in df.columns:
        df["Quitado_em"] = pd.to_datetime(df["Quitado_em"], errors="coerce").dt.date
    # limpa linhas totalmente vazias
    if len(df):
        empty_mask = df.fillna("").astype(str).apply(lambda r: "".join(r.values), axis=1) == ""
        df = df.loc[~empty_mask].copy()
    return df

def salvar_df(ws, df: pd.DataFrame):
    # ordena colunas para manter consistência
    cols = list(dict.fromkeys(list(df.columns) + COLS_OBRIGATORIAS))
    df = df.reindex(columns=cols)
    set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)

# ============== CONEXÃO & STATUS ==============
sh = abrir_planilha(SHEET_ID_MEIRE)
aba_fem, abas = localizar_aba_feminino(sh)
ws = sh.worksheet(aba_fem)

with st.container():
    st.success(f"Conectado em: **{PLANILHA_URL_MEIRE.split('/')[-2]}**")
    st.caption(f"Abas disponíveis: {', '.join(abas)}")

# ============== DADOS BASE ==============
df_base = carregar_df(ws)

# opções dinâmicas
clientes_opts = sorted([c for c in df_base["Cliente"].dropna().unique() if str(c).strip() != ""])
servicos_opts = sorted([s for s in df_base["Serviço"].dropna().unique() if str(s).strip() != ""])
combos_opts = sorted([c for c in df_base["Combo"].dropna().unique() if str(c).strip() != ""])
formas_pagamento = sorted([c for c in df_base.get("Conta", pd.Series([])).dropna().unique() if str(c).strip() != ""] + ["Carteira", "Pix", "Nubank", "Dinheiro", "Cartão"])
periodos_opts = sorted([p for p in df_base.get("Período", pd.Series([])).dropna().unique() if str(p).strip() != ""])

# ============== SIDEBAR ==============
st.sidebar.header("Ações")
modo = st.sidebar.radio(
    "Escolha:",
    ["➕ Lançar fiado", "💵 Registrar pagamento", "📄 Em aberto & exportação"],
    index=0
)

# ======================================================================
# 1) LANÇAR FIADO
# ======================================================================
if modo.startswith("➕"):
    st.subheader("Lançar fiado — cria UMA linha por serviço na Base Feminina (Conta='Fiado')")

    colA, colB = st.columns([1,1])
    with colA:
        data_atend = st.date_input("Data do atendimento", value=date.today())
        cliente_sel = st.selectbox("Cliente (selecione)", options=["—"] + clientes_opts, index=0)
        cliente_digitado = st.text_input("Ou digite o nome do cliente", "")
        combo_txt = st.text_input("Combo (use 'serv1+serv2')", value="")
        servico_sel = st.selectbox("Ou selecione um serviço (se não usar combo)", options=["—"] + servicos_opts, index=0)
        valor_unico = st.text_input("Valor (R$) — se informar combo, este valor será usado no 1º serviço (pode deixar vazio)", value="")

    with colB:
        venc_opc = st.date_input("Vencimento (opcional)", value=None, format="YYYY/MM/DD")
        fase = st.selectbox("Fase", options=["Dono (sozinha)", "Autônomo (prestador)", "Dono + funcionário"], index=0)
        tipo = st.selectbox("Tipo", options=["Serviço", "Produto"], index=0)
        periodo = st.selectbox("Período (opcional)", options=["—"] + periodos_opts, index=0)
        observ = st.text_area("Observação (opcional)", "")

    funcionario = "Meire"  # fixo
    conta = "Fiado"       # fixo

    if st.button("Salvar fiado", type="primary"):
        # valida cliente
        cliente = cliente_sel if cliente_sel != "—" else cliente_digitado.strip()
        if not cliente:
            st.error("Informe o cliente (selecione ou digite).")
            st.stop()

        # define serviços a inserir
        linhas = []
        if combo_txt.strip():
            partes = [p.strip() for p in combo_txt.split("+") if p.strip()]
            if not partes:
                st.error("Combo informado está vazio depois de separar por '+'.")
                st.stop()
            # valor único (opcional) é aplicado apenas ao 1º serviço
            valores = [moeda_to_float(valor_unico)] + [0.0]*(len(partes)-1)
            for i, srv in enumerate(partes):
                linhas.append({"Serviço": srv, "Valor": valores[i], "Combo": combo_txt})
        else:
            if servico_sel == "—":
                st.error("Informe um combo OU selecione um serviço.")
                st.stop()
            linhas.append({"Serviço": servico_sel, "Valor": moeda_to_float(valor_unico), "Combo": ""})

        # monta dataframe das novas linhas
        novos = []
        for L in linhas:
            novos.append({
                "Data": data_atend,
                "Serviço": L["Serviço"],
                "Valor": L["Valor"],
                "Conta": conta,
                "Cliente": cliente,
                "Combo": L["Combo"],
                "Funcionário": funcionario,
                "Fase": fase,
                "Tipo": tipo,
                "Período": (periodo if periodo != "—" else ""),
                "Fiado_Vencimento": venc_opc,
                "Fiado_Status": "Em aberto",
                "Quitado_em": "",
                "Observação": observ
            })
        df_novos = pd.DataFrame(novos)

        # anexa e salva
        df_final = pd.concat([df_base, df_novos], ignore_index=True)
        salvar_df(ws, df_final)

        st.success(f"Fiado lançado com sucesso para **{cliente}** ({len(df_novos)} linha(s)).")
        st.balloons()

# ======================================================================
# 2) REGISTRAR PAGAMENTO
# ======================================================================
elif modo.startswith("💵"):
    st.subheader("Registrar pagamento (quitar por competência) — atualiza as linhas (não cria novas)")

    col1, col2, col3 = st.columns([1,1,1])
    with col1:
        cliente_pg = st.selectbox("Cliente", options=["—"] + clientes_opts, index=0)
        forma = st.selectbox("Forma de pagamento", options=formas_pagamento, index=0)
    with col2:
        data_quit = st.date_input("Data do pagamento (Quitado em)", value=date.today())
        somente_servicos = st.checkbox("Somente 'Serviço' (ignorar 'Produto')", value=True)
    with col3:
        filtro_vencidos = st.checkbox("Apenas vencidos (Fiado_Vencimento < hoje)", value=False)
        incluir_sem_venc = st.checkbox("Incluir fiados sem vencimento", value=True)

    if cliente_pg == "—":
        st.info("Selecione o cliente para ver os fiados em aberto.")
    else:
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
            st.warning("Nenhuma linha em aberto com os filtros atuais.")
        else:
            # permite selecionar múltiplas linhas por índice exibido
            df_show = df_aberto.copy()
            # mostra colunas principais
            cols_show = ["Data", "Serviço", "Valor", "Fiado_Vencimento", "Período", "Observação"]
            cols_show = [c for c in cols_show if c in df_show.columns]
            st.dataframe(df_show[cols_show].reset_index(drop=True))

            sel_todos = st.checkbox("Selecionar todos", value=True)
            if sel_todos:
                idx_escolhidos = df_show.index.tolist()
            else:
                idx_escolhidos = st.multiselect(
                    "Selecione as linhas a quitar (pela posição mostrada acima)",
                    options=list(range(len(df_show))),
                    format_func=lambda i: f"Linha #{i+1} - {df_show.iloc[i]['Serviço']} - R$ {df_show.iloc[i]['Valor']:.2f}"
                )
                idx_escolhidos = [df_show.index[i] for i in idx_escolhidos]

            if st.button("Registrar pagamento (quitar selecionadas)", type="primary", disabled=len(idx_escolhidos) == 0):
                df_edit = df_base.copy()
                # atualiza as linhas escolhidas
                for idx in idx_escolhidos:
                    df_edit.loc[idx, "Fiado_Status"] = "Pago"
                    df_edit.loc[idx, "Quitado_em"] = data_quit
                    # mantém a competência do atendimento na coluna Data
                    # e atualiza a forma de pagamento realmente usada
                    df_edit.loc[idx, "Conta"] = forma

                salvar_df(ws, df_edit)
                st.success(f"Quitado com sucesso: {len(idx_escolhidos)} linha(s) de {cliente_pg}.")
                st.balloons()

# ======================================================================
# 3) EM ABERTO & EXPORTAÇÃO
# ======================================================================
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
    st.dataframe(df_em_aberto[cols_show].sort_values(by=["Cliente","Data"], ascending=[True, True]).reset_index(drop=True), use_container_width=True)

    # Exportar
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df_em_aberto.to_excel(writer, index=False, sheet_name="Fiado_Em_Aberto")
    st.download_button("📥 Baixar Excel (em aberto)", data=buf.getvalue(), file_name=f"fiado_em_aberto_{date.today()}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# Rodapé informativo
st.caption(f"Conectado em: dados_barbearia | Planilha: {PLANILHA_URL_MEIRE}")
