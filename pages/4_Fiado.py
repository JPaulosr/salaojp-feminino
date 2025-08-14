# -*- coding: utf-8 -*-
# 12_Fiado_Feminino.py — Fiado integrado à Base (Feminino/Meire)
# - Combo por linhas com valores editáveis
# - Registrar pagamento por cliente (seleciona 1+ IDs; "selecionar todos")
# - Sugere última forma de pagamento do cliente (vinda da Base Feminina)
# - Quitar por COMPETÊNCIA (atualiza as linhas; não cria novas)
# - Lançar comissão em "Despesas" no mesmo fluxo de quitação (opcional)
# - Comissão usa a mesma data do fiado (se única); senão, cai na data do pagamento
# - Exportação Excel (openpyxl) ou CSV (fallback)
# - Sidebar expandida por padrão

import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from datetime import date, datetime
from io import BytesIO
import pytz

st.set_page_config(page_title="Fiado (Feminino | Meire)", page_icon="💳", layout="wide",
                   initial_sidebar_state="expanded")
st.title("💳 Controle de Fiado — Feminino (Meire)")

# ===== CONFIG =====
SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"
ABA_BASE = "Base de Dados Feminino"
ABA_LANC = "Fiado_Lancamentos_Fem"
ABA_PAGT = "Fiado_Pagamentos_Fem"
ABA_DESP = "Despesas"  # mesma aba de despesas da planilha
TZ = pytz.timezone("America/Sao_Paulo")
DATA_FMT = "%d/%m/%Y"

BASE_COLS_MIN = ["Data","Serviço","Valor","Conta","Cliente","Combo","Funcionário","Fase","Tipo","Período"]
EXTRA_COLS    = ["StatusFiado","IDLancFiado","VencimentoFiado","DataPagamento"]

# valores padrão típicos do feminino (ajuste livre)
VALORES_PADRAO = {
    "Unha mão": 20.0,
    "Unha Pé": 25.0,
    "Sobrancelhas": 15.0,
    "Escova": 25.0,
    "Progressiva": 100.0,
    "Progressiva (curta)": 75.0,
    "Tinta": 20.0,
    "Botox": 50.0,
}

# ===== Conexão =====
@st.cache_resource
def conectar_sheets():
    info = st.secrets.get("GCP_SERVICE_ACCOUNT", st.secrets.get("gcp_service_account"))
    scopes = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SHEET_ID)

def garantir_aba(ss, nome, cols):
    try:
        ws = ss.worksheet(nome)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=nome, rows=200, cols=max(10, len(cols)))
        ws.append_row(cols)
        return ws
    if not ws.row_values(1):
        ws.append_row(cols)
    return ws

def garantir_base_cols(ss):
    ws = garantir_aba(ss, ABA_BASE, BASE_COLS_MIN + EXTRA_COLS)
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0).dropna(how="all")
    for c in BASE_COLS_MIN + EXTRA_COLS:
        if c not in df.columns:
            df[c] = ""
    df = df[[*BASE_COLS_MIN, *EXTRA_COLS, *[c for c in df.columns if c not in BASE_COLS_MIN+EXTRA_COLS]]]
    ws.clear()
    set_with_dataframe(ws, df)
    return ws

def garantir_aba_despesas(ss):
    cols_min = ["Data","Prestador","Descrição","Valor","Forma de Pagamento"]
    ws = garantir_aba(ss, ABA_DESP, cols_min)
    headers = ws.row_values(1) or cols_min
    return ws, headers

@st.cache_data
def carregar_tudo():
    ss = conectar_sheets()
    ws_base = garantir_base_cols(ss)
    ws_lanc = garantir_aba(ss, ABA_LANC,
        ["IDLanc","DataAtendimento","Cliente","Combo","Servicos","ValorTotal","Vencimento","Funcionario","Fase","Tipo","Periodo"])
    ws_pagt = garantir_aba(ss, ABA_PAGT,
        ["IDPagamento","IDLanc","DataPagamento","Cliente","FormaPagamento","ValorPago","Obs"])
    garantir_aba_despesas(ss)

    df_base = get_as_dataframe(ws_base, evaluate_formulas=True, header=0).dropna(how="all")
    df_lanc = get_as_dataframe(ws_lanc, evaluate_formulas=True, header=0).dropna(how="all")
    df_pagt = get_as_dataframe(ws_pagt, evaluate_formulas=True, header=0).dropna(how="all")

    try:
        dfb = df_base.copy()
        dfb["Cliente"] = dfb["Cliente"].astype(str).str.strip()
        clientes = sorted([c for c in dfb["Cliente"].dropna().unique() if c])
        combos  = sorted([c for c in dfb["Combo"].dropna().unique() if c])
        servs   = sorted([s for s in dfb["Serviço"].dropna().unique() if s] + list(VALORES_PADRAO.keys()))
        contas_raw = [c for c in dfb["Conta"].dropna().astype(str).str.strip().unique() if c]
        contas = sorted([c for c in contas_raw if c.lower() != "fiado"])
    except Exception:
        clientes, combos, servs, contas = [], [], [], []
    return df_base, df_lanc, df_pagt, clientes, combos, servs, contas

def salvar_df(nome_aba, df):
    ss = conectar_sheets()
    ws = ss.worksheet(nome_aba)
    ws.clear()
    set_with_dataframe(ws, df)

def append_row(nome_aba, vals):
    ss = conectar_sheets()
    ss.worksheet(nome_aba).append_row(vals, value_input_option="USER_ENTERED")

def gerar_id(prefixo):
    return f"{prefixo}-{datetime.now(TZ).strftime('%Y%m%d%H%M%S%f')[:-3]}"

def parse_combo(combo_str):
    if not combo_str:
        return []
    partes = [p.strip() for p in str(combo_str).split("+") if p.strip()]
    ajustadas = []
    for p in partes:
        hit = next((k for k in VALORES_PADRAO.keys() if k.lower() == p.lower()), p)
        ajustadas.append(hit)
    return ajustadas

def ultima_forma_pagto_cliente(df_base, cliente):
    if df_base.empty or not cliente:
        return None
    df = df_base[(df_base["Cliente"] == cliente) & (df_base["Conta"].str.lower() != "fiado")].copy()
    if df.empty:
        return None
    try:
        df["__d"] = pd.to_datetime(df["Data"], format=DATA_FMT, errors="coerce")
        df = df.sort_values("__d", ascending=False)
    except Exception:
        pass
    return str(df.iloc[0]["Conta"]) if not df.empty else None

def inserir_despesas_lote(linhas_despesas):
    ss = conectar_sheets()
    ws, headers = garantir_aba_despesas(ss)

    def pega_col(nome_alvo):
        for h in headers:
            if h.strip().lower() == nome_alvo.strip().lower():
                return h
        return None

    col_data      = pega_col("Data") or "Data"
    col_prest     = pega_col("Prestador") or "Prestador"
    col_desc      = pega_col("Descrição") or "Descrição"
    col_valor     = pega_col("Valor") or "Valor"
    col_forma_pag = pega_col("Forma de Pagamento") or "Forma de Pagamento"

    if not headers:
        headers = [col_data, col_prest, col_desc, col_valor, col_forma_pag]
        ws.append_row(headers)

    for d in linhas_despesas:
        linha = {
            col_data: d.get("Data",""),
            col_prest: d.get("Prestador",""),
            col_desc: d.get("Descrição",""),
            col_valor: d.get("Valor",""),
            col_forma_pag: d.get("Forma de Pagamento",""),
        }
        ordered = [linha.get(h, "") for h in headers]
        ws.append_row(ordered, value_input_option="USER_ENTERED")

# ===== Página =====
df_base, df_lanc, df_pagt, clientes, combos_exist, servs_exist, contas_exist = carregar_tudo()

st.sidebar.header("Ações")
acao = st.sidebar.radio("Escolha:", ["➕ Lançar fiado","💰 Registrar pagamento","📋 Em aberto & exportação"])

# ---------- 1) Lançar fiado ----------
if acao == "➕ Lançar fiado":
    st.subheader("➕ Lançar fiado — cria UMA linha por serviço na Base Feminina (Conta='Fiado', StatusFiado='Em aberto')")

    c1, c2 = st.columns(2)
    with c1:
        cliente = st.selectbox("Cliente", options=[""] + clientes, index=0)
        if not cliente:
            cliente = st.text_input("Ou digite o nome do cliente", "")
        combo_str = st.selectbox("Combo (use 'unha mão+sobrancelhas')", [""] + combos_exist)
        servico_unico = st.selectbox("Ou selecione um serviço (se não usar combo)", [""] + servs_exist)
        funcionario = st.selectbox("Funcionário", ["Meire"], index=0)
    with c2:
        data_atend = st.date_input("Data do atendimento", value=date.today())
        venc = st.date_input("Vencimento (opcional)", value=date.today())
        fase = st.text_input("Fase", value="Dono + funcionário")
        tipo = st.selectbox("Tipo", ["Serviço", "Produto"], index=0)
        periodo = st.selectbox("Período (opcional)", ["", "Manhã", "Tarde", "Noite"], index=0)

    servicos = parse_combo(combo_str) if combo_str else ([servico_unico] if servico_unico else [])
    valores_custom = {}
    if servicos:
        st.markdown("#### 💰 Edite os valores antes de salvar")
        for s in servicos:
            padrao = VALORES_PADRAO.get(s, 0.0)
            valores_custom[s] = st.number_input(
                f"{s} (padrão: R$ {padrao:.2f})", value=float(padrao), step=1.0, format="%.2f", key=f"valor_{s}"
            )

    if st.button("Salvar fiado", use_container_width=True):
        if not cliente:
            st.error("Informe o cliente.")
        elif not servicos:
            st.error("Informe combo ou um serviço.")
        else:
            idl = gerar_id("L")
            data_str = data_atend.strftime(DATA_FMT)
            venc_str = venc.strftime(DATA_FMT) if venc else ""

            novas = []
            for s in servicos:
                valor_item = float(valores_custom.get(s, VALORES_PADRAO.get(s, 0.0)))
                novas.append({
                    "Data": data_str, "Serviço": s, "Valor": valor_item, "Conta": "Fiado",
                    "Cliente": cliente, "Combo": combo_str if combo_str else "", "Funcionário": funcionario,
                    "Fase": fase, "Tipo": tipo, "Período": periodo,
                    "StatusFiado": "Em aberto", "IDLancFiado": idl, "VencimentoFiado": venc_str,
                    "DataPagamento": ""
                })

            ss = conectar_sheets()
            ws_base = ss.worksheet(ABA_BASE)
            dfb = get_as_dataframe(ws_base, evaluate_formulas=True, header=0).dropna(how="all")
            for c in BASE_COLS_MIN + EXTRA_COLS:
                if c not in dfb.columns:
                    dfb[c] = ""
            dfb = pd.concat([dfb, pd.DataFrame(novas)], ignore_index=True)
            salvar_df(ABA_BASE, dfb)

            total = float(pd.to_numeric(pd.DataFrame(novas)["Valor"], errors="coerce").fillna(0).sum())
            append_row(ABA_LANC, [idl, data_str, cliente, combo_str, "+".join(servicos),
                                  total, venc_str, funcionario, fase, tipo, periodo])

            st.success(f"Fiado criado para **{cliente}** — ID: {idl}. Geradas {len(novas)} linhas na Base Feminina.")
            st.cache_data.clear()

# ---------- 2) Registrar pagamento (COMPETÊNCIA) ----------
elif acao == "💰 Registrar pagamento":
    st.subheader("💰 Registrar pagamento — escolha o cliente e depois o(s) fiado(s) em aberto")

    df_abertos = df_base[df_base.get("StatusFiado", "") == "Em aberto"].copy()
    clientes_abertos = sorted(df_abertos["Cliente"].dropna().unique().tolist())

    colc1, colc2 = st.columns([1, 1])
    with colc1:
        cliente_sel = st.selectbox("Cliente com fiado em aberto", options=[""] + clientes_abertos, index=0)

    ultima = ultima_forma_pagto_cliente(df_base, cliente_sel) if cliente_sel else None
    lista_contas = contas_exist or ["Pix", "Dinheiro", "Cartão", "Transferência", "Outro"]
    default_idx = lista_contas.index(ultima) if (ultima in lista_contas) else 0
    with colc2:
        forma_pag = st.selectbox("Forma de pagamento (quitação)", options=lista_contas, index=default_idx)

    # IDs do cliente com rótulo amigável
    ids_opcoes = []
    if cliente_sel:
        grupo_cli = df_abertos[df_abertos["Cliente"] == cliente_sel].copy()
        grupo_cli["Data"] = pd.to_datetime(grupo_cli["Data"], errors="coerce").dt.strftime(DATA_FMT)
        grupo_cli["Valor"] = pd.to_numeric(grupo_cli["Valor"], errors="coerce").fillna(0)

        def atraso_max(idval):
            v = grupo_cli.loc[grupo_cli["IDLancFiado"] == idval, "VencimentoFiado"].dropna().astype(str)
            try:
                vdt = pd.to_datetime(v.iloc[0], format=DATA_FMT, errors="coerce").date() if not v.empty else None
            except Exception:
                vdt = None
            if vdt:
                d = (date.today() - vdt).days
                return d if d > 0 else 0
            return 0

        resumo_ids = (
            grupo_cli.groupby("IDLancFiado", as_index=False)
            .agg(Data=("Data", "min"), ValorTotal=("Valor", "sum"), Qtde=("Serviço", "count"), Combo=("Combo", "first"))
        )
        for _, r in resumo_ids.iterrows():
            atraso = atraso_max(r["IDLancFiado"])
            badge = "Em dia" if atraso <= 0 else f"{int(atraso)}d atraso"
            rotulo = f"{r['IDLancFiado']} • {r['Data']} • {int(r['Qtde'])} serv. • R$ {r['ValorTotal']:.2f} • {badge}"
            if pd.notna(r["Combo"]) and str(r["Combo"]).strip():
                rotulo += f" • {r['Combo']}"
            ids_opcoes.append((r["IDLancFiado"], rotulo))

    ids_valores = [i[0] for i in ids_opcoes]
    labels = {i: l for i, l in ids_opcoes}

    select_all = st.checkbox("Selecionar todos os fiados deste cliente", value=False, disabled=not bool(ids_valores))
    id_selecionados = st.multiselect(
        "Selecione 1 ou mais fiados do cliente",
        options=ids_valores,
        default=(ids_valores if select_all else []),
        format_func=lambda x: labels.get(x, x),
    )

    cold1, cold2 = st.columns([1, 1])
    with cold1:
        data_pag = st.date_input("Data do pagamento", value=date.today())
    with cold2:
        obs = st.text_input("Observação (opcional)", "")

    total_sel = 0.0
    bloco_comissao = {}
    registrar_comissao = False

    if id_selecionados:
        subset = df_abertos[df_abertos["IDLancFiado"].isin(id_selecionados)].copy()
        subset["Valor"] = pd.to_numeric(subset["Valor"], errors="coerce").fillna(0)
        total_sel = float(subset["Valor"].sum())

        st.info(
            f"Cliente: **{cliente_sel}** • IDs: {', '.join(id_selecionados)} • "
            f"Total: **R$ {total_sel:,.2f}**".replace(",", "X").replace(".", ",").replace("X", ".")
        )

        resumo_srv = (
            subset.groupby("Serviço", as_index=False)
            .agg(Qtd=("Serviço", "count"), Total=("Valor", "sum"))
            .sort_values(["Qtd", "Total"], ascending=[False, False])
        )
        resumo_srv["Total"] = resumo_srv["Total"].map(
            lambda v: f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        )
        st.caption("Resumo por serviço selecionado:")
        st.dataframe(resumo_srv, use_container_width=True, hide_index=True)

        # Comissão (desativada por padrão; ative se quiser lançar p/ Meire)
        st.markdown("---")
        funcs = subset["Funcionário"].dropna().astype(str).unique().tolist()
        registrar_comissao = st.checkbox("Registrar comissão na aba Despesas agora", value=False)

        subset["__DataAtend"] = pd.to_datetime(subset["Data"], format=DATA_FMT, errors="coerce").dt.date
        ref_date_por_func = {}
        for f in funcs:
            datas_f = set(subset.loc[subset["Funcionário"] == f, "__DataAtend"].dropna().tolist())
            ref_date_por_func[f] = list(datas_f)[0] if len(datas_f) == 1 else None

        if registrar_comissao:
            st.caption("Edite os valores sugeridos (padrão 50% do subtotal por funcionário).")
            for f in funcs:
                subf = subset[subset["Funcionário"] == f]
                subtotal_f = float(subf["Valor"].sum())

                st.markdown(f"**Funcionário:** {f}")
                c1, c2, c3, c4 = st.columns([1,1,1,2])
                with c1:
                    ref_dt = ref_date_por_func.get(f)
                    data_base = ref_dt if ref_dt is not None else data_pag
                    data_desp_f = st.date_input(f"Data da despesa ({f})", value=data_base, key=f"dt_{f}")
                with c2:
                    forma_desp_f = st.selectbox(f"Forma de Pagamento ({f})",
                                                options=["Dinheiro","Pix","Cartão","Transferência","Outro"],
                                                index=0, key=f"fp_{f}")
                with c3:
                    valor_sug = round(subtotal_f * 0.50, 2)
                    valor_com_f = st.number_input(f"Valor comissão ({f}) — sugestão 50%",
                                                  value=float(valor_sug), min_value=0.0, step=1.0, format="%.2f",
                                                  key=f"vl_{f}")
                with c4:
                    desc_f = st.text_input(f"Descrição ({f})", value=f"Comissão {f}", key=f"ds_{f}")

                bloco_comissao[f] = {
                    "Data": data_desp_f.strftime(DATA_FMT),
                    "Prestador": f,
                    "Descrição": desc_f,
                    "Valor": valor_com_f,
                    "Forma de Pagamento": forma_desp_f,
                }

    disabled_btn = not (cliente_sel and id_selecionados and forma_pag)
    if st.button("Registrar pagamento", use_container_width=True, disabled=disabled_btn):
        ss = conectar_sheets()
        ws_base = ss.worksheet(ABA_BASE)
        dfb = get_as_dataframe(ws_base, evaluate_formulas=True, header=0).dropna(how="all")

        if "DataPagamento" not in dfb.columns:
            dfb["DataPagamento"] = ""

        mask = dfb.get("IDLancFiado", "").isin(id_selecionados)
        if not mask.any():
            st.error("Nenhuma linha encontrada para os IDs selecionados.")
        else:
            subset_all = dfb[mask].copy()
            subset_all["Valor"] = pd.to_numeric(subset_all["Valor"], errors="coerce").fillna(0)
            total_pago = float(subset_all["Valor"].sum())

            # Atualiza por COMPETÊNCIA (não cria linhas novas)
            dfb.loc[mask, "Conta"] = forma_pag
            dfb.loc[mask, "StatusFiado"] = "Pago"
            dfb.loc[mask, "VencimentoFiado"] = ""
            dfb.loc[mask, "DataPagamento"] = data_pag.strftime(DATA_FMT)

            salvar_df(ABA_BASE, dfb)

            append_row(
                ABA_PAGT,
                [
                    f"P-{datetime.now(TZ).strftime('%Y%m%d%H%M%S%f')[:-3]}",
                    ";".join(id_selecionados),
                    data_pag.strftime(DATA_FMT),
                    cliente_sel,
                    forma_pag,
                    total_pago,
                    obs,
                ],
            )

            if registrar_comissao and bloco_comissao:
                linhas = []
                for func, dados in bloco_comissao.items():
                    if float(dados.get("Valor", 0) or 0) <= 0:
                        continue
                    linhas.append(dados)
                if linhas:
                    try:
                        inserir_despesas_lote(linhas)
                        st.success("Comissão lançada na aba **Despesas**.")
                    except Exception as e:
                        st.warning(f"Pagamento quitado, mas houve problema ao lançar comissão em Despesas: {e}")

            st.success(
                f"Pagamento registrado para **{cliente_sel}** (competência). "
                f"IDs quitados: {', '.join(id_selecionados)}. "
                f"Total: R$ {total_pago:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            )
            st.cache_data.clear()

# ---------- 3) Em aberto & exportação ----------
else:
    st.subheader("📋 Fiados em aberto (agrupados por ID) — Feminino")
    if df_base.empty:
        st.info("Sem dados.")
    else:
        em_aberto = df_base[df_base.get("StatusFiado","") == "Em aberto"].copy()
        if em_aberto.empty:
            st.success("Nenhum fiado em aberto 🎉")
        else:
            colf1, colf2 = st.columns([2,1])
            with colf1:
                filtro_cliente = st.text_input("Filtrar por cliente (opcional)", "")
                if filtro_cliente.strip():
                    em_aberto = em_aberto[
                        em_aberto["Cliente"].str.contains(filtro_cliente.strip(), case=False, na=False)
                    ]
            with colf2:
                funcionarios_abertos = sorted(
                    em_aberto["Funcionário"].dropna().astype(str).unique().tolist()
                )
                filtro_func = st.selectbox("Filtrar por funcionário (opcional)", [""] + funcionarios_abertos)
                if filtro_func:
                    em_aberto = em_aberto[em_aberto["Funcionário"] == filtro_func]

            hoje = date.today()
            def parse_dt(x):
                try:
                    return datetime.strptime(str(x), DATA_FMT).date()
                except Exception:
                    return None
            em_aberto["__venc"] = em_aberto["VencimentoFiado"].apply(parse_dt)
            em_aberto["DiasAtraso"] = em_aberto["__venc"].apply(
                lambda d: (hoje - d).days if (d is not None and hoje > d) else 0
            )
            em_aberto["Situação"] = em_aberto["DiasAtraso"].apply(lambda n: "Em dia" if n<=0 else f"{int(n)}d atraso")

            em_aberto["Valor"] = pd.to_numeric(em_aberto["Valor"], errors="coerce").fillna(0)
            resumo = (
                em_aberto.groupby(["IDLancFiado","Cliente"], as_index=False)
                .agg(ValorTotal=("Valor","sum"), QtdeServicos=("Serviço","count"),
                     Combo=("Combo","first"), MaxAtraso=("DiasAtraso","max"))
            )
            resumo["Situação"] = resumo["MaxAtraso"].apply(lambda n: "Em dia" if n<=0 else f"{int(n)}d atraso")

            st.dataframe(
                resumo.sort_values(["MaxAtraso","ValorTotal"], ascending=[False, False])[[
                    "IDLancFiado","Cliente","ValorTotal","QtdeServicos","Combo","Situação"
                ]],
                use_container_width=True, hide_index=True
            )

            total = float(resumo["ValorTotal"].sum())
            st.metric("Total em aberto", f"R$ {total:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))

            try:
                from openpyxl import Workbook  # noqa
                buf = BytesIO()
                with pd.ExcelWriter(buf, engine="openpyxl") as w:
                    em_aberto.sort_values(["Cliente","IDLancFiado","Data"]).to_excel(
                        w, index=False, sheet_name="Fiado_Em_Aberto_Feminino"
                    )
                st.download_button("⬇️ Exportar (Excel)", data=buf.getvalue(), file_name="fiado_em_aberto_feminino.xlsx")
            except Exception:
                csv_bytes = em_aberto.sort_values(["Cliente","IDLancFiado","Data"]).to_csv(
                    index=False
                ).encode("utf-8-sig")
                st.download_button("⬇️ Exportar (CSV)", data=csv_bytes, file_name="fiado_em_aberto_feminino.csv")
