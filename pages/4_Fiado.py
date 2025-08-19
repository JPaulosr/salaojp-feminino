# -*- coding: utf-8 -*-
# 12_Fiado.py — App Feminino (Base & Fotos exclusivas + Telegram Feminino)
# - Usa apenas as abas femininas: "Base de Dados Feminino" e "clientes_status_feminino"
# - Roteamento Telegram: Meire → canal Feminino; demais → JP (cópia)
# - Lançar fiado, quitar por competência, cartões gravam LÍQUIDO em Valor

import streamlit as st
import pandas as pd
import gspread
import requests
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe
from gspread.utils import rowcol_to_a1
from datetime import date, datetime, timedelta
from io import BytesIO
import pytz, unicodedata

# =========================
# TELEGRAM (com fallback)
# =========================
TELEGRAM_TOKEN_CONST = "8257359388:AAGayJElTPT0pQadtamVf8LoL7R6EfWzFGE"
TELEGRAM_CHAT_ID_JPAULO_CONST = "493747253"
TELEGRAM_CHAT_ID_FEMININO_CONST = "-1002965378062"   # canal Salão JP Feminino

def _get_secret(name: str, default: str | None = None) -> str | None:
    try:
        val = st.secrets.get(name)
        val = (val or "").strip()
        if val:
            return val
    except Exception:
        pass
    return (default or "").strip() or None

def _get_token() -> str | None:
    return _get_secret("TELEGRAM_TOKEN", TELEGRAM_TOKEN_CONST)

def _get_chat_id_jp() -> str | None:
    return _get_secret("TELEGRAM_CHAT_ID_JPAULO", TELEGRAM_CHAT_ID_JPAULO_CONST)

def _get_chat_id_fem() -> str | None:
    return _get_secret("TELEGRAM_CHAT_ID_FEMININO", TELEGRAM_CHAT_ID_FEMININO_CONST)

def _ok(token, chat):  # validação simples
    return bool(str(token).strip() and str(chat).strip())

def tg_send(text: str, chat_id: str | None = None) -> bool:
    token = _get_token(); chat = chat_id or _get_chat_id_jp()
    if not _ok(token, chat): return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=30
        )
        j = r.json(); return bool(r.ok and j.get("ok"))
    except Exception:
        return False

def tg_send_photo(photo_url: str, caption: str, chat_id: str | None = None) -> bool:
    token = _get_token(); chat = chat_id or _get_chat_id_jp()
    if not _ok(token, chat): return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendPhoto",
            data={"chat_id": chat, "photo": photo_url, "caption": caption, "parse_mode": "HTML"},
            timeout=30
        )
        j = r.json()
        return bool(r.ok and j.get("ok")) or tg_send(caption, chat)
    except Exception:
        return tg_send(caption, chat)

# =========================
# Roteamento por funcionário (Feminino)
# =========================
FEMININO_FUNCS = {"meire"}  # adicione outras profissionais aqui

def _norm_name(s: str) -> str:
    s = unicodedata.normalize("NFKC", str(s or "").strip()).casefold()
    s = unicodedata.normalize("NFD", s)
    return "".join(ch for ch in s if unicodedata.category(ch) != "Mn")

def chat_por_funcionario(funcionario: str) -> str:
    return _get_chat_id_fem() if _norm_name(funcionario) in FEMININO_FUNCS else _get_chat_id_jp()

# =========================
# FOTOS (clientes_status_feminino)
# =========================
STATUS_ABA = "clientes_status_feminino"
FOTO_COL_CANDIDATES = ["link_foto", "foto", "imagem", "url_foto", "foto_link", "link", "image"]

def _norm(s: str) -> str:
    s = (s or "").strip().casefold()
    s = unicodedata.normalize("NFD", s)
    return "".join(ch for ch in s if unicodedata.category(ch) != "Mn")

@st.cache_data(show_spinner=False)
def carregar_fotos_mapa():
    try:
        sh = conectar_sheets()
        if STATUS_ABA not in [w.title for w in sh.worksheets()]:
            return {}
        ws = sh.worksheet(STATUS_ABA)
        df = get_as_dataframe(ws).fillna("")
        df.columns = [str(c).strip() for c in df.columns]
        df = df.loc[:, ~pd.Index(df.columns).duplicated(keep="first")]
        cols_lower = {c.lower(): c for c in df.columns}
        foto_col = next((cols_lower[c] for c in FOTO_COL_CANDIDATES if c in cols_lower), None)
        cli_col  = next((cols_lower[c] for c in ["cliente","nome","nome_cliente"] if c in cols_lower), None)
        if not (foto_col and cli_col): return {}
        tmp = df[[cli_col, foto_col]].copy()
        tmp.columns = ["Cliente", "Foto"]
        tmp["k"] = tmp["Cliente"].astype(str).map(_norm)
        return {r["k"]: str(r["Foto"]).strip() for _, r in tmp.iterrows() if str(r["Foto"]).strip()}
    except Exception:
        return {}

# =========================
# UTILS
# =========================
def proxima_terca(d: date) -> date:
    wd = d.weekday()
    return d + timedelta(days=(1 - wd) % 7)

def _fmt_brl(v: float) -> str:
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def _fmt_pct(p: float) -> str:
    try: return f"{p:.2f}%"
    except Exception: return "-"

def _norm_key(s: str) -> str:
    return unicodedata.normalize("NFKC", str(s).strip()).casefold()

def col_map(ws):
    headers = ws.row_values(1); cmap = {}
    for i, h in enumerate(headers):
        k = _norm_key(h)
        if k and k not in cmap: cmap[k] = i + 1
    return cmap

def ensure_headers(ws, desired_headers):
    headers = ws.row_values(1)
    if not headers:
        ws.append_row(desired_headers); return {h: i+1 for i, h in enumerate(desired_headers)}
    missing = [h for h in desired_headers if h not in headers]
    if missing:
        ws.update('A1', [headers + missing]); headers = ws.row_values(1)
    return {h: i+1 for i, h in enumerate(headers)}

def append_rows_generic(ws, dicts, default_headers=None):
    headers = ws.row_values(1)
    if not headers:
        headers = default_headers or sorted({k for d in dicts for k in d.keys()}); ws.append_row(headers)
    hdr_norm = [_norm_key(h) for h in headers]
    rows = []
    for d in dicts:
        d_norm = {_norm_key(k): v for k, v in d.items()}
        rows.append([d_norm.get(hn, "") for hn in hdr_norm])
    if rows: ws.append_rows(rows, value_input_option="USER_ENTERED")

def contains_cartao(s: str) -> bool:
    KW = {"cart","cartao","cartão","credito","crédito","debito","débito","maquina","maquininha","pos",
          "pagseguro","mercadopago","mercado pago","sumup","stone","cielo","rede","getnet","safra","visa","master","elo","nubank"}
    x = unicodedata.normalize("NFKD", (s or "")).encode("ascii","ignore").decode("ascii")
    x = x.lower().replace(" ", "")
    return any(k in x for k in KW)

def servicos_compactos_por_ids_parcial(df_rows: pd.DataFrame) -> str:
    if df_rows.empty: return "-"
    partes = []
    for _, grp in df_rows.groupby("IDLancFiado"):
        servs = sorted(set(grp["Serviço"].dropna().astype(str).str.strip().tolist()))
        partes.append("+".join(servs) if servs else "-")
    vistos, out = [], []
    for p in partes:
        if p and p not in vistos: vistos.append(p); out.append(p)
    return " | ".join(out) if out else "-"

def historico_cliente_por_ano(df_base: pd.DataFrame, cliente: str) -> dict[int, float]:
    if df_base is None or df_base.empty or not cliente: return {}
    df = df_base.copy()
    df["__dt"] = pd.to_datetime(df["Data"], format="%d/%m/%Y", errors="coerce")
    df["__valor"] = pd.to_numeric(df["Valor"], errors="coerce").fillna(0.0)
    df = df[(df["Cliente"].astype(str).str.strip() == str(cliente).strip()) & df["__dt"].notna()]
    if df.empty: return {}
    grp = df.groupby(df["__dt"].dt.year)["__valor"].sum().to_dict()
    return {int(ano): float(round(v, 2)) for ano, v in grp.items()}

def breakdown_por_servico_no_ano(df_base: pd.DataFrame, cliente: str, ano: int, max_itens: int = 8):
    if df_base is None or df_base.empty or not cliente or not ano:
        return pd.DataFrame(columns=["Serviço","Qtd","Total"]), 0, 0.0, 0, 0.0
    df = df_base.copy()
    df["__dt"] = pd.to_datetime(df["Data"], format="%d/%m/%Y", errors="coerce")
    df["__valor"] = pd.to_numeric(df["Valor"], errors="coerce").fillna(0.0)
    df = df[(df["Cliente"].astype(str).str.strip() == str(cliente).strip()) & (df["__dt"].dt.year == ano)]
    if df.empty:
        return pd.DataFrame(columns=["Serviço","Qtd","Total"]), 0, 0.0, 0, 0.0
    agg = (df.groupby("Serviço", dropna=True)
             .agg(Qtd=("Serviço","count"), Total=("__valor","sum"))
             .reset_index()).sort_values("Total", ascending=False)
    total_qtd = int(agg["Qtd"].sum()); total_val = float(agg["Total"].sum())
    top = agg.head(max_itens).copy()
    outros = agg.iloc[max_itens:] if len(agg) > max_itens else pd.DataFrame(columns=agg.columns)
    outros_qtd = int(outros["Qtd"].sum()) if not outros.empty else 0
    outros_val = float(outros["Total"].sum()) if not outros.empty else 0.0
    top["Qtd"] = top["Qtd"].astype(int); top["Total"] = top["Total"].astype(float).round(2)
    return top, total_qtd, total_val, outros_qtd, outros_val

def format_extras_numeric(ws):
    cmap = col_map(ws)
    def fmt(name, ntype, pattern):
        col = cmap.get(_norm_key(name))
        if not col: return
        a1_from = rowcol_to_a1(2, col); a1_to = rowcol_to_a1(50000, col)
        try: ws.format(f"{a1_from}:{a1_to}", {"numberFormat": {"type": ntype, "pattern": pattern}})
        except Exception: pass
    fmt("ValorBrutoRecebido","NUMBER","0.00")
    fmt("ValorLiquidoRecebido","NUMBER","0.00")
    fmt("TaxaCartaoValor","NUMBER","0.00")
    fmt("TaxaCartaoPct","PERCENT","0.00%")

# =========================
# APP / SHEETS (FEMININO)
# =========================
st.set_page_config(page_title="Fiado | Salão JP Feminino", page_icon="💅", layout="wide",
                   initial_sidebar_state="expanded")
st.title("💳 Controle de Fiado — Feminino")

SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"
ABA_BASE = "Base de Dados Feminino"          # 👈 FEMININO
ABA_LANC = "Fiado_Lancamentos_Fem"           # pode ser separado do masculino
ABA_PAGT = "Fiado_Pagamentos_Fem"
ABA_TAXAS = "Cartao_Taxas_Fem"

TZ = pytz.timezone("America/Sao_Paulo")
DATA_FMT = "%d/%m/%Y"

BASE_COLS_MIN = ["Data","Serviço","Valor","Conta","Cliente","Combo","Funcionário","Fase","Tipo","Período"]
EXTRA_COLS    = ["StatusFiado","IDLancFiado","VencimentoFiado","DataPagamento"]
BASE_PAG_EXTRAS = [
    "ValorBrutoRecebido","ValorLiquidoRecebido",
    "TaxaCartaoValor","TaxaCartaoPct",
    "FormaPagDetalhe","PagamentoID"
]
BASE_COLS_ALL = BASE_COLS_MIN + EXTRA_COLS + BASE_PAG_EXTRAS

VALORES_PADRAO = {
    "Unha pé": 25.0, "Unha mão": 25.0, "Pé/Mão": 50.0, "Designer de Henna": 30.0,
    "Escova": 35.0, "Progressiva": 150.0
}

COMISSAO_FUNCIONARIOS = set()  # vazio por padrão no Feminino (ajuste se precisar)
COMISSAO_PERC_PADRAO = 0.50

TAXAS_COLS = ["IDPagamento","Cliente","DataPag","Bandeira","Tipo","Parcelas","Bruto","Liquido","TaxaValor","TaxaPct","IDLancs"]
PAGT_COLS  = ["IDPagamento","IDLancs","DataPagamento","Cliente","Forma","TotalLiquido","Obs","TotalBruto","TaxaValor","TaxaPct"]

@st.cache_resource
def conectar_sheets():
    info = st.secrets["GCP_SERVICE_ACCOUNT"]
    scopes = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SHEET_ID)

def garantir_aba(ss, nome, cols):
    try:
        ws = ss.worksheet(nome)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=nome, rows=200, cols=max(10, len(cols)))
        ws.append_row(cols); return ws
    if not ws.row_values(1): ws.append_row(cols)
    return ws

def read_base_raw(ss):
    ws = garantir_aba(ss, ABA_BASE, BASE_COLS_ALL)
    ensure_headers(ws, BASE_COLS_ALL)
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
    df.columns = [str(c).strip() for c in df.columns]
    df = df.loc[:, ~pd.Index(df.columns).duplicated(keep="first")]
    for c in BASE_COLS_ALL:
        if c not in df.columns: df[c] = ""
    df = df[[*BASE_COLS_ALL, *[c for c in df.columns if c not in BASE_COLS_ALL]]]
    return df, ws

def append_rows_base(ws, novas_dicts):
    headers = ws.row_values(1)
    if not headers: headers = BASE_COLS_ALL; ws.append_row(headers)
    hdr_norm = [_norm_key(h) for h in headers]
    rows = []
    for d in novas_dicts:
        d_norm = {_norm_key(k): v for k, v in d.items()}
        rows.append([d_norm.get(hn, "") for hn in hdr_norm])
    if rows: ws.append_rows(rows, value_input_option="USER_ENTERED")

@st.cache_data
def carregar_listas():
    ss = conectar_sheets()
    ws_base = garantir_aba(ss, ABA_BASE, BASE_COLS_ALL)
    ensure_headers(ws_base, BASE_COLS_ALL)
    df_list = get_as_dataframe(ws_base, evaluate_formulas=True, header=0).fillna("")
    df_list.columns = [str(c).strip() for c in df_list.columns]
    df_list = df_list.loc[:, ~pd.Index(df_list.columns).duplicated(keep="first")]
    clientes = sorted([c for c in df_list.get("Cliente","").astype(str).str.strip().unique() if c])
    combos  = sorted([c for c in df_list.get("Combo","").astype(str).str.strip().unique() if c])
    servs   = sorted([s for s in df_list.get("Serviço","").astype(str).str.strip().unique() if s])
    contas_raw = [c for c in df_list.get("Conta","").astype(str).str.strip().unique() if c]
    base_contas = sorted([c for c in contas_raw if c.lower() != "fiado"])
    if "Nubank CNPJ" not in base_contas: base_contas.append("Nubank CNPJ")
    return clientes, combos, servs, base_contas

# ===== Caches
clientes_exist, combos_exist, servs_exist, contas_exist = carregar_listas()
FOTOS = carregar_fotos_mapa()

st.sidebar.header("Ações")
acao = st.sidebar.radio("Escolha:", ["➕ Lançar fiado","💰 Registrar pagamento","📋 Em aberto & exportação"])

# ---------- 1) Lançar fiado ----------
if acao == "➕ Lançar fiado":
    st.subheader("➕ Lançar fiado — Feminino (gera 1 linha por serviço)")

    c1, c2 = st.columns(2)
    with c1:
        cliente = st.selectbox("Cliente", options=[""] + clientes_exist, index=0)
        if not cliente:
            cliente = st.text_input("Ou digite o nome do cliente", "")
        combo_str = st.selectbox("Combo (use '+')", [""] + combos_exist)
        servico_unico = st.selectbox("Ou selecione um serviço (se não usar combo)", [""] + servs_exist)
        funcionario = st.selectbox("Funcionário(a)", ["Meire","Outro"], index=0)  # 👈 foco no Feminino
    with c2:
        data_atend = st.date_input("Data do atendimento", value=date.today())
        venc = st.date_input("Vencimento (opcional)", value=date.today())
        fase = st.text_input("Fase", value="Dono + funcionária")
        tipo = st.selectbox("Tipo", ["Serviço","Produto"], index=0)
        periodo = st.selectbox("Período (opcional)", ["","Manhã","Tarde","Noite"], index=0)

    def parse_combo(combo_str):
        if not combo_str: return []
        return [p.strip() for p in str(combo_str).split("+") if p.strip()]

    servicos = parse_combo(combo_str) if combo_str else ([servico_unico] if servico_unico else [])
    valores_custom = {}
    if servicos:
        st.markdown("#### 💰 Edite os valores antes de salvar")
        for s in servicos:
            padrao = VALORES_PADRAO.get(s, 0.0)
            valores_custom[s] = st.number_input(f"{s} (padrão: R$ {padrao:.2f})",
                                                value=float(padrao), step=1.0, format="%.2f", key=f"valor_{s}")

    if st.button("Salvar fiado", use_container_width=True):
        if not cliente:
            st.error("Informe o cliente.")
        elif not servicos:
            st.error("Informe combo ou um serviço.")
        else:
            idl = f"L-{datetime.now(TZ).strftime('%Y%m%d%H%M%S%f')[:-3]}"
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
                    "DataPagamento": "",
                    "ValorBrutoRecebido":"", "ValorLiquidoRecebido":"", "TaxaCartaoValor":"", "TaxaCartaoPct":"",
                    "FormaPagDetalhe":"", "PagamentoID":""
                })
            ss = conectar_sheets()
            ws_base = garantir_aba(ss, ABA_BASE, BASE_COLS_ALL)
            ensure_headers(ws_base, BASE_COLS_ALL)
            append_rows_base(ws_base, novas)

            total = float(pd.to_numeric(pd.DataFrame(novas)["Valor"], errors="coerce").fillna(0).sum())
            ws_l = garantir_aba(ss, ABA_LANC, ["IDLanc","Data","Cliente","Combo","Servicos","Total","Venc","Func","Fase","Tipo","Periodo"])
            append_rows_generic(ws_l, [{
                "IDLanc": idl, "Data": data_str, "Cliente": cliente, "Combo": combo_str,
                "Servicos": "+".join(servicos), "Total": total, "Venc": venc_str, "Func": funcionario,
                "Fase": fase, "Tipo": tipo, "Periodo": periodo
            }])

            st.success(f"Fiado criado para **{cliente}** — ID: {idl}.")
            st.cache_data.clear()

            # Envio: destino do Feminino + cópia para JP (se diferente)
            total_fmt = _fmt_brl(total)
            servicos_txt = combo_str.strip() if (combo_str and combo_str.strip()) else ("+".join(servicos) if servicos else "-")
            msg_html = (
                "🧾 <b>Novo fiado (Feminino)</b>\n"
                f"👤 Cliente: <b>{cliente}</b>\n"
                f"🧰 Serviço(s): <b>{servicos_txt}</b>\n"
                f"💵 Total: <b>{total_fmt}</b>\n"
                f"📅 Atendimento: {data_str}\n"
                f"⏳ Vencimento: {venc_str or '-'}\n"
                f"🆔 ID: <code>{idl}</code>"
            )
            foto = FOTOS.get(_norm(cliente))
            destino = chat_por_funcionario(funcionario)
            if foto: tg_send_photo(foto, msg_html, chat_id=destino)
            else:    tg_send(msg_html, chat_id=destino)
            if destino != _get_chat_id_jp():
                if foto: tg_send_photo(foto, "🧾 <b>Cópia</b>\n" + msg_html, chat_id=_get_chat_id_jp())
                else:    tg_send("🧾 <b>Cópia</b>\n" + msg_html, chat_id=_get_chat_id_jp())

# ---------- 2) Registrar pagamento ----------
elif acao == "💰 Registrar pagamento":
    st.subheader("💰 Registrar pagamento — Feminino")

    ss = conectar_sheets()
    df_base_full, ws_base = read_base_raw(ss)

    df_abertos = df_base_full[df_base_full.get("StatusFiado","") == "Em aberto"].copy()
    clientes_abertos = sorted(df_abertos["Cliente"].dropna().astype(str).str.strip().unique().tolist())

    colc1, colc2 = st.columns([1, 1])
    with colc1:
        cliente_sel = st.selectbox("Cliente com fiado em aberto", options=[""] + clientes_abertos, index=0)

    def ultima_forma_pagto_cliente(df_base, cliente):
        if df_base.empty or not cliente: return None
        df = df_base[(df_base["Cliente"] == cliente) & (df_base["Conta"].str.lower() != "fiado")].copy()
        if df.empty: return None
        try:
            df["__d"] = pd.to_datetime(df["Data"], format=DATA_FMT, errors="coerce")
            df = df.sort_values("__d", ascending=False)
        except Exception: pass
        return str(df.iloc[0]["Conta"]) if not df.empty else None

    ultima = ultima_forma_pagto_cliente(df_base_full, cliente_sel) if cliente_sel else None
    lista_contas_default = ["Pix","Dinheiro","Cartão","Transferência","Pagseguro","Mercado Pago","Nubank CNPJ",
                            "SumUp","Cielo","Stone","Getnet","Outro"]
    contas_exist = sorted(set([*lista_contas_default]))
    default_idx = contas_exist.index(ultima) if (ultima in contas_exist) else 0
    with colc2:
        forma_pag = st.selectbox("Forma de pagamento (quitação)", options=contas_exist, index=default_idx)

    modo_sel = st.radio("Modo de seleção", ["Por ID (combo inteiro)", "Por linha (serviço)"], index=0, horizontal=True)

    ids_opcoes, id_selecionados = [], []
    linhas_label_map, linhas_indices_sel = {}, []

    if cliente_sel:
        grupo_cli = df_abertos[df_abertos["Cliente"].astype(str).str.strip() == str(cliente_sel).strip()].copy()

        if modo_sel.startswith("Por ID"):
            grupo_cli["Data"]  = pd.to_datetime(grupo_cli["Data"], format=DATA_FMT, errors="coerce").dt.strftime(DATA_FMT)
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
                .agg(Data=("Data","min"), ValorTotal=("Valor","sum"), Qtde=("Serviço","count"), Combo=("Combo","first"))
            )
            for _, r in resumo_ids.iterrows():
                atraso = atraso_max(r["IDLancFiado"])
                badge = "Em dia" if atraso <= 0 else f"{int(atraso)}d atraso"
                rotulo = f"{r['IDLancFiado']} • {r['Data']} • {int(r['Qtde'])} serv. • R$ {r['ValorTotal']:.2f} • {badge}"
                if pd.notna(r["Combo"]) and str(r["Combo"]).strip(): rotulo += f" • {r['Combo']}"
                ids_opcoes.append((r["IDLancFiado"], rotulo))

            ids_valores = [i[0] for i in ids_opcoes]
            labels_id = {i: l for i, l in ids_opcoes}
            select_all_ids = st.checkbox("Selecionar todos deste cliente", value=False, disabled=not bool(ids_valores))
            id_selecionados = st.multiselect(
                "Selecione 1 ou mais fiados",
                options=ids_valores,
                default=(ids_valores if select_all_ids else []),
                format_func=lambda x: labels_id.get(x, x),
            )
        else:
            linhas_cli = grupo_cli.copy()
            linhas_cli["IdxBase"] = linhas_cli.index
            linhas_cli["DataFmt"] = pd.to_datetime(linhas_cli["Data"], format=DATA_FMT, errors="coerce").dt.strftime(DATA_FMT)
            linhas_cli["ValorNum"] = pd.to_numeric(linhas_cli["Valor"], errors="coerce").fillna(0.0)
            for _, r in linhas_cli.iterrows():
                lbl = f"{r['IDLancFiado']} • {r['DataFmt'] or '-'} • {r['Serviço']} • R$ {r['ValorNum']:.2f} • {r['Funcionário']}"
                linhas_label_map[int(r["IdxBase"])] = lbl
            linhas_todas = list(linhas_label_map.keys())
            select_all_linhas = st.checkbox("Selecionar todas as linhas", value=False, disabled=not bool(linhas_todas))
            linhas_indices_sel = st.multiselect(
                "Selecione linhas específicas",
                options=linhas_todas,
                default=(linhas_todas if select_all_linhas else []),
                format_func=lambda i: linhas_label_map.get(i, str(i)),
            )

    cold1, cold2 = st.columns(2)
    with cold1:
        data_pag = st.date_input("Data do pagamento", value=date.today())
    with cold2:
        obs = st.text_input("Observação (opcional)", "", key="obs")

    subset_preview = pd.DataFrame()
    if cliente_sel:
        if modo_sel.startswith("Por ID"):
            subset_preview = df_abertos[df_abertos["IDLancFiado"].isin(id_selecionados)].copy()
        else:
            subset_preview = df_abertos[df_abertos.index.isin(linhas_indices_sel)].copy()

    total_sel = 0.0
    valor_liquido_cartao = None
    bandeira_cartao = ""; tipo_cartao = "Crédito"; parcelas_cartao = 1

    if not subset_preview.empty:
        subset_preview["Valor"] = pd.to_numeric(subset_preview["Valor"], errors="coerce").fillna(0)
        total_sel = float(subset_preview["Valor"].sum())
        st.info(f"Cliente: **{cliente_sel}** • Total bruto selecionado: **{_fmt_brl(total_sel)}**")

        if contains_cartao(forma_pag):
            with st.expander("💳 Detalhes da maquininha (opcional)", expanded=True):
                c1, c2 = st.columns(2)
                with c1:
                    valor_liquido_cartao = st.number_input("Valor recebido (líquido)", value=float(total_sel), step=1.0, format="%.2f")
                    bandeira_cartao = st.selectbox("Bandeira", ["","Visa","Mastercard","Elo","Hipercard","Amex","Outros"], index=0)
                with c2:
                    tipo_cartao = st.selectbox("Tipo", ["Débito","Crédito"], index=1)
                    parcelas_cartao = st.number_input("Parcelas (crédito)", min_value=1, max_value=12, value=1, step=1)
                taxa_valor_est = max(0.0, float(total_sel) - float(valor_liquido_cartao or 0.0))
                taxa_pct_est = (taxa_valor_est/float(total_sel)*100.0) if total_sel>0 else 0.0
                st.metric("Taxa estimada", _fmt_brl(taxa_valor_est), _fmt_pct(taxa_pct_est))

        resumo_srv = (subset_preview.groupby("Serviço", as_index=False)
                      .agg(Qtd=("Serviço","count"), Total=("Valor","sum"))
                      .sort_values(["Qtd","Total"], ascending=[False,False]))
        resumo_srv["Total"] = resumo_srv["Total"].map(_fmt_brl)
        st.caption("Resumo por serviço:"); st.dataframe(resumo_srv, use_container_width=True, hide_index=True)

    tem_sel = (bool(id_selecionados) if modo_sel.startswith("Por ID") else bool(linhas_indices_sel))
    if st.button("Registrar pagamento", use_container_width=True, disabled=not (cliente_sel and tem_sel and forma_pag)):
        dfb, ws_base2 = read_base_raw(ss)
        ensure_headers(ws_base2, BASE_COLS_ALL); format_extras_numeric(ws_base2)

        mask = (dfb.get("IDLancFiado","").isin(id_selecionados)) if modo_sel.startswith("Por ID") else dfb.index.isin(linhas_indices_sel)
        if not mask.any():
            st.error("Nenhuma linha encontrada."); st.stop()

        subset_all = dfb[mask].copy()
        subset_all["Valor"] = pd.to_numeric(subset_all["Valor"], errors="coerce").fillna(0)
        total_bruto = float(subset_all["Valor"].sum())
        data_pag_str = data_pag.strftime(DATA_FMT)
        id_pag = f"P-{datetime.now(TZ).strftime('%Y%m%d%H%M%S%f')[:-3]}"
        total_liquido = float(valor_liquido_cartao or total_bruto) if contains_cartao(forma_pag) else total_bruto
        taxa_total_valor = max(0.0, total_bruto - total_liquido)
        taxa_total_pct   = (taxa_total_valor/total_bruto*100.0) if total_bruto>0 else 0.0

        headers_map = col_map(ws_base2)
        updates, liq_acum, idxs = [], 0.0, list(subset_all.index)
        for i, idx in enumerate(idxs):
            row_no = int(idx) + 2
            bruto_i = float(subset_all.loc[idx,"Valor"])
            liq_i = round(total_liquido * (bruto_i/total_bruto), 2) if total_bruto>0 else 0.0
            if i == len(idxs)-1: liq_i = round(total_liquido - liq_acum, 2)
            liq_acum += liq_i
            taxa_i = round(bruto_i - liq_i, 2)
            taxa_pct_i = (taxa_i/bruto_i*100.0) if bruto_i>0 else 0.0
            pairs = {
                "Conta": forma_pag, "StatusFiado": "Pago", "VencimentoFiado": "", "DataPagamento": data_pag_str,
                "Valor": liq_i, "ValorBrutoRecebido": bruto_i, "ValorLiquidoRecebido": liq_i,
                "TaxaCartaoValor": taxa_i, "TaxaCartaoPct": round(taxa_pct_i,4),
                "FormaPagDetalhe": f"{(bandeira_cartao or '-')} | {tipo_cartao} | {int(parcelas_cartao)}x" if contains_cartao(forma_pag) else "",
                "PagamentoID": id_pag
            }
            for col, val in pairs.items():
                c = headers_map.get(_norm_key(col))
                if c: updates.append({"range": rowcol_to_a1(row_no, c), "values": [[val]]})
        if updates: ws_base2.batch_update(updates, value_input_option="USER_ENTERED")

        # logs extras
        if contains_cartao(forma_pag):
            ws_taxas = garantir_aba(ss, ABA_TAXAS, TAXAS_COLS)
            ensure_headers(ws_taxas, TAXAS_COLS)
            append_rows_generic(ws_taxas, [{
                "IDPagamento": id_pag, "Cliente": cliente_sel, "DataPag": data_pag_str, "Bandeira": bandeira_cartao,
                "Tipo": tipo_cartao, "Parcelas": int(parcelas_cartao), "Bruto": total_bruto, "Liquido": total_liquido,
                "TaxaValor": round(taxa_total_valor,2), "TaxaPct": round(taxa_total_pct,4),
                "IDLancs": ";".join(sorted(set(subset_all["IDLancFiado"].astype(str))))
            }], default_headers=TAXAS_COLS)

        ws_p = garantir_aba(ss, ABA_PAGT, PAGT_COLS)
        ensure_headers(ws_p, PAGT_COLS)
        append_rows_generic(ws_p, [{
            "IDPagamento": id_pag, "IDLancs": ";".join(sorted(set(subset_all["IDLancFiado"].astype(str)))),
            "DataPagamento": data_pag_str, "Cliente": cliente_sel, "Forma": forma_pag,
            "TotalLiquido": total_liquido, "Obs": (obs or ""), "TotalBruto": total_bruto,
            "TaxaValor": round(taxa_total_valor,2), "TaxaPct": round(taxa_total_pct,4),
        }], default_headers=PAGT_COLS)

        st.success(f"Pagamento registrado para **{cliente_sel}**. Líquido: {_fmt_brl(total_liquido)} (bruto {_fmt_brl(total_bruto)}).")
        st.cache_data.clear()

        # Envio: Feminino + cópia JP
        serv_txt = servicos_compactos_por_ids_parcial(subset_all)
        ids_txt = ", ".join(sorted(set(subset_all["IDLancFiado"].astype(str))))
        msg_html = (
            "✅ <b>Fiado quitado (Feminino)</b>\n"
            f"👤 Cliente: <b>{cliente_sel}</b>\n"
            f"🧰 Serviço(s): <b>{serv_txt}</b>\n"
            f"💳 Forma: <b>{forma_pag}</b>\n"
            f"💵 Bruto: <b>{_fmt_brl(total_bruto)}</b>\n"
            f"💵 Líquido: <b>{_fmt_brl(total_liquido)}</b>\n"
            f"🧾 Taxa: <b>{_fmt_brl(taxa_total_valor)} ({_fmt_pct(taxa_total_pct)})</b>\n"
            f"📅 Data pagto: {data_pag_str}\n"
            f"🗂️ IDs: <code>{ids_txt}</code>\n"
            f"📝 Obs: {obs or '-'}"
        )
        foto = FOTOS.get(_norm(cliente_sel))
        destino = _get_chat_id_fem()
        if foto: tg_send_photo(foto, msg_html, chat_id=destino)
        else:    tg_send(msg_html, chat_id=destino)
        if destino != _get_chat_id_jp():
            if foto: tg_send_photo(foto, "🧾 <b>Cópia</b>\n" + msg_html, chat_id=_get_chat_id_jp())
            else:    tg_send("🧾 <b>Cópia</b>\n" + msg_html, chat_id=_get_chat_id_jp())

# ---------- 3) Em aberto & exportação ----------
else:
    st.subheader("📋 Fiados em aberto — Feminino (agrupados por ID)")
    ss = conectar_sheets()
    df_base_full, _ = read_base_raw(ss)

    if df_base_full.empty:
        st.info("Sem dados.")
    else:
        em_aberto = df_base_full[df_base_full.get("StatusFiado","") == "Em aberto"].copy()
        if em_aberto.empty:
            st.success("Nenhum fiado em aberto 🎉")
        else:
            colf1, colf2 = st.columns([2,1])
            with colf1:
                filtro_cliente = st.text_input("Filtrar por cliente (opcional)", "")
                if filtro_cliente.strip():
                    em_aberto = em_aberto[
                        em_aberto["Cliente"].astype(str).str.contains(filtro_cliente.strip(), case=False, na=False)
                    ]
            with colf2:
                funcs = sorted(em_aberto["Funcionário"].dropna().astype(str).unique().tolist())
                filtro_func = st.selectbox("Filtrar por funcionária (opcional)", [""] + funcs)
                if filtro_func:
                    em_aberto = em_aberto[em_aberto["Funcionário"] == filtro_func]

            hoje = date.today()
            def parse_dt(x):
                try: return datetime.strptime(str(x), DATA_FMT).date()
                except Exception: return None
            em_aberto["__venc"] = em_aberto["VencimentoFiado"].apply(parse_dt)
            em_aberto["DiasAtraso"] = em_aberto["__venc"].apply(lambda d: (hoje - d).days if (d and hoje>d) else 0)
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
            st.metric("Total em aberto", _fmt_brl(total))

            try:
                from openpyxl import Workbook  # noqa
                buf = BytesIO()
                with pd.ExcelWriter(buf, engine="openpyxl") as w:
                    em_aberto.sort_values(["Cliente","IDLancFiado","Data"]).to_excel(
                        w, index=False, sheet_name="Fiado_Em_Aberto_Fem"
                    )
                st.download_button("⬇️ Exportar (Excel)", data=buf.getvalue(), file_name="fiado_em_aberto_feminino.xlsx")
            except Exception:
                csv_bytes = em_aberto.sort_values(["Cliente","IDLancFiado","Data"]).to_csv(index=False).encode("utf-8-sig")
                st.download_button("⬇️ Exportar (CSV)", data=csv_bytes, file_name="fiado_em_aberto_feminino.csv")
