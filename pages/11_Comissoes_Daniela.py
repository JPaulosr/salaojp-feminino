# 11_Adicionar_Atendimento.py — VERSÃO FEMININO COMPLETA
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
from collections import Counter

# =========================
# CONFIG
# =========================
SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"

# >>> Abas FEMININO <<<
ABA_DADOS = "Base de Dados Feminino"
STATUS_ABA = "clientes_status_feminino"

FOTO_COL_CANDIDATES = ["link_foto", "foto", "imagem", "url_foto", "foto_link", "link", "image"]

TZ = "America/Sao_Paulo"
REL_MULT = 1.5
DATA_FMT = "%d/%m/%Y"

COLS_OFICIAIS = [
    "Data", "Serviço", "Valor", "Conta", "Cliente", "Combo",
    "Funcionário", "Fase", "Tipo", "Período"
]
COLS_FIADO = ["StatusFiado", "IDLancFiado", "VencimentoFiado", "DataPagamento"]

# Extras para pagamento com cartão (gravamos também na Base)
COLS_PAG_EXTRAS = [
    "ValorBrutoRecebido", "ValorLiquidoRecebido",
    "TaxaCartaoValor", "TaxaCartaoPct",
    "FormaPagDetalhe", "PagamentoID"
]

# Funcionárias FEM (ajuste aqui se necessário)
FUNCIONARIOS_FEM = ["Daniela", "Meire"]

# =========================
# TELEGRAM (IDs)
# =========================
TELEGRAM_TOKEN_CONST = "8257359388:AAGayJElTPT0pQadtamVf8LoL7R6EfWzFGE"
TELEGRAM_CHAT_ID_JPAULO_CONST = "493747253"
TELEGRAM_CHAT_ID_VINICIUS_CONST = "-1001234567890"
TELEGRAM_CHAT_ID_FEMININO_CONST = "-1002965378062"  # Salão JP Feminino

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

def _get_chat_id_vini() -> str | None:
    return _get_secret("TELEGRAM_CHAT_ID_VINICIUS", TELEGRAM_CHAT_ID_VINICIUS_CONST)

def _get_chat_id_fem() -> str | None:
    return _get_secret("TELEGRAM_CHAT_ID_FEMININO", TELEGRAM_CHAT_ID_FEMININO_CONST)

def _check_tg_ready(token: str | None, chat_id: str | None) -> bool:
    return bool((token or "").strip() and (chat_id or "").strip())

def _chat_id_por_func(funcionario: str) -> str | None:
    if funcionario == "Vinicius":
        return _get_chat_id_vini()
    if funcionario in FUNCIONARIOS_FEM:
        return _get_chat_id_fem()
    return _get_chat_id_jp()

# =========================
# UTILS
# =========================
def _norm(s: str) -> str:
    s = (s or "").strip().casefold()
    s = unicodedata.normalize("NFD", s)
    return "".join(ch for ch in s if unicodedata.category(ch) != "Mn")

def _norm_key(s: str) -> str:
    return unicodedata.normalize("NFKC", str(s).strip()).casefold()

def classificar_relative(dias, media):
    if media is None: return ("⚪ Sem média", "Sem média")
    if dias <= media: return ("🟢 Em dia", "Em dia")
    elif dias <= media * REL_MULT: return ("🟠 Pouco atrasado", "Pouco atrasado")
    else: return ("🔴 Muito atrasado", "Muito atrasado")

def now_br():
    return datetime.now(pytz.timezone(TZ)).strftime("%d/%m/%Y %H:%M:%S")

def _cap_first(s: str) -> str:
    return (str(s).strip().lower().capitalize()) if s is not None else ""

def contains_cartao(s: str) -> bool:
    MAQ = {
        "cart", "cartao", "cartão",
        "credito", "crédito", "debito", "débito",
        "maquina", "maquininha", "maquineta", "pos",
        "pagseguro", "mercadopago", "mercado pago",
        "sumup", "stone", "cielo", "rede", "getnet", "safra",
        "visa", "master", "elo", "hiper", "amex",
        "nubank", "nubank cnpj"
    }
    x = unicodedata.normalize("NFKD", (s or "")).encode("ascii", "ignore").decode("ascii")
    x = x.lower().replace(" ", "")
    return any(k in x for k in MAQ)

def is_nao_cartao(conta: str) -> bool:
    """True se a forma de pagamento NÃO for cartão (ex.: PIX, Dinheiro, Transferência)."""
    s = unicodedata.normalize("NFKD", (conta or "")).encode("ascii","ignore").decode("ascii").lower()
    tokens = {"pix", "dinheiro", "carteira", "cash", "especie", "espécie", "transfer", "transferencia", "transferência", "ted", "doc"}
    return any(t in s for t in tokens)

def default_card_flag(conta: str) -> bool:
    """Padrão do interruptor 'Tratar como cartão?' – desliga em PIX/transferência/dinheiro."""
    if is_nao_cartao(conta):
        return False
    return contains_cartao(conta)

def gerar_pag_id(prefixo="A"):
    return f"{prefixo}-{datetime.now(pytz.timezone(TZ)).strftime('%Y%m%d%H%M%S%f')[:-3]}"

def _fmt_brl(v: float) -> str:
    try:
        v = float(v)
    except Exception:
        v = 0.0
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# =========================
# SHEETS
# =========================
@st.cache_resource
def conectar_sheets():
    info = st.secrets["GCP_SERVICE_ACCOUNT"]
    escopo = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credenciais = Credentials.from_service_account_info(info, scopes=escopo)
    cliente = gspread.authorize(credenciais)
    return cliente.open_by_key(SHEET_ID)

def ler_cabecalho(aba):
    try:
        headers = aba.row_values(1)
        return [h.strip() for h in headers] if headers else []
    except Exception:
        return []

def _cmap(ws):
    headers = ler_cabecalho(ws)
    cmap = {}
    for i, h in enumerate(headers):
        k = _norm_key(h)
        if k and k not in cmap:
            cmap[k] = i + 1
    return cmap

def format_extras_numeric(ws):
    """Força formato numérico/percentual nas colunas extras (evita virar '07:12:00')."""
    cmap = _cmap(ws)
    def fmt(name, ntype, pattern):
        c = cmap.get(_norm_key(name))
        if not c: return
        a1_from = rowcol_to_a1(2, c)
        a1_to = rowcol_to_a1(50000, c)
        try:
            ws.format(f"{a1_from}:{a1_to}", {"numberFormat": {"type": ntype, "pattern": pattern}})
        except Exception:
            pass
    fmt("ValorBrutoRecebido", "NUMBER", "0.00")
    fmt("ValorLiquidoRecebido", "NUMBER", "0.00")
    fmt("TaxaCartaoValor", "NUMBER", "0.00")
    fmt("TaxaCartaoPct", "PERCENT", "0.00%")

def carregar_base():
    aba = conectar_sheets().worksheet(ABA_DADOS)
    df = get_as_dataframe(aba).dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]
    df = df.loc[:, ~pd.Index(df.columns).duplicated(keep="first")]
    for c in [*COLS_OFICIAIS, *COLS_FIADO, *COLS_PAG_EXTRAS]:
        if c not in df.columns:
            df[c] = ""
    norm = {"manha": "Manhã", "Manha": "Manhã", "manha ": "Manhã", "tarde": "Tarde", "noite": "Noite"}
    df["Período"] = df["Período"].astype(str).str.strip().replace(norm)
    df.loc[~df["Período"].isin(["Manhã", "Tarde", "Noite"]), "Período"] = ""
    df["Combo"] = df["Combo"].fillna("")
    return df, aba

def salvar_base(df_final: pd.DataFrame):
    aba = conectar_sheets().worksheet(ABA_DADOS)
    headers_existentes = ler_cabecalho(aba) or [*COLS_OFICIAIS, *COLS_FIADO, *COLS_PAG_EXTRAS]
    colunas_alvo = list(dict.fromkeys([*headers_existentes, *COLS_OFICIAIS, *COLS_FIADO, *COLS_PAG_EXTRAS]))
    for c in colunas_alvo:
        if c not in df_final.columns:
            df_final[c] = ""
    df_final = df_final[colunas_alvo]
    aba.clear()
    set_with_dataframe(aba, df_final, include_index=False, include_column_header=True)
    try:
        format_extras_numeric(aba)
    except Exception:
        pass

# =========================
# FOTOS (status sheet)
# =========================
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
        cli_col = next((cols_lower[c] for c in ["cliente", "nome", "nome_cliente"] if c in cols_lower), None)
        if not (foto_col and cli_col): return {}
        tmp = df[[cli_col, foto_col]].copy()
        tmp.columns = ["Cliente", "Foto"]
        tmp["k"] = tmp["Cliente"].astype(str).map(_norm)
        return {r["k"]: str(r["Foto"]).strip() for _, r in tmp.iterrows() if str(r["Foto"]).strip()}
    except Exception:
        return {}
FOTOS = carregar_fotos_mapa()

# =========================
# TELEGRAM – envio
# =========================
def tg_send(text: str, chat_id: str | None = None) -> bool:
    token = _get_token()
    chat = chat_id or _get_chat_id_jp()
    if not _check_tg_ready(token, chat):
        return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        r = requests.post(url, json=payload, timeout=30)
        js = r.json() if r.headers.get("content-type","").startswith("application/json") else {}
        return bool(r.ok and js.get("ok"))
    except Exception:
        return False

def tg_send_photo(photo_url: str, caption: str, chat_id: str | None = None) -> bool:
    token = _get_token()
    chat = chat_id or _get_chat_id_jp()
    if not _check_tg_ready(token, chat):
        return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendPhoto"
        payload = {"chat_id": chat, "photo": photo_url, "caption": caption, "parse_mode": "HTML"}
        r = requests.post(url, data=payload, timeout=30)
        js = r.json() if r.headers.get("content-type","").startswith("application/json") else {}
        if r.ok and js.get("ok"):
            return True
        # fallback: manda texto
        return tg_send(caption, chat_id=chat)
    except Exception:
        # fallback: manda texto
        return tg_send(caption, chat_id=chat)

# ---- BOTÃO/HELPER: enviar direto ao canal do Salão Feminino ----
def enviar_para_salao_feminino(df_all, cliente, data_str, funcionario):
    # monta legenda padrão com extras do cartão
    servico_label, valor_total, _, _, periodo_label = _resumo_do_dia(df_all, cliente, data_str)
    caption = make_card_caption_v2(
        df_all, cliente, data_str, funcionario, servico_label, valor_total, periodo_label,
        append_sections=[_secao_pag_cartao(df_all, cliente, data_str)]
    )
    foto = FOTOS.get(_norm(cliente))
    chat_fem = _get_chat_id_fem()
    if foto:
        return tg_send_photo(foto, caption, chat_id=chat_fem)
    return tg_send(caption, chat_id=chat_fem)

# =========================
# CARD – resumo/histórico + BLOCO CARTÃO
# =========================
def _resumo_do_dia(df_all: pd.DataFrame, cliente: str, data_str: str):
    d = df_all[
        (df_all["Cliente"].astype(str).str.strip() == cliente) &
        (df_all["Data"].astype(str).str.strip() == data_str)
    ].copy()

    d["Valor"] = pd.to_numeric(d["Valor"], errors="coerce").fillna(0.0)
    servicos = [str(s).strip() for s in d["Serviço"].fillna("").tolist() if str(s).strip()]
    valor_total = float(d["Valor"].sum()) if not d.empty else 0.0
    is_combo = len(servicos) > 1 or (d["Combo"].fillna("").str.strip() != "").any()

    if servicos:
        label = " + ".join(servicos) + (" (Combo)" if is_combo else " (Simples)")
    else:
        label = "-"

    periodo_vals = [p for p in d["Período"].astype(str).str.strip().tolist() if p]
    periodo_label = max(set(periodo_vals), key=periodo_vals.count) if periodo_vals else "-"

    return label, valor_total, is_combo, servicos, periodo_label

def _ano_from_date_str(data_str: str) -> int | None:
    dt = pd.to_datetime(data_str, format=DATA_FMT, errors="coerce")
    return None if pd.isna(dt) else int(dt.year)

def _year_sections_for_jpaulo(df_all: pd.DataFrame, cliente: str, ano: int) -> tuple[str, str]:
    d = df_all.copy()
    d = d[d["Cliente"].astype(str).str.strip() == cliente].copy()
    d["_dt"] = pd.to_datetime(d["Data"], format=DATA_FMT, errors="coerce")
    d = d.dropna(subset=["_dt"])
    d["ano"] = d["_dt"].dt.year
    d = d[d["ano"] == ano].copy()

    if d.empty:
        return (f"📚 <b>Histórico por ano</b>\n{ano}: R$ 0,00", f"🧾 <b>{ano}: por serviço</b>\n—")

    d["Valor"] = pd.to_numeric(d["Valor"], errors="coerce").fillna(0.0)
    total_ano = float(d["Valor"].sum())
    sec_hist = "📚 <b>Histórico por ano</b>\n" + f"{ano}: <b>{_fmt_brl(total_ano)}</b>"

    grp = (
        d.dropna(subset=["Serviço"])
         .assign(Serviço=lambda x: x["Serviço"].astype(str).str.strip())
         .groupby("Serviço", as_index=False)
         .agg(qtd=("Serviço", "count"), total=("Valor", "sum"))
         .sort_values(["total", "qtd"], ascending=[False, False])
    )
    linhas_serv = [
        f"{r['Serviço']}: <b>{int(r['qtd'])}×</b> • <b>{_fmt_brl(float(r['total']))}</b>"
        for _, r in grp.iterrows()
    ]
    sec_serv = "🧾 <b>{}: por serviço</b>\n{}".format(ano, "\n".join(linhas_serv) if linhas_serv else "—")

    freq_dias = Counter()
    for dia, bloco in d.groupby(d["_dt"].dt.date):
        func_most = (bloco["Funcionário"].astype(str).str.strip()
                     .value_counts(dropna=False).idxmax() if not bloco.empty else "-")
        if func_most in ["JPaulo", "Vinicius"] + FUNCIONARIOS_FEM:
            freq_dias[func_most] += 1
    if freq_dias:
        ordem = ["JPaulo", "Vinicius"] + FUNCIONARIOS_FEM
        linhas_func = [f"{f}: <b>{freq_dias.get(f,0)}</b> visita(s)" for f in ordem if f in freq_dias]
        sec_serv += "\n\n👥 <b>Frequência por funcionário</b>\n" + "\n".join(linhas_func)

    return sec_hist, sec_serv

def _secao_pag_cartao(df_all: pd.DataFrame, cliente: str, data_str: str) -> str:
    df = df_all[
        (df_all["Cliente"].astype(str).str.strip() == cliente) &
        (df_all["Data"].astype(str).str.strip() == data_str)
    ].copy()
    if df.empty:
        return ""

    df["_idx"] = df.index
    com_pid = df[df["PagamentoID"].astype(str).str.strip() != ""].copy()
    if com_pid.empty:
        return ""

    latest_row = com_pid.loc[com_pid["_idx"].idxmax()]
    pid = str(latest_row["PagamentoID"]).strip()
    bloco = df[df["PagamentoID"].astype(str).str.strip() == pid].copy()

    bruto  = pd.to_numeric(bloco.get("ValorBrutoRecebido", 0), errors="coerce").fillna(0).sum()
    liqui  = pd.to_numeric(bloco.get("ValorLiquidoRecebido", 0), errors="coerce").fillna(0).sum()
    taxa_v = pd.to_numeric(bloco.get("TaxaCartaoValor", 0), errors="coerce").fillna(0).sum()
    if liqui <= 0:
        liqui = pd.to_numeric(bloco.get("Valor", 0), errors="coerce").fillna(0).sum()
    taxa_pct = (taxa_v / bruto * 100.0) if bruto > 0 else 0.0

    det = ""
    if "FormaPagDetalhe" in bloco.columns:
        s = bloco["FormaPagDetalhe"].astype(str).str.strip()
        s = s[s != ""]
        if not s.empty:
            det = s.iloc[0]
    conta = ""
    if "Conta" in bloco.columns:
        s2 = bloco["Conta"].astype(str).str.strip()
        s2 = s2[s2 != ""]
        if not s2.empty:
            conta = s2.iloc[0]

    linhas = [
        "------------------------------",
        "💳 <b>Pagamento no cartão</b>",
        f"Forma: <b>{conta or '-'}</b>{(' · ' + det) if det else ''}",
        f"Bruto: <b>{_fmt_brl(bruto)}</b> · Líquido: <b>{_fmt_brl(liqui)}</b>",
        f"Taxa total: <b>{_fmt_brl(taxa_v)} ({taxa_pct:.2f}%)</b>",
    ]
    return "\n".join(linhas)

def make_card_caption_v2(df_all, cliente, data_str, funcionario, servico_label, valor_total, periodo_label,
                         append_sections: list[str] | None = None):
    d_hist = df_all[df_all["Cliente"].astype(str).str.strip() == cliente].copy()
    d_hist["_dt"] = pd.to_datetime(d_hist["Data"], format=DATA_FMT, errors="coerce")
    d_hist = d_hist.dropna(subset=["_dt"]).sort_values("_dt")

    unique_days = sorted(set(d_hist["_dt"].dt.date.tolist()))
    total_atend = len(unique_days)

    dt_atual = pd.to_datetime(data_str, format=DATA_FMT, errors="coerce")
    dia_atual = None if pd.isna(dt_atual) else dt_atual.date()

    prev_days = [d for d in unique_days if (dia_atual is None or d < dia_atual)]
    prev_date = prev_days[-1] if prev_days else None

    if prev_date is not None:
        ultimo_reg = d_hist[d_hist["_dt"].dt.date == prev_date].iloc[-1]
        ultimo_func = str(ultimo_reg.get("Funcionário", "-"))
    else:
        ultimo_func = "-"

    if prev_date is not None and dia_atual is not None:
        dias_str = f"{(dia_atual - prev_date).days} dias"
    else:
        dias_str = "-"

    if len(unique_days) >= 2:
        ts = [pd.to_datetime(x) for x in unique_days]
        diffs = [(ts[i] - ts[i-1]).days for i in range(1, len(ts))]
        media = sum(diffs) / len(diffs) if diffs else None
    else:
        media = None

    media_str = "-" if media is None else f"{media:.1f} dias".replace(".", ",")
    valor_str = _fmt_brl(valor_total)

    base = (
        "📌 <b>Atendimento registrado</b>\n"
        f"👤 Cliente: <b>{cliente}</b>\n"
        f"🗓️ Data: <b>{data_str}</b>\n"
        f"🕒 Período: <b>{periodo_label}</b>\n"
        f"✂️ Serviço: <b>{servico_label}</b>\n"
        f"💰 Valor: <b>{valor_str}</b>\n"
        f"👩‍🦰 Atendido por: <b>{funcionario}</b>"
    )

    if append_sections:
        base += "\n\n" + "\n\n".join([s for s in append_sections if s and s.strip()])

    return base

def enviar_card(df_all, cliente, funcionario, data_str, servico=None, valor=None, combo=None):
    if servico is None or valor is None:
        servico_label, valor_total, _, _, periodo_label = _resumo_do_dia(df_all, cliente, data_str)
    else:
        is_combo = bool(combo and str(combo).strip())
        servico_label = (f"{servico} (Combo)" if is_combo and "+" in str(servico)
                         else f"{servico} (Simples)" if not is_combo else f"{servico} (Combo)")
        valor_total = float(valor)
        _, _, _, _, periodo_label = _resumo_do_dia(df_all, cliente, data_str)

    sec_cartao = _secao_pag_cartao(df_all, cliente, data_str)
    extras_base = [sec_cartao] if sec_cartao else []

    ano = _ano_from_date_str(data_str)
    extras_jp = extras_base.copy()
    if ano is not None:
        sec_hist, sec_serv = _year_sections_for_jpaulo(df_all, cliente, ano)
        extras_jp.extend([sec_hist, sec_serv])

    foto = FOTOS.get(_norm(cliente))

    caption_base = make_card_caption_v2(
        df_all, cliente, data_str, funcionario, servico_label, valor_total, "-",
        append_sections=extras_base
    )
    caption_jp = make_card_caption_v2(
        df_all, cliente, data_str, funcionario, servico_label, valor_total, "-",
        append_sections=extras_jp
    )

    # VINICIUS: canal dele + JP
    if funcionario == "Vinicius":
        chat_v = _get_chat_id_vini()
        if foto: tg_send_photo(foto, caption_base, chat_id=chat_v)
        else:    tg_send(caption_base, chat_id=chat_v)
        chat_jp = _get_chat_id_jp()
        if foto: tg_send_photo(foto, caption_jp, chat_id=chat_jp)
        else:    tg_send(caption_jp, chat_id=chat_jp)
        return

    # FEMININO: canal feminino + JP
    if funcionario in FUNCIONARIOS_FEM:
        chat_fem = _get_chat_id_fem()
        if foto: tg_send_photo(foto, caption_base, chat_id=chat_fem)
        else:    tg_send(caption_base, chat_id=chat_fem)
        chat_jp = _get_chat_id_jp()
        if foto: tg_send_photo(foto, caption_jp, chat_id=chat_jp)
        else:    tg_send(caption_jp, chat_id=chat_jp)
        return

    # Fallback (outros): envia para JP
    destino = _chat_id_por_func(funcionario)
    if foto: tg_send_photo(foto, caption_base, chat_id=destino)
    else:    tg_send(caption_base, chat_id=destino)

# =========================
# VALORES DE SERVIÇO (exemplos; ajuste conforme necessário)
# =========================
VALORES = {
    "Corte": 35.0, "Escova": 25.0, "Unha mão": 25.0, "Unha pé": 25.0, "Sobrancelhas": 25.0,
    "Designer de Henna": 30.0, "Manicure": 25.0, "Pedicure": 30.0, "Progressiva": 150.0,
}
def obter_valor_servico(servico):
    for k, v in VALORES.items():
        if k.lower() == str(servico).lower():
            return v
    return 0.0

def _preencher_fiado_vazio(linha: dict):
    for c in [*COLS_FIADO, *COLS_PAG_EXTRAS]:
        linha.setdefault(c, "")
    return linha

def ja_existe_atendimento(cliente, data, servico, combo=""):
    df, _ = carregar_base()
    df["Combo"] = df["Combo"].fillna("")
    servico_norm = _cap_first(servico)
    df_serv_norm = df["Serviço"].astype(str).map(_cap_first)
    f = (
        (df["Cliente"].astype(str).str.strip() == cliente) &
        (df["Data"].astype(str).str.strip() == data) &
        (df_serv_norm == servico_norm) &
        (df["Combo"].astype(str).str.strip() == str(combo).strip())
    )
    return not df[f].empty

def sugestoes_do_cliente(df_all, cli, conta_default, periodo_default, funcionario_default):
    d = df_all[df_all["Cliente"].astype(str).str.strip() == cli].copy()
    if d.empty: return conta_default, periodo_default, funcionario_default
    d["_dt"] = pd.to_datetime(d["Data"], format=DATA_FMT, errors="coerce")
    d = d.dropna(subset=["_dt"]).sort_values("_dt")
    if d.empty: return conta_default, periodo_default, funcionario_default
    ultima = d.iloc[-1]
    conta = (ultima.get("Conta") or "").strip() or conta_default
    periodo = (ultima.get("Período") or "").strip() or periodo_default
    func = (ultima.get("Funcionário") or "").strip() or funcionario_default
    if periodo not in ["Manhã", "Tarde", "Noite"]: periodo = periodo_default
    # no feminino, prioriza Daniela/Meire como sugestão
    if func not in FUNCIONARIOS_FEM + ["JPaulo", "Vinicius"]:
        func = funcionario_default
    return conta, periodo, func

# =========================
# UI – Cabeçalho
# =========================
st.set_page_config(layout="wide")
st.title("📅 Adicionar Atendimento (Feminino)")

# =========================
# DADOS BASE PARA SUGESTÕES
# =========================
df_existente, _ = carregar_base()
df_existente["_dt"] = pd.to_datetime(df_existente["Data"], format=DATA_FMT, errors="coerce")
df_2025 = df_existente[df_existente["_dt"].dt.year == 2025]

clientes_existentes = sorted(df_2025["Cliente"].dropna().unique())
df_2025 = df_2025[df_2025["Serviço"].notna()].copy()
servicos_existentes = sorted(df_2025["Serviço"].str.strip().unique())
contas_existentes = sorted([c for c in df_2025["Conta"].dropna().astype(str).str.strip().unique() if c])
combos_existentes = sorted([c for c in df_2025["Combo"].dropna().astype(str).str.strip().unique() if c])

# =========================
# FORM – Globais
# =========================
modo_lote = st.toggle("📦 Cadastro em Lote (vários clientes de uma vez)", value=False)

col1, col2 = st.columns(2)
with col1:
    data = st.date_input("Data", value=datetime.today()).strftime("%d/%m/%Y")
    conta_global = st.selectbox(
        "Forma de Pagamento (padrão)",
        list(dict.fromkeys(contas_existentes + ["Carteira", "Pix", "Transferência",
                                               "Nubank CNPJ", "Nubank", "Pagseguro", "Mercado Pago"]))
    )
with col2:
    funcionario_global = st.selectbox("Funcionário (padrão)", FUNCIONARIOS_FEM, index=0)
    tipo = st.selectbox("Tipo", ["Serviço", "Produto"])
periodo_global = st.selectbox("Período do Atendimento (padrão)", ["Manhã", "Tarde", "Noite"])
fase = "Dono + funcionário"

# =========================
# MODO UM POR VEZ
# =========================
if not modo_lote:
    cA, cB = st.columns(2)
    with cA:
        cliente = st.selectbox("Nome do Cliente", clientes_existentes)
    with cB:
        novo_nome = st.text_input("Ou digite um novo nome de cliente")
        cliente = novo_nome if novo_nome else cliente

    sug_conta, sug_periodo, sug_func = sugestoes_do_cliente(
        df_existente, cliente, conta_global, periodo_global, funcionario_global
    )
    conta = st.selectbox(
        "Forma de Pagamento",
        list(dict.fromkeys([sug_conta] + contas_existentes +
                           ["Carteira", "Pix", "Transferência", "Nubank CNPJ", "Nubank", "Pagseguro", "Mercado Pago"]))
    )

    force_off = is_nao_cartao(conta)
    usar_cartao = st.checkbox(
        "Tratar como cartão (com taxa)?",
        value=(False if force_off else default_card_flag(conta)),
        key="flag_card_um",
        disabled=force_off,
        help=("Desabilitado para PIX/Dinheiro/Transferência." if force_off else None)
    )

    funcionario = st.selectbox("Funcionário", FUNCIONARIOS_FEM,
                               index=(FUNCIONARIOS_FEM.index(sug_func) if sug_func in FUNCIONARIOS_FEM else 0))
    periodo_opcao = st.selectbox("Período do Atendimento", ["Manhã", "Tarde", "Noite"],
                                 index=["Manhã", "Tarde", "Noite"].index(sug_periodo))

    ultimo = df_existente[df_existente["Cliente"] == cliente]
    ultimo = ultimo.sort_values("Data", ascending=False).iloc[0] if not ultimo.empty else None
    combo = ""
    if ultimo is not None:
        ult_combo = ultimo.get("Combo", "")
        combo = st.selectbox("Combo (último primeiro)", [""] + list(dict.fromkeys([ult_combo] + combos_existentes)))

    def bloco_cartao_ui(total_bruto_padrao: float):
        with st.expander("💳 Pagamento no cartão (informe o LÍQUIDO recebido)", expanded=True):
            c1, c2 = st.columns(2)
            with c1:
                liquido = st.number_input("Valor recebido (líquido)", value=float(total_bruto_padrao), step=1.0, format="%.2f")
                bandeira = st.selectbox("Bandeira", ["", "Visa", "Mastercard", "Elo", "Hipercard", "Amex", "Outros"], index=0)
            with c2:
                tipo_cartao = st.selectbox("Tipo", ["Débito", "Crédito"], index=1)
                parcelas = st.number_input("Parcelas (se crédito)", min_value=1, max_value=12, value=1, step=1)
            taxa_val = max(0.0, float(total_bruto_padrao) - float(liquido or 0.0))
            taxa_pct = (taxa_val / float(total_bruto_padrao) * 100.0) if total_bruto_padrao > 0 else 0.0
            st.caption(f"Taxa estimada: {_fmt_brl(taxa_val)} ({taxa_pct:.2f}%)")
            return float(liquido or 0.0), str(bandeira), str(tipo_cartao), int(parcelas), float(taxa_val), float(taxa_pct)

    if "combo_salvo" not in st.session_state:
        st.session_state.combo_salvo = False
    if "simples_salvo" not in st.session_state:
        st.session_state.simples_salvo = False
    if st.button("🧹 Limpar formulário"):
        st.session_state.combo_salvo = False
        st.session_state.simples_salvo = False
        st.rerun()

    # -------- COMBO (um por vez) --------
    if combo:
        st.subheader("💰 Edite os valores do combo antes de salvar:")
        valores_customizados = {}
        for s in combo.split("+"):
            s2 = s.strip()
            valores_customizados[s2] = st.number_input(
                f"{s2} (padrão: R$ {obter_valor_servico(s2)})",
                value=obter_valor_servico(s2), step=1.0, key=f"valor_{s2}"
            )

        liquido_total = None
        bandeira = ""
        tipo_cartao = "Crédito"
        parcelas = 1
        dist_modo = "Proporcional (padrão)"
        alvo_servico = None

        if usar_cartao and not is_nao_cartao(conta):
            with st.expander("💳 Pagamento no cartão (informe o LÍQUIDO recebido)", expanded=True):
                c1, c2 = st.columns(2)
                with c1:
                    total_bruto_combo = float(sum(valores_customizados.values()))
                    liquido_total = st.number_input("Valor recebido (líquido)", value=total_bruto_combo, step=1.0, format="%.2f")
                    bandeira = st.selectbox("Bandeira", ["", "Visa", "Mastercard", "Elo", "Hipercard", "Amex", "Outros"], index=0)
                with c2:
                    tipo_cartao = st.selectbox("Tipo", ["Débito", "Crédito"], index=1)
                    parcelas = st.number_input("Parcelas (se crédito)", min_value=1, max_value=12, value=1, step=1)

                dist_modo = st.radio("Distribuição do desconto/taxa",
                                     ["Proporcional (padrão)", "Concentrar em um serviço"],
                                     horizontal=False)
                if dist_modo == "Concentrar em um serviço":
                    alvo_servico = st.selectbox("Aplicar TODO o desconto/taxa em", list(valores_customizados.keys()))

                taxa_val = max(0.0, total_bruto_combo - float(liquido_total or 0.0))
                taxa_pct = (taxa_val / total_bruto_combo * 100.0) if total_bruto_combo > 0 else 0.0
                st.caption(f"Taxa estimada: {_fmt_brl(taxa_val)} ({taxa_pct:.2f}%)")

        if not st.session_state.combo_salvo and st.button("✅ Confirmar e Salvar Combo"):
            duplicado = any(ja_existe_atendimento(cliente, data, _cap_first(s), combo) for s in combo.split("+"))
            if duplicado:
                st.warning("⚠️ Combo já registrado para este cliente e data.")
            else:
                df_all, _ = carregar_base()
                novas = []
                total_bruto = float(sum(valores_customizados.values()))
                usar_cartao_efetivo = usar_cartao and not is_nao_cartao(conta)
                id_pag = gerar_pag_id("A") if usar_cartao_efetivo else ""

                soma_outros = None
                if usar_cartao_efetivo and dist_modo == "Concentrar em um serviço" and alvo_servico:
                    soma_outros = sum(v for k, v in valores_customizados.items() if k != alvo_servico)

                for s in combo.split("+"):
                    s2_raw = s.strip()
                    s2_norm = _cap_first(s2_raw)
                    bruto_i = float(valores_customizados.get(s2_raw, obter_valor_servico(s2_norm)))

                    if usar_cartao_efetivo and total_bruto > 0:
                        if dist_modo == "Concentrar em um serviço" and alvo_servico:
                            if s2_raw == alvo_servico:
                                liq_i = float(liquido_total or 0.0) - float(soma_outros or 0.0)
                                liq_i = round(max(0.0, liq_i), 2)
                            else:
                                liq_i = round(bruto_i, 2)
                        else:
                            liq_i = round(float(liquido_total or 0.0) * (bruto_i / total_bruto), 2)

                        taxa_i = round(bruto_i - liq_i, 2)
                        taxa_pct_i = (taxa_i / bruto_i * 100.0) if bruto_i > 0 else 0.0
                        valor_para_base = liq_i
                        extras = {
                            "ValorBrutoRecebido": bruto_i,
                            "ValorLiquidoRecebido": liq_i,
                            "TaxaCartaoValor": taxa_i,
                            "TaxaCartaoPct": round(taxa_pct_i, 4),
                            "FormaPagDetalhe": f"{bandeira or '-'} | {tipo_cartao} | {int(parcelas)}x",
                            "PagamentoID": id_pag,
                        }
                    else:
                        valor_para_base = bruto_i
                        extras = {}

                    linha = _preencher_fiado_vazio({
                        "Data": data, "Serviço": s2_norm,
                        "Valor": valor_para_base,
                        "Conta": conta, "Cliente": cliente, "Combo": combo,
                        "Funcionário": funcionario, "Fase": fase, "Tipo": tipo, "Período": periodo_opcao,
                        **extras
                    })
                    novas.append(linha)

                if usar_cartao_efetivo and novas:
                    soma_liq = sum(float(n.get("Valor", 0) or 0) for n in novas)
                    delta = round(float(liquido_total or 0.0) - soma_liq, 2)
                    if abs(delta) >= 0.01:
                        idx_ajuste = len(novas) - 1
                        if dist_modo == "Concentrar em um serviço" and alvo_servico:
                            for i, n in enumerate(novas):
                                if _norm_key(n.get("Serviço","")) == _norm_key(_cap_first(alvo_servico)):
                                    idx_ajuste = i; break
                        novas[idx_ajuste]["Valor"] = float(novas[idx_ajuste]["Valor"]) + delta
                        bsel = float(novas[idx_ajuste].get("ValorBrutoRecebido", 0) or 0)
                        lsel = float(novas[idx_ajuste]["Valor"])
                        tsel = round(bsel - lsel, 2)
                        psel = (tsel / bsel * 100.0) if bsel > 0 else 0.0
                        novas[idx_ajuste]["ValorLiquidoRecebido"] = lsel
                        novas[idx_ajuste]["TaxaCartaoValor"] = tsel
                        novas[idx_ajuste]["TaxaCartaoPct"] = round(psel, 4)

                df_final = pd.concat([df_all, pd.DataFrame(novas)], ignore_index=True)
                salvar_base(df_final)
                st.session_state.combo_salvo = True
                st.success(f"✅ Atendimento salvo com sucesso para {cliente} no dia {data}.")
                enviar_card(
                    df_final, cliente, funcionario, data,
                    servico=combo.replace("+", " + "),
                    valor=sum(float(n["Valor"]) for n in novas),
                    combo=combo
                )

    # -------- SIMPLES (um por vez) --------
    else:
        st.subheader("✂️ Selecione o serviço e valor:")
        servico = st.selectbox("Serviço", servicos_existentes)
        valor = st.number_input("Valor", value=obter_valor_servico(servico), step=1.0)

        if usar_cartao and not is_nao_cartao(conta):
            liquido_total, bandeira, tipo_cartao, parcelas, _, _ = bloco_cartao_ui(valor)
        else:
            liquido_total, bandeira, tipo_cartao, parcelas = None, "", "Crédito", 1

        if not st.session_state.simples_salvo and st.button("📁 Salvar Atendimento"):
            servico_norm = _cap_first(servico)
            if ja_existe_atendimento(cliente, data, servico_norm):
                st.warning("⚠️ Atendimento já registrado para este cliente, data e serviço.")
            else:
                df_all, _ = carregar_base()
                usar_cartao_efetivo = usar_cartao and not is_nao_cartao(conta)
                if usar_cartao_efetivo:
                    id_pag = gerar_pag_id("A")
                    bruto = float(valor)
                    liq = float(liquido_total or 0.0)
                    taxa_v = round(max(0.0, bruto - liq), 2)
                    taxa_pct = round((taxa_v / bruto * 100.0), 4) if bruto > 0 else 0.0
                    nova = _preencher_fiado_vazio({
                        "Data": data, "Serviço": servico_norm, "Valor": liq, "Conta": conta,
                        "Cliente": cliente, "Combo": "", "Funcionário": funcionario,
                        "Fase": fase, "Tipo": tipo, "Período": periodo_opcao,
                        "ValorBrutoRecebido": bruto,
                        "ValorLiquidoRecebido": liq,
                        "TaxaCartaoValor": taxa_v,
                        "TaxaCartaoPct": taxa_pct,
                        "FormaPagDetalhe": f"{bandeira or '-'} | {tipo_cartao} | {int(parcelas)}x",
                        "PagamentoID": id_pag
                    })
                else:
                    nova = _preencher_fiado_vazio({
                        "Data": data, "Serviço": servico_norm, "Valor": valor, "Conta": conta,
                        "Cliente": cliente, "Combo": "", "Funcionário": funcionario,
                        "Fase": fase, "Tipo": tipo, "Período": periodo_opcao,
                    })
                df_final = pd.concat([df_all, pd.DataFrame([nova])], ignore_index=True)
                salvar_base(df_final)
                st.session_state.simples_salvo = True
                st.success(f"✅ Atendimento salvo com sucesso para {cliente} no dia {data}.")
                enviar_card(df_final, cliente, funcionario, data, servico=servico_norm, valor=float(nova["Valor"]), combo="")

    # ---- BOTÃO MANUAL: enviar para o SALÃO FEMININO ----
    st.divider()
    if st.button("📤 Enviar para Salão Feminino"):
        df_all_btn, _ = carregar_base()
        ok = enviar_para_salao_feminino(df_all_btn, cliente, data, funcionario)
        if ok:
            st.success("✅ Card enviado ao Salão Feminino.")
        else:
            st.error("❌ Não consegui enviar o card (verifique o token/chat_id do Telegram).")

# =========================
# MODO LOTE AVANÇADO
# =========================
else:
    st.info("Defina atendimento individual por cliente (misture combos e simples). Também escolha forma de pagamento, período e funcionário para cada um.")

    clientes_multi = st.multiselect("Clientes existentes", clientes_existentes)
    novos_nomes_raw = st.text_area("Ou cole novos nomes (um por linha)", value="")
    novos_nomes = [n.strip() for n in novos_nomes_raw.splitlines() if n.strip()]
    lista_final = list(dict.fromkeys(clientes_multi + novos_nomes))
    st.write(f"Total selecionados: **{len(lista_final)}**")

    enviar_cards = st.checkbox("Enviar card no Telegram após salvar", value=True)

    for cli in lista_final:
        with st.container(border=True):
            st.subheader(f"⚙️ Atendimento para {cli}")
            sug_conta, sug_periodo, sug_func = sugestoes_do_cliente(
                df_existente, cli, conta_global, periodo_global, funcionario_global
            )

            tipo_at = st.radio(f"Tipo de atendimento para {cli}", ["Simples", "Combo"], horizontal=True, key=f"tipo_{cli}")

            st.selectbox(
                f"Forma de Pagamento de {cli}",
                list(dict.fromkeys([sug_conta] + contas_existentes +
                                   ["Carteira", "Pix", "Transferência", "Nubank CNPJ", "Nubank", "Pagseguro", "Mercado Pago"]))    ,
                key=f"conta_{cli}"
            )

            # trava de cartão
            force_off_cli = is_nao_cartao(st.session_state.get(f"conta_{cli}", ""))

            st.checkbox(
                f"{cli} - Tratar como cartão (com taxa)?",
                value=(False if force_off_cli else default_card_flag(st.session_state.get(f"conta_{cli}", ""))),
                key=f"flag_card_{cli}",
                disabled=force_off_cli,
                help=("Desabilitado para PIX/Dinheiro/Transferência." if force_off_cli else None),
            )

            # uso efetivo
            use_card_cli = (not force_off_cli) and bool(st.session_state.get(f"flag_card_{cli}", False))

            st.selectbox(f"Período do Atendimento de {cli}", ["Manhã", "Tarde", "Noite"],
                         index=["Manhã", "Tarde", "Noite"].index(sug_periodo), key=f"periodo_{cli}")
            st.selectbox(f"Funcionário de {cli}", FUNCIONARIOS_FEM,
                         index=(FUNCIONARIOS_FEM.index(sug_func) if sug_func in FUNCIONARIOS_FEM else 0),
                         key=f"func_{cli}")

            if tipo_at == "Combo":
                st.selectbox(f"Combo para {cli} (formato corte+escova, etc.)", [""] + combos_existentes, key=f"combo_{cli}")
                combo_cli = st.session_state.get(f"combo_{cli}", "")
                if combo_cli:
                    total_padrao = 0.0
                    itens = []
                    for s in combo_cli.split("+"):
                        s2 = s.strip()
                        val = st.number_input(f"{cli} - {s2} (padrão: R$ {obter_valor_servico(s2)})",
                                              value=obter_valor_servico(s2), step=1.0, key=f"valor_{cli}_{s2}")
                        itens.append((s2, val))
                        total_padrao += float(val)

                    if use_card_cli and not is_nao_cartao(st.session_state.get(f"conta_{cli}", "")):
                        with st.expander(f"💳 {cli} - Pagamento no cartão", expanded=True):
                            c1, c2 = st.columns(2)
                            with c1:
                                st.number_input(f"{cli} - Valor recebido (líquido)", value=float(total_padrao), step=1.0, key=f"liq_{cli}")
                                st.selectbox(f"{cli} - Bandeira", ["", "Visa", "Mastercard", "Elo", "Hipercard", "Amex", "Outros"], index=0, key=f"bandeira_{cli}")
                            with c2:
                                st.selectbox(f"{cli} - Tipo", ["Débito", "Crédito"], index=1, key=f"tipo_cartao_{cli}")
                                st.number_input(f"{cli} - Parcelas", min_value=1, max_value=12, value=1, step=1, key=f"parc_{cli}")

                            st.radio(f"{cli} - Distribuição do desconto/taxa",
                                     ["Proporcional (padrão)", "Concentrar em um serviço"],
                                     horizontal=False, key=f"dist_{cli}")
                            if st.session_state.get(f"dist_{cli}", "Proporcional (padrão)") == "Concentrar em um serviço":
                                st.selectbox(f"{cli} - Aplicar TODO o desconto/taxa em",
                                             [nm for (nm, _) in itens], key=f"alvo_{cli}")

            else:
                st.selectbox(f"Serviço simples para {cli}", servicos_existentes, key=f"servico_{cli}")
                serv_cli = st.session_state.get(f"servico_{cli}", None)
                st.number_input(f"{cli} - Valor do serviço",
                                value=(obter_valor_servico(serv_cli) if serv_cli else 0.0),
                                step=1.0, key=f"valor_{cli}_simples")
                if use_card_cli and not is_nao_cartao(st.session_state.get(f"conta_{cli}", "")):
                    with st.expander(f"💳 {cli} - Pagamento no cartão", expanded=True):
                        c1, c2 = st.columns(2)
                        with c1:
                            st.number_input(f"{cli} - Valor recebido (líquido)", value=float(st.session_state.get(f"valor_{cli}_simples", 0.0)), step=1.0, key=f"liq_{cli}")
                            st.selectbox(f"{cli} - Bandeira", ["", "Visa", "Mastercard", "Elo", "Hipercard", "Amex", "Outros"], index=0, key=f"bandeira_{cli}")
                        with c2:
                            st.selectbox(f"{cli} - Tipo", ["Débito", "Crédito"], index=1, key=f"tipo_cartao_{cli}")
                            st.number_input(f"{cli} - Parcelas", min_value=1, max_value=12, value=1, step=1, key=f"parc_{cli}")

    if st.button("💾 Salvar TODOS atendimentos"):
        if not lista_final:
            st.warning("Selecione ou informe ao menos um cliente.")
        else:
            df_all, _ = carregar_base()
            novas, clientes_salvos = [], set()
            funcionario_por_cliente = {}

            for cli in lista_final:
                tipo_at = st.session_state.get(f"tipo_{cli}", "Simples")
                conta_cli = st.session_state.get(f"conta_{cli}", conta_global)
                use_card_cli = bool(st.session_state.get(f"flag_card_{cli}", False)) and not is_nao_cartao(conta_cli)
                periodo_cli = st.session_state.get(f"periodo_{cli}", periodo_global)
                func_cli = st.session_state.get(f"func_{cli}", funcionario_global)

                if tipo_at == "Combo":
                    combo_cli = st.session_state.get(f"combo_{cli}", "")
                    if not combo_cli:
                        st.warning(f"⚠️ {cli}: combo não definido. Pulando."); continue
                    if any(ja_existe_atendimento(cli, data, _cap_first(s), combo_cli) for s in str(combo_cli).split("+")):
                        st.warning(f"⚠️ {cli}: já existia COMBO em {data}. Pulando."); continue

                    itens = []
                    total_bruto = 0.0
                    for s in str(combo_cli).split("+"):
                        s2_raw = s.strip()
                        s2_norm = _cap_first(s2_raw)
                        val = float(st.session_state.get(f"valor_{cli}_{s2_raw}", obter_valor_servico(s2_norm)))
                        itens.append((s2_raw, s2_norm, val))
                        total_bruto += val

                    id_pag = gerar_pag_id("A") if use_card_cli else ""
                    liq_total_cli = float(st.session_state.get(f"liq_{cli}", total_bruto)) if use_card_cli else total_bruto

                    dist_modo = st.session_state.get(f"dist_{cli}", "Proporcional (padrão)")
                    alvo = st.session_state.get(f"alvo_{cli}", None)
                    soma_outros = None
                    if use_card_cli and dist_modo == "Concentrar em um serviço" and alvo:
                        soma_outros = sum(val for (r, _, val) in itens if r != alvo)

                    for (s_raw, s_norm, bruto_i) in itens:
                        if use_card_cli and total_bruto > 0:
                            if dist_modo == "Concentrar em um serviço" and alvo:
                                if s_raw == alvo:
                                    liq_i = liq_total_cli - float(soma_outros or 0.0)
                                    liq_i = round(max(0.0, liq_i), 2)
                                else:
                                    liq_i = round(bruto_i, 2)
                            else:
                                liq_i = round(liq_total_cli * (bruto_i / total_bruto), 2)

                            taxa_i = round(bruto_i - liq_i, 2)
                            taxa_pct_i = (taxa_i / bruto_i * 100.0) if bruto_i > 0 else 0.0
                            extras = {
                                "ValorBrutoRecebido": bruto_i,
                                "ValorLiquidoRecebido": liq_i,
                                "TaxaCartaoValor": taxa_i,
                                "TaxaCartaoPct": round(taxa_pct_i, 4),
                                "FormaPagDetalhe": f"{st.session_state.get(f'bandeira_{cli}','-')} | {st.session_state.get(f'tipo_cartao_{cli}','Crédito')} | {int(st.session_state.get(f'parc_{cli}',1))}x",
                                "PagamentoID": id_pag
                            }
                            valor_para_base = liq_i
                        else:
                            extras = {}
                            valor_para_base = bruto_i

                        novas.append(_preencher_fiado_vazio({
                            "Data": data, "Serviço": s_norm, "Valor": valor_para_base, "Conta": conta_cli,
                            "Cliente": cli, "Combo": combo_cli, "Funcionário": func_cli,
                            "Fase": fase, "Tipo": tipo, "Período": periodo_cli, **extras
                        }))

                    if use_card_cli:
                        indices_cli = [i for i, n in enumerate(novas) if n["Cliente"] == cli and n["Combo"] == combo_cli]
                        soma_liq = sum(float(novas[i]["Valor"]) for i in indices_cli)
                        delta = round(liq_total_cli - soma_liq, 2)
                        if abs(delta) >= 0.01 and indices_cli:
                            idx_ajuste = indices_cli[-1]
                            if dist_modo == "Concentrar em um serviço" and alvo:
                                for i in indices_cli:
                                    if _norm_key(novas[i]["Serviço"]) == _norm_key(_cap_first(alvo)):
                                        idx_ajuste = i; break
                            novas[idx_ajuste]["Valor"] = float(novas[idx_ajuste]["Valor"]) + delta
                            bsel = float(novas[idx_ajuste].get("ValorBrutoRecebido", 0) or 0)
                            lsel = float(novas[idx_ajuste]["Valor"])
                            tsel = round(bsel - lsel, 2)
                            psel = (tsel / bsel * 100.0) if bsel > 0 else 0.0
                            novas[idx_ajuste]["ValorLiquidoRecebido"] = lsel
                            novas[idx_ajuste]["TaxaCartaoValor"] = tsel
                            novas[idx_ajuste]["TaxaCartaoPct"] = round(psel, 4)

                    clientes_salvos.add(cli)
                    funcionario_por_cliente[cli] = func_cli

                else:
                    serv_cli = st.session_state.get(f"servico_{cli}", None)
                    serv_norm = _cap_first(serv_cli) if serv_cli else ""
                    if not serv_norm:
                        st.warning(f"⚠️ {cli}: serviço simples não definido. Pulando."); continue
                    if ja_existe_atendimento(cli, data, serv_norm):
                        st.warning(f"⚠️ {cli}: já existia atendimento simples ({serv_norm}) em {data}. Pulando."); continue
                    bruto = float(st.session_state.get(f"valor_{cli}_simples", obter_valor_servico(serv_norm)))

                    if use_card_cli:
                        liq = float(st.session_state.get(f"liq_{cli}", bruto))
                        taxa_v = round(max(0.0, bruto - liq), 2)
                        taxa_pct = round((taxa_v / bruto * 100.0), 4) if bruto > 0 else 0.0
                        novas.append(_preencher_fiado_vazio({
                            "Data": data, "Serviço": serv_norm, "Valor": liq, "Conta": conta_cli,
                            "Cliente": cli, "Combo": "", "Funcionário": func_cli,
                            "Fase": fase, "Tipo": tipo, "Período": periodo_cli,
                            "ValorBrutoRecebido": bruto, "ValorLiquidoRecebido": liq,
                            "TaxaCartaoValor": taxa_v, "TaxaCartaoPct": taxa_pct,
                            "FormaPagDetalhe": f"{st.session_state.get(f'bandeira_{cli}','-')} | {st.session_state.get(f'tipo_cartao_{cli}','Crédito')} | {int(st.session_state.get(f'parc_{cli}',1))}x",
                            "PagamentoID": gerar_pag_id("A")
                        }))
                    else:
                        novas.append(_preencher_fiado_vazio({
                            "Data": data, "Serviço": serv_norm, "Valor": bruto, "Conta": conta_cli,
                            "Cliente": cli, "Combo": "", "Funcionário": func_cli,
                            "Fase": fase, "Tipo": tipo, "Período": periodo_cli,
                        }))

                    clientes_salvos.add(cli)
                    funcionario_por_cliente[cli] = func_cli

            if not novas:
                st.warning("Nenhuma linha válida para inserir.")
            else:
                df_final = pd.concat([df_all, pd.DataFrame(novas)], ignore_index=True)
                salvar_base(df_final)
                st.success(f"✅ {len(novas)} linhas inseridas para {len(clientes_salvos)} cliente(s).")

                if enviar_cards:
                    for cli in sorted(clientes_salvos):
                        enviar_card(df_final, cli, funcionario_por_cliente.get(cli, FUNCIONARIOS_FEM[0]), data)

                # ---- BOTÃO MANUAL: reenviar cards do lote ao canal do salão feminino ----
                if st.button("📤 Reenviar cards ao Salão Feminino (lote)"):
                    df_all2, _ = carregar_base()
                    enviados = 0
                    for cli in sorted(clientes_salvos):
                        func_cli = funcionario_por_cliente.get(cli, FUNCIONARIOS_FEM[0])
                        if enviar_para_salao_feminino(df_all2, cli, data, func_cli):
                            enviados += 1
                    st.success(f"✅ {enviados} card(s) enviados ao Salão Feminino.")
