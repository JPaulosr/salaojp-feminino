# 11_Adicionar_Atendimento.py — VERSÃO FEMININO COMPLETA (atualizada c/ foto 200px)
# -*- coding: utf-8 -*-
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
from typing import Optional

# =========================
# CONFIG
# =========================
SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"

# >>> Abas FEMININO <<<
ABA_DADOS = "Base de Dados Feminino"
STATUS_ABA = "clientes_status_feminino"   # planilha com nome+foto

FOTO_COL_CANDIDATES = ["link_foto", "foto", "imagem", "url_foto", "foto_link", "link", "image"]

TZ = "America/Sao_Paulo"
REL_MULT = 1.5
DATA_FMT = "%d/%m/%Y"

COLS_OFICIAIS = [
    "Data", "Serviço", "Valor", "Conta", "Cliente", "Combo",
    "Funcionário", "Fase", "Tipo", "Período"
]
COLS_FIADO = ["StatusFiado", "IDLancFiado", "VencimentoFiado", "DataPagamento"]

# Extras para pagamento com cartão
COLS_PAG_EXTRAS = [
    "ValorBrutoRecebido", "ValorLiquidoRecebido",
    "TaxaCartaoValor", "TaxaCartaoPct",
    "FormaPagDetalhe", "PagamentoID"
]

FUNCIONARIOS_FEM = ["Daniela", "Meire"]

# =========================
# TELEGRAM IDs
# =========================
TELEGRAM_TOKEN = "8257359388:AAGayJElTPT0pQadtamVf8LoL7R6EfWzFGE"
TELEGRAM_CHAT_ID_JPAULO = "493747253"
TELEGRAM_CHAT_ID_VINICIUS = "-1001234567890"
TELEGRAM_CHAT_ID_FEMININO = "-1002965378062"   # canal geral Feminino (Meire)
TELEGRAM_CHAT_ID_DANIELA  = "-1003039502089"   # canal exclusivo clientes da Daniela

# =========================
# UTILS
# =========================
def _norm(s: str) -> str:
    s = (s or "").strip().casefold()
    s = unicodedata.normalize("NFD", s)
    return "".join(ch for ch in s if unicodedata.category(ch) != "Mn")

def _fmt_brl(v: float) -> str:
    try:
        v = float(v)
    except Exception:
        v = 0.0
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def gerar_pag_id(prefixo="A"):
    return f"{prefixo}-{datetime.now(pytz.timezone(TZ)).strftime('%Y%m%d%H%M%S%f')[:-3]}"

def _cap_first(s: str) -> str:
    return (str(s).strip().lower().capitalize()) if s is not None else ""

def is_nao_cartao(conta: str) -> bool:
    s = unicodedata.normalize("NFKD", (conta or "")).encode("ascii","ignore").decode("ascii").lower()
    tokens = {"pix", "dinheiro", "carteira", "cash", "especie", "espécie", "transfer", "transferencia", "transferência", "ted", "doc"}
    return any(t in s for t in tokens)

def default_card_flag(conta: str) -> bool:
    if is_nao_cartao(conta):
        return False
    # heurística simples: se tiver nome de máquina/bandeira, assume cartão
    x = unicodedata.normalize("NFKD", (conta or "")).encode("ascii","ignore").decode("ascii").lower().replace(" ","")
    sinais = ["cart", "credito", "debito", "pagseguro", "mercadopago", "sumup", "stone", "cielo", "rede", "getnet", "visa", "master", "elo", "hiper", "amex"]
    return any(k in x for k in sinais)

def _calc_payout_daniela(valor_total: float, pct: Optional[float]) -> str:
    if pct is None:
        return ""
    try:
        pctf = max(0.0, min(100.0, float(pct)))
    except Exception:
        pctf = 0.0
    valor_receber = round(float(valor_total or 0.0) * (pctf / 100.0), 2)
    return f"💰 Daniela recebe: <b>{_fmt_brl(valor_receber)}</b> ({pctf:.0f}%)"

# =========================
# GOOGLE SHEETS
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
        k = unicodedata.normalize("NFKC", str(h).strip()).casefold()
        if k and k not in cmap:
            cmap[k] = i + 1
    return cmap

def format_extras_numeric(ws):
    cmap = _cmap(ws)
    def fmt(name, ntype, pattern):
        c = cmap.get(unicodedata.normalize("NFKC", name).casefold())
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

def get_foto_url(nome: str) -> Optional[str]:
    if not nome: return None
    url = FOTOS.get(_norm(nome))
    return url if (url and url.strip()) else None

# =========================
# TELEGRAM – envio (com fallback)
# =========================
def tg_send(text: str, chat_id: str) -> bool:
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        r = requests.post(url, json=payload, timeout=30)
        return r.ok
    except Exception:
        return False

def tg_send_photo(photo_url: Optional[str], caption: str, chat_id: str) -> bool:
    if not photo_url:
        return tg_send(caption, chat_id)
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
        payload = {"chat_id": chat_id, "photo": photo_url, "caption": caption, "parse_mode": "HTML"}
        r = requests.post(url, data=payload, timeout=30)
        if r.ok: return True
        return tg_send(caption, chat_id)
    except Exception:
        return tg_send(caption, chat_id)

# =========================
# CARD – resumo/histórico
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
    label = " + ".join(servicos) + (" (Combo)" if is_combo else " (Simples)") if servicos else "-"
    periodo_vals = [p for p in d["Período"].astype(str).str.strip().tolist() if p]
    periodo_label = max(set(periodo_vals), key=periodo_vals.count) if periodo_vals else "-"
    return label, valor_total, is_combo, servicos, periodo_label

def _conta_do_dia(df_all: pd.DataFrame, cliente: str, data_str: str) -> Optional[str]:
    d = df_all[
        (df_all["Cliente"].astype(str).str.strip()==cliente) &
        (df_all["Data"].astype(str).str.strip()==data_str)
    ]
    if d.empty or "Conta" not in d.columns: return None
    s = d["Conta"].astype(str).str.strip()
    s = s[s!=""]
    try:
        return s.mode().iat[0]
    except Exception:
        return None

def make_card_caption(df_all, cliente, data_str, funcionario, servico_label, valor_total, periodo_label,
                      pct_daniela: Optional[float] = None,
                      append_sections=None,
                      conta_pag: Optional[str] = None):
    valor_str = _fmt_brl(valor_total)
    forma = (conta_pag or "-")
    base = (
        "📌 <b>Atendimento registrado</b>\n"
        f"👤 Cliente: <b>{cliente}</b>\n"
        f"🗓️ Data: <b>{data_str}</b>\n"
        f"🕒 Período: <b>{periodo_label}</b>\n"
        f"💳 Forma de pagamento: <b>{forma}</b>\n"
        f"✂️ Serviço: <b>{servico_label}</b>\n"
        f"💰 Valor total: <b>{valor_str}</b>\n"
        f"👩‍🦰 Atendido por: <b>{funcionario}</b>"
    )
    if funcionario == "Daniela" and pct_daniela is not None:
        base += "\n" + _calc_payout_daniela(valor_total, pct_daniela)
    if append_sections:
        base += "\n\n" + "\n\n".join([s for s in append_sections if s and s.strip()])
    return base

def enviar_card(df_all, cliente, funcionario, data_str,
                servico: Optional[str] = None,
                valor: Optional[float] = None,
                combo: Optional[str] = None,
                pct_daniela: Optional[float] = None,
                conta_pag: Optional[str] = None):
    if servico is None or valor is None:
        servico_label, valor_total, _, _, periodo_label = _resumo_do_dia(df_all, cliente, data_str)
    else:
        is_combo = bool(combo and str(combo).strip())
        servico_label = (f"{servico} (Combo)" if (is_combo or "+" in str(servico)) else f"{servico} (Simples)")
        valor_total = float(valor)
        _, _, _, _, periodo_label = _resumo_do_dia(df_all, cliente, data_str)

    if not conta_pag:
        conta_pag = _conta_do_dia(df_all, cliente, data_str)

    caption = make_card_caption(
        df_all, cliente, data_str, funcionario, servico_label, valor_total, periodo_label,
        pct_daniela=(pct_daniela if funcionario == "Daniela" else None),
        append_sections=None,
        conta_pag=conta_pag
    )

    foto = get_foto_url(cliente)

    # Roteamento
    if funcionario == "Vinicius":
        tg_send_photo(foto, caption, chat_id=TELEGRAM_CHAT_ID_VINICIUS)
        tg_send_photo(foto, caption, chat_id=TELEGRAM_CHAT_ID_JPAULO)
    elif funcionario == "Daniela":
        tg_send_photo(foto, caption, chat_id=TELEGRAM_CHAT_ID_DANIELA)
        tg_send_photo(foto, caption, chat_id=TELEGRAM_CHAT_ID_JPAULO)
    elif funcionario == "Meire":
        tg_send_photo(foto, caption, chat_id=TELEGRAM_CHAT_ID_FEMININO)
        tg_send_photo(foto, caption, chat_id=TELEGRAM_CHAT_ID_JPAULO)
    else:
        tg_send_photo(foto, caption, chat_id=TELEGRAM_CHAT_ID_JPAULO)

# =========================
# UI – FORMULÁRIO
# =========================
st.set_page_config(layout="wide", page_title="Adicionar Atendimento (Feminino)", page_icon="💇‍♀️")
st.title("📅 Adicionar Atendimento (Feminino)")

df_existente, _ = carregar_base()
df_existente["_dt"] = pd.to_datetime(df_existente["Data"], format=DATA_FMT, errors="coerce")
df_2025 = df_existente[df_existente["_dt"].dt.year == 2025]

clientes_existentes = sorted(df_2025["Cliente"].dropna().unique())
df_2025 = df_2025[df_2025["Serviço"].notna()].copy()
servicos_existentes = sorted(df_2025["Serviço"].str.strip().unique()) or ["Corte"]
contas_existentes = sorted([c for c in df_2025["Conta"].dropna().astype(str).str.strip().unique() if c])
combos_existentes = sorted([c for c in df_2025["Combo"].dropna().astype(str).str.strip().unique() if c])

modo_lote = st.toggle("📦 Cadastro em Lote (vários clientes de uma vez)", value=False)
data = st.date_input("Data", value=datetime.now(pytz.timezone(TZ)).date()).strftime("%d/%m/%Y")
fase = "Dono + funcionário"

if not modo_lote:
    # --- Cliente + Foto ---
    cliente = st.selectbox("Nome do Cliente", clientes_existentes)
    novo_nome = st.text_input("Ou digite um novo nome de cliente")
    if novo_nome.strip():
        cliente = novo_nome.strip()

    # FOTO 200px logo abaixo do nome
    foto_url = get_foto_url(cliente)
    if foto_url:
        st.image(foto_url, caption=cliente, width=200)

    # --- Pagamento / Funcionário / Período ---
    conta = st.selectbox(
        "Forma de Pagamento (Conta)",
        list(dict.fromkeys(contas_existentes + ["Carteira", "Pix", "Transferência", "Nubank CNPJ", "Nubank", "Pagseguro", "Mercado Pago"]))
    )
    funcionario = st.selectbox("Funcionário", FUNCIONARIOS_FEM, index=0)
    periodo_opcao = st.selectbox("Período do Atendimento", ["Manhã", "Tarde", "Noite"], index=0)
    tipo = st.selectbox("Tipo", ["Serviço", "Produto"], index=0)

    usar_cartao = st.checkbox(
        "Tratar como cartão (com taxa)?",
        value=(False if is_nao_cartao(conta) else default_card_flag(conta)),
        disabled=is_nao_cartao(conta),
        help=("Desabilitado para PIX/Dinheiro/Transferência." if is_nao_cartao(conta) else None)
    )

    # Percentual Daniela quando aplicável
    pct_daniela = None
    if funcionario == "Daniela":
        pct_daniela = st.number_input("Percentual da Daniela (%)", min_value=0.0, max_value=100.0, value=50.0, step=1.0)

    # --- Simples ou Combo ---
    tipo_at = st.radio("Tipo de lançamento", ["Simples", "Combo"], horizontal=True)
    if tipo_at == "Simples":
        servico = st.selectbox("Serviço", servicos_existentes)
        valor = st.number_input("Valor", value=0.0, step=1.0, format="%.2f")

        # Se cartão, pergunta o líquido e band/tipo/parcelas
        if usar_cartao and not is_nao_cartao(conta):
            with st.expander("💳 Pagamento no cartão (informe o LÍQUIDO recebido)", expanded=True):
                c1, c2 = st.columns(2)
                with c1:
                    liquido = st.number_input("Valor recebido (líquido)", value=float(valor or 0.0), step=1.0, format="%.2f")
                    bandeira = st.selectbox("Bandeira", ["", "Visa", "Mastercard", "Elo", "Hipercard", "Amex", "Outros"], index=0)
                with c2:
                    tipo_cartao = st.selectbox("Tipo", ["Débito", "Crédito"], index=1)
                    parcelas = st.number_input("Parcelas (se crédito)", min_value=1, max_value=12, value=1, step=1)
                taxa_v = max(0.0, float(valor or 0.0) - float(liquido or 0.0))
                taxa_pct = (taxa_v / float(valor or 0.0) * 100.0) if (valor or 0.0) > 0 else 0.0
                st.caption(f"Taxa estimada: {_fmt_brl(taxa_v)} ({taxa_pct:.2f}%)")
        else:
            liquido, bandeira, tipo_cartao, parcelas = None, "", "Crédito", 1

        if st.button("📁 Salvar Atendimento"):
            # monta a linha para a base
            df_all, _aba = carregar_base()
            if usar_cartao and not is_nao_cartao(conta):
                id_pag = gerar_pag_id("A")
                bruto = float(valor or 0.0)
                liq = float(liquido or 0.0)
                taxa_v = round(max(0.0, bruto - liq), 2)
                taxa_pct = round((taxa_v / bruto * 100.0), 4) if bruto > 0 else 0.0
                nova = {
                    "Data": data, "Serviço": _cap_first(servico), "Valor": liq, "Conta": conta,
                    "Cliente": cliente, "Combo": "", "Funcionário": funcionario,
                    "Fase": fase, "Tipo": tipo, "Período": periodo_opcao,
                    "ValorBrutoRecebido": bruto, "ValorLiquidoRecebido": liq,
                    "TaxaCartaoValor": taxa_v, "TaxaCartaoPct": taxa_pct,
                    "FormaPagDetalhe": f"{bandeira or '-'} | {tipo_cartao} | {int(parcelas)}x",
                    "PagamentoID": id_pag
                }
            else:
                nova = {
                    "Data": data, "Serviço": _cap_first(servico), "Valor": float(valor or 0.0), "Conta": conta,
                    "Cliente": cliente, "Combo": "", "Funcionário": funcionario,
                    "Fase": fase, "Tipo": tipo, "Período": periodo_opcao
                }
            # completa colunas faltantes
            for c in [*COLS_FIADO, *COLS_PAG_EXTRAS]:
                nova.setdefault(c, "")

            df_final = pd.concat([df_all, pd.DataFrame([nova])], ignore_index=True)
            salvar_base(df_final)

            enviar_card(df_final, cliente, funcionario, data,
                        servico=_cap_first(servico),
                        valor=float(nova["Valor"]),
                        combo="",
                        pct_daniela=pct_daniela,
                        conta_pag=conta)
            st.success("✅ Atendimento salvo e card enviado.")

    else:
        combo = st.selectbox("Combo (ex: corte+escova)", [""] + combos_existentes)
        total_combo = st.number_input("Total do Combo", value=0.0, step=1.0, format="%.2f")

        # cartão (líquido total)
        if usar_cartao and not is_nao_cartao(conta):
            with st.expander("💳 Pagamento no cartão (informe o LÍQUIDO total do combo)", expanded=True):
                c1, c2 = st.columns(2)
                with c1:
                    liquido_total = st.number_input("Valor recebido (líquido)", value=float(total_combo or 0.0), step=1.0, format="%.2f")
                    bandeira_c = st.selectbox("Bandeira", ["", "Visa", "Mastercard", "Elo", "Hipercard", "Amex", "Outros"], index=0)
                with c2:
                    tipo_cartao_c = st.selectbox("Tipo", ["Débito", "Crédito"], index=1)
                    parcelas_c = st.number_input("Parcelas (se crédito)", min_value=1, max_value=12, value=1, step=1)
                taxa_v_c = max(0.0, float(total_combo or 0.0) - float(liquido_total or 0.0))
                taxa_pct_c = (taxa_v_c / float(total_combo or 0.0) * 100.0) if (total_combo or 0.0) > 0 else 0.0
                st.caption(f"Taxa estimada: {_fmt_brl(taxa_v_c)} ({taxa_pct_c:.2f}%)")
        else:
            liquido_total, bandeira_c, tipo_cartao_c, parcelas_c = None, "", "Crédito", 1

        if st.button("✅ Confirmar e Salvar Combo"):
            if not combo.strip():
                st.warning("Informe o combo."); st.stop()

            df_all, _aba = carregar_base()
            id_pag = gerar_pag_id("A") if (usar_cartao and not is_nao_cartao(conta)) else ""
            linhas = []

            # divide proporcionalmente (para manter simples)
            itens = [s.strip() for s in combo.split("+") if s.strip()]
            if not itens:
                st.warning("Combo sem itens."); st.stop()

            pesos = []
            # se já existirem preços base na memória (VALORES), você pode substituir aqui
            # por enquanto, usa divisão igualitária
            for _ in itens:
                pesos.append(1.0)
            soma_pesos = sum(pesos) or 1.0

            for i, sraw in enumerate(itens):
                s_norm = _cap_first(sraw)
                parte_bruta = float(total_combo or 0.0) * (pesos[i] / soma_pesos)

                if usar_cartao and not is_nao_cartao(conta):
                    liq = round(float(liquido_total or 0.0) * (pesos[i] / soma_pesos), 2)
                    taxa_v = round(parte_bruta - liq, 2)
                    taxa_pct = round((taxa_v / parte_bruta * 100.0), 4) if parte_bruta > 0 else 0.0
                    valor_base = liq
                    extras = {
                        "ValorBrutoRecebido": parte_bruta,
                        "ValorLiquidoRecebido": liq,
                        "TaxaCartaoValor": taxa_v,
                        "TaxaCartaoPct": taxa_pct,
                        "FormaPagDetalhe": f"{bandeira_c or '-'} | {tipo_cartao_c} | {int(parcelas_c)}x",
                        "PagamentoID": id_pag
                    }
                else:
                    valor_base = parte_bruta
                    extras = {}

                linha = {
                    "Data": data, "Serviço": s_norm, "Valor": valor_base, "Conta": conta,
                    "Cliente": cliente, "Combo": combo, "Funcionário": funcionario,
                    "Fase": "Dono + funcionário", "Tipo": "Serviço", "Período": periodo_opcao
                }
                for c in [*COLS_FIADO, *COLS_PAG_EXTRAS]:
                    linha.setdefault(c, "")
                linha.update(extras)
                linhas.append(linha)

            # Ajuste de arredondamento para bater o líquido total (quando cartão)
            if usar_cartao and not is_nao_cartao(conta) and linhas:
                soma_liq = sum(float(l["Valor"]) for l in linhas)
                delta = round(float(liquido_total or 0.0) - soma_liq, 2)
                if abs(delta) >= 0.01:
                    linhas[-1]["Valor"] = float(linhas[-1]["Valor"]) + delta
                    # reatualiza TaxaCartao nos campos da última linha
                    bsel = float(linhas[-1].get("ValorBrutoRecebido", 0) or 0)
                    lsel = float(linhas[-1]["Valor"])
                    tsel = round(bsel - lsel, 2)
                    psel = (tsel / bsel * 100.0) if bsel > 0 else 0.0
                    linhas[-1]["ValorLiquidoRecebido"] = lsel
                    linhas[-1]["TaxaCartaoValor"] = tsel
                    linhas[-1]["TaxaCartaoPct"] = round(psel, 4)

            df_final = pd.concat([df_all, pd.DataFrame(linhas)], ignore_index=True)
            salvar_base(df_final)

            enviar_card(df_final, cliente, funcionario, data,
                        servico=combo.replace("+"," + "), valor=sum(float(l["Valor"]) for l in linhas),
                        combo=combo, pct_daniela=pct_daniela, conta_pag=conta)
            st.success("✅ Combo salvo e card enviado.")

else:
    st.info("Modo Lote disponível — se quiser, adapto para também mostrar foto em cada bloco de cliente 😉")
