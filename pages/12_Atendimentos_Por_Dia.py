# -*- coding: utf-8 -*-
# 15_Atendimentos_Feminino_Por_Dia.py
# KPIs do dia (Feminino), por funcionária, conferência (gravar/excluir no Sheets)
# e EXPORTAR PARA MOBILLS (tudo ou só NÃO conferidos) + pós-exportação marcar conferidos.

import streamlit as st
import pandas as pd
import gspread
import io, textwrap, re
import plotly.express as px
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe
from gspread.utils import rowcol_to_a1
from datetime import datetime, date
import pytz
import numpy as np

# =========================
# CONFIG
# =========================
SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"
ABA_DADOS = "Base de Dados Feminino"   # Feminino
TZ = "America/Sao_Paulo"
DATA_FMT = "%d/%m/%Y"
FUNCIONARIAS = ["Meire", "Daniela"]    # ajuste aqui se necessário
DATA_CORRETA = datetime(2025, 5, 11).date()  # regra de Clientes por Cliente+Data a partir desta data

# =========================
# UTILS
# =========================
def _tz_now():
    return datetime.now(pytz.timezone(TZ))

def _fmt_data(d):
    if pd.isna(d): return ""
    if isinstance(d, (pd.Timestamp, datetime)): return d.strftime(DATA_FMT)
    if isinstance(d, date): return d.strftime(DATA_FMT)
    d2 = pd.to_datetime(str(d), dayfirst=True, errors="coerce")
    return "" if pd.isna(d2) else d2.strftime(DATA_FMT)

def _norm_col(name: str) -> str:
    return re.sub(r"[\s\W_]+", "", str(name).strip().lower())

def _to_bool(x):
    if isinstance(x, (bool, np.bool_)): return bool(x)
    if isinstance(x, (int, float)) and not pd.isna(x): return float(x) != 0.0
    s = str(x).strip().lower()
    return s in ("1", "true", "verdadeiro", "sim", "ok", "y", "yes")

@st.cache_resource(show_spinner=False)
def _conectar_sheets():
    info = st.secrets["GCP_SERVICE_ACCOUNT"]
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)

# ---------- helpers Sheets ----------
def _headers_and_indices(ws):
    headers = ws.row_values(1)
    norms = [_norm_col(h) for h in headers]
    idxs = [i for i, n in enumerate(norms) if n == "conferido"]  # 0-based
    chosen = idxs[-1] if idxs else None  # SEMPRE a última
    return headers, norms, idxs, chosen

def _ensure_conferido_column(ws):
    """Garante coluna 'Conferido' e retorna índice 1-based da ÚLTIMA ocorrência."""
    headers, norms, idxs, chosen = _headers_and_indices(ws)
    if chosen is not None:
        return chosen + 1  # 1-based
    col = len(headers) + 1
    ws.update_cell(1, col, "Conferido")
    return col

def _update_conferido(ws, updates):
    """Atualiza 1 a 1 para garantir persistência na mesma coluna."""
    if not updates: return
    col_conf = _ensure_conferido_column(ws)
    for u in updates:
        row = int(u["row"])
        val = "TRUE" if u["value"] else "FALSE"
        ws.update_cell(row, col_conf, val)

def _delete_rows(ws, rows):
    for r in sorted(set(rows), reverse=True):
        try:
            ws.delete_rows(int(r))
        except Exception as e:
            st.warning(f"Falha ao excluir linha {r}: {e}")

def _fetch_conferido_map(ws):
    """Lê a ÚLTIMA coluna 'Conferido' e devolve {SheetRow:int -> bool}."""
    col_conf = _ensure_conferido_column(ws)
    a1 = rowcol_to_a1(1, col_conf)  # e.g. 'W1' ou 'AA1'
    col_letters = "".join(ch for ch in a1 if ch.isalpha())
    rng = f"{col_letters}2:{col_letters}"
    vals = ws.get(rng, value_render_option="UNFORMATTED_VALUE")

    m = {}
    rownum = 2
    for row in vals:
        v = row[0] if row else ""
        if isinstance(v, (bool, np.bool_)):
            b = bool(v)
        elif isinstance(v, (int, float)) and not pd.isna(v):
            b = float(v) != 0.0
        else:
            s = str(v).strip().lower()
            b = s in ("1", "true", "verdadeiro", "sim", "ok", "y", "yes")
        m[rownum] = b
        rownum += 1
    return m

# ---------- leitura base ----------
@st.cache_data(ttl=60, show_spinner=False)
def carregar_base():
    gc = _conectar_sheets()
    sh = gc.open_by_key(SHEET_ID)
    ws = sh.worksheet(ABA_DADOS)

    df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
    df = df.dropna(how="all")
    if df is None or df.empty:
        return pd.DataFrame()

    df["SheetRow"] = df.index + 2
    df.columns = [str(c).strip() for c in df.columns]

    base_cols = ["Data", "Serviço", "Valor", "Conta", "Cliente", "Combo",
                 "Funcionário", "Fase", "Hora Chegada", "Hora Início",
                 "Hora Saída", "Hora Saída do Salão", "Tipo"]
    for c in base_cols:
        if c not in df.columns:
            df[c] = None

    # strings padronizadas
    for col in ["Cliente", "Serviço", "Funcionário", "Conta", "Combo", "Tipo", "Fase"]:
        if col not in df.columns: df[col] = ""
        df[col] = df[col].astype(str).fillna("").str.strip()

    # datas
    def parse_data(x):
        if pd.isna(x): return None
        if isinstance(x, (datetime, pd.Timestamp)): return x.date()
        s = str(x).strip()
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%y"):
            try:
                return datetime.strptime(s, fmt).date()
            except Exception:
                pass
        return None
    df["Data_norm"] = df["Data"].apply(parse_data)

    # valores
    def parse_valor(v):
        if pd.isna(v): return 0.0
        s = str(v).strip().replace("R$", "").replace(" ", "")
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", ".")
        try:
            return float(s)
        except Exception:
            return 0.0
    df["Valor_num"] = df["Valor"].apply(parse_valor)

    # 'Conferido' direto da coluna correta do Sheets
    conferido_map = _fetch_conferido_map(ws)
    df["Conferido"] = df["SheetRow"].map(lambda r: bool(conferido_map.get(int(r), False))).astype(bool)

    # debug
    headers = ws.row_values(1)
    conf_sources = [h for h in headers if _norm_col(h) == "conferido"]
    df.attrs["__conferido_sources__"] = conf_sources or []

    return df

# ---------- agregações ----------
def filtrar_por_dia(df, dia):
    if df.empty or dia is None: return df.iloc[0:0]
    return df[df["Data_norm"] == dia].copy()

def contar_atendimentos_dia(df):
    if df.empty: return 0
    d0 = df["Data_norm"].dropna()
    if d0.empty: return 0
    dia = d0.iloc[0]
    if dia < DATA_CORRETA:
        return len(df)  # antes da regra, cada linha = 1 atendimento
    # depois da regra, 1 por Cliente+Data
    return df.groupby(["Cliente", "Data_norm"]).ngroups

def kpis(df):
    if df.empty: return 0, 0, 0.0, 0.0
    clientes = contar_atendimentos_dia(df)
    servicos = len(df)
    receita = float(df["Valor_num"].sum())
    ticket = (receita / clientes) if clientes > 0 else 0.0
    return clientes, servicos, receita, ticket

def format_moeda(v):
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def preparar_tabela_exibicao(df):
    cols_ordem = [
        "Data", "Cliente", "Serviço", "Valor", "Conta", "Funcionário",
        "Combo", "Tipo", "Hora Chegada", "Hora Início", "Hora Saída", "Hora Saída do Salão"
    ]
    for c in cols_ordem:
        if c not in df.columns:
            df[c] = ""
    out = df.copy()
    out["Data"] = out["Data_norm"].apply(_fmt_data)
    out["Valor"] = out["Valor_num"].apply(format_moeda)
    return out[cols_ordem]

# ---------- Excel helpers ----------
def _choose_excel_engine():
    import importlib.util
    for eng in ("xlsxwriter", "openpyxl"):
        if importlib.util.find_spec(eng) is not None:
            return eng
    return None

def _to_xlsx_bytes(dfs_by_sheet: dict):
    engine = _choose_excel_engine()
    if not engine:
        return None
    with io.BytesIO() as buf:
        with pd.ExcelWriter(buf, engine=engine) as writer:
            for sheet, df in dfs_by_sheet.items():
                df.to_excel(writer, sheet_name=sheet, index=False)
        return buf.getvalue()

# ============= HELPER HTML (render seguro) =============
def html(s: str):
    st.markdown(textwrap.dedent(s), unsafe_allow_html=True)

def card(label, val):
    return f'<div class="card"><div class="label">{label}</div><div class="value">{val}</div></div>'

# =========================
# UI
# =========================
st.set_page_config(page_title="Atendimentos por Dia (Feminino)", page_icon="📅", layout="wide")
st.title("📅 Atendimentos por Dia — Feminino")
st.caption("KPIs do dia, comparativo por funcionária, conferência e exportação para Mobills.")

# ===== Sidebar =====
if st.sidebar.button("🔄 Recarregar dados agora"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.markdown("### Percentual de comissão por funcionária")
comissoes_pct = {}
for func in FUNCIONARIAS:
    comissoes_pct[func] = st.sidebar.number_input(
        f"% comissão de {func}", min_value=0.0, max_value=100.0, value=0.0, step=1.0,
        help="Use 0 se não aplica comissão para esta funcionária."
    )

with st.spinner("Carregando base..."):
    df_base = carregar_base()

# Seletor de dia
hoje = _tz_now().date()
dia_selecionado = st.date_input("Dia", value=hoje, format="DD/MM/YYYY")
df_dia = filtrar_por_dia(df_base, dia_selecionado)
if df_dia.empty:
    st.info("Nenhum atendimento encontrado para o dia selecionado.")
    st.stop()

# Debug rápido
st.sidebar.caption("Colunas 'Conferido' no cabeçalho: " + ", ".join(df_base.attrs.get("__conferido_sources__", ["<nenhuma>"])))
st.sidebar.caption(f"Conferidos no dia: {int(df_dia['Conferido'].fillna(False).sum())}")

# ====== KPIs ======
html("""
<style>
.metrics-wrap{display:flex;flex-wrap:wrap;gap:12px;margin:8px 0}
.metrics-wrap .card{
  background:rgba(255,255,255,0.04);
  border:1px solid rgba(255,255,255,0.08);
  border-radius:12px;
  padding:12px 14px;
  min-width:160px;
  flex:1 1 200px;
}
.metrics-wrap .card .label{font-size:0.9rem;opacity:.85;margin-bottom:6px}
.metrics-wrap .card .value{font-weight:700;font-size:clamp(18px,3.8vw,28px);line-height:1.15}
.section-h{font-weight:700;margin:12px 0 6px}
</style>
""")

# KPIs gerais
cli, srv, rec, tkt = kpis(df_dia)

# Total de comissões do dia (de acordo com os percentuais definidos)
total_comissoes = 0.0
for func in FUNCIONARIAS:
    df_f = df_dia[df_dia["Funcionário"].astype(str).str.casefold() == func.casefold()]
    _, _, rec_f, _ = kpis(df_f)
    total_comissoes += rec_f * (comissoes_pct.get(func, 0.0) / 100.0)

receita_salao_pos = rec - total_comissoes

html(
    '<div class="metrics-wrap">'
    + card("👥 Clientes atendidos", f"{cli}")
    + card("✂️ Serviços realizados", f"{srv}")
    + card("🧾 Ticket médio", format_moeda(tkt))
    + card("💰 Receita do dia (bruta)", format_moeda(rec))
    + card("🏢 Receita do salão (pós-comissões)", format_moeda(receita_salao_pos))
    + "</div>"
)
st.markdown("---")

# ===== Por Funcionária =====
st.subheader("📊 Por Funcionária (dia selecionado)")
cols = st.columns(len(FUNCIONARIAS))
for i, func in enumerate(FUNCIONARIAS):
    df_f = df_dia[df_dia["Funcionário"].astype(str).str.casefold() == func.casefold()]
    cli_f, srv_f, rec_f, tkt_f = kpis(df_f)
    com_pct = comissoes_pct.get(func, 0.0)
    com_val = rec_f * (com_pct / 100.0)
    with cols[i]:
        html(f'<div class="section-h">{func}</div>')
        html('<div class="metrics-wrap">' +
             card("Clientes", f"{cli_f}") +
             card("Serviços", f"{srv_f}") +
             card("🧾 Ticket médio", format_moeda(tkt_f)) +
             card("Receita", format_moeda(rec_f)) +
             card(f"💵 Comissão ({com_pct:.0f}%)", format_moeda(com_val)) +
             '</div>')

# ===== Gráfico =====
df_comp = []
for func in FUNCIONARIAS:
    df_f = df_dia[df_dia["Funcionário"].astype(str).str.casefold() == func.casefold()]
    cli_f, srv_f, _, _ = kpis(df_f)
    df_comp.append({"Funcionária": func, "Clientes": cli_f, "Serviços": srv_f})
df_comp = pd.DataFrame(df_comp)

fig = px.bar(
    df_comp.melt(id_vars="Funcionária", var_name="Métrica", value_name="Quantidade"),
    x="Funcionária", y="Quantidade", color="Métrica", barmode="group",
    title=f"Comparativo de atendimentos — {dia_selecionado.strftime('%d/%m/%Y')}"
)
st.plotly_chart(fig, use_container_width=True)

# ========================================================
# 🔎 MODO DE CONFERÊNCIA
# ========================================================
st.markdown("---")
st.subheader("🧾 Conferência do dia (marcar conferido e excluir)")

df_conf = df_dia.copy()
df_conf["Conferido"] = df_conf["Conferido"].apply(_to_bool).astype(bool)

df_conf_view = df_conf[[
    "SheetRow", "Cliente", "Serviço", "Funcionário", "Valor", "Conta", "Conferido"
]].copy()
df_conf_view["Excluir"] = False

st.caption("Edite **Conferido** e/ou marque **Excluir**. Depois clique em **Aplicar mudanças**.")
edited = st.data_editor(
    df_conf_view,
    use_container_width=True,
    hide_index=True,
    column_config={
        "SheetRow": st.column_config.NumberColumn("SheetRow", help="Nº real no Sheets", disabled=True),
        "Cliente": st.column_config.TextColumn("Cliente", disabled=True),
        "Serviço": st.column_config.TextColumn("Serviço", disabled=True),
        "Funcionário": st.column_config.TextColumn("Funcionário", disabled=True),
        "Valor": st.column_config.TextColumn("Valor", disabled=True),
        "Conta": st.column_config.TextColumn("Conta", disabled=True),
        "Conferido": st.column_config.CheckboxColumn("Conferido"),
        "Excluir": st.column_config.CheckboxColumn("Excluir"),
    },
    key="editor_conferencia_fem"
)

if st.button("✅ Aplicar mudanças (gravar no Sheets)", type="primary"):
    try:
        gc = _conectar_sheets()
        sh = gc.open_by_key(SHEET_ID)
        ws = sh.worksheet(ABA_DADOS)

        # Atualiza 'Conferido'
        orig_by_row = df_conf.set_index("SheetRow")["Conferido"].apply(_to_bool).to_dict()
        updates = []
        for _, r in edited.iterrows():
            rownum = int(r["SheetRow"])
            new_val = bool(_to_bool(r["Conferido"]))
            old_val = bool(_to_bool(orig_by_row.get(rownum, False)))
            if new_val != old_val:
                updates.append({"row": rownum, "value": new_val})
        _update_conferido(ws, updates)

        # Exclui marcados
        rows_to_delete = [int(r["SheetRow"]) for _, r in edited.iterrows() if bool(_to_bool(r["Excluir"]))]
        _delete_rows(ws, rows_to_delete)

        st.success("Alterações aplicadas com sucesso!")
        st.cache_data.clear()
        st.rerun()
    except Exception as e:
        st.error(f"Falha ao aplicar mudanças: {e}")

# ========================================================
# 📤 EXPORTAR PARA MOBILLS
# ========================================================
st.markdown("---")
st.subheader("📤 Exportar para Mobills")

export_only_unchecked = st.checkbox(
    "Exportar **apenas os NÃO conferidos**",
    value=True,
    help="Desmarque para exportar TODOS os registros do dia."
)

df_export_base = df_dia.copy()
df_export_base["Conferido"] = df_export_base["Conferido"].apply(_to_bool).astype(bool)
if export_only_unchecked:
    df_export_base = df_export_base[~df_export_base["Conferido"].fillna(False)]

st.caption(
    f"Selecionados para exportação: **{len(df_export_base)}** de **{len(df_dia)}** registros."
)

# ===== Resumo por Cliente =====
st.markdown("### Resumo por Cliente (dia selecionado)")
grp_dia = (
    df_dia
    .groupby("Cliente", as_index=False)
    .agg(Qtd_Serviços=("Serviço", "count"),
         Valor_Total=("Valor_num", "sum"))
    .sort_values(["Valor_Total", "Qtd_Serviços"], ascending=[False, False])
)
grp_dia["Valor_Total"] = grp_dia["Valor_Total"].apply(format_moeda)
st.dataframe(
    grp_dia.rename(columns={"Qtd_Serviços": "Qtd. Serviços", "Valor_Total": "Valor Total"}),
    use_container_width=True,
    hide_index=True
)

st.markdown("### Resumo por Cliente (seleção para exportação)")
if df_export_base.empty:
    st.info("Nada na seleção atual (verifique o filtro de NÃO conferidos).")
else:
    grp_sel = (
        df_export_base
        .groupby("Cliente", as_index=False)
        .agg(Qtd_Serviços=("Serviço", "count"),
             Valor_Total=("Valor_num", "sum"))
        .sort_values(["Valor_Total", "Qtd_Serviços"], ascending=[False, False])
    )
    grp_sel["Valor_Total"] = grp_sel["Valor_Total"].apply(format_moeda)
    st.dataframe(
        grp_sel.rename(columns={"Qtd_Serviços": "Qtd. Serviços", "Valor_Total": "Valor Total"}),
        use_container_width=True,
        hide_index=True
    )

conta_fallback = st.text_input("Conta padrão (quando vazio na base)", value="Nubank CNPJ")

def _fmt_data_ddmmyyyy(d):
    return d.strftime("%d/%m/%Y") if pd.notna(d) else ""

def _descricao(row):
    # Pode personalizar: usar o nome da funcionária como descrição
    func = str(row.get("Funcionário", "")).strip() or "Funcionária"
    return func

def _categoria(row):
    serv = (str(row.get("Serviço", "")).strip() or "Serviço")
    func = str(row.get("Funcionário", "")).strip()
    return f"Lucro {func} > {serv}" if func else f"Lucro Feminino > {serv}"

if df_export_base.empty:
    st.info("Nada a exportar (com o filtro atual).")
else:
    df_mob = df_export_base.copy()
    df_mob["Data"] = df_mob["Data_norm"].apply(_fmt_data_ddmmyyyy)
    df_mob["Descrição"] = df_mob.apply(_descricao, axis=1)
    df_mob["Valor"] = pd.to_numeric(df_mob["Valor_num"], errors="coerce").fillna(0.0)

    df_mob["Conta"] = df_mob["Conta"].fillna("").astype(str).str.strip()
    df_mob.loc[df_mob["Conta"] == "", "Conta"] = conta_fallback

    df_mob["Categoria"] = df_mob.apply(_categoria, axis=1)
    df_mob["serviço"] = df_mob["Serviço"].astype(str).fillna("").str.strip()
    df_mob["cliente"] = df_mob["Cliente"].astype(str).fillna("").str.strip()
    df_mob["Combo"]   = df_mob.get("Combo", "").astype(str).fillna("").str.strip()

    cols_final = ["Data", "Descrição", "Valor", "Conta", "Categoria", "serviço", "cliente", "Combo"]
    df_mobills = df_mob[cols_final].copy()

    st.markdown("**Prévia (Mobills)**")
    st.dataframe(df_mobills, use_container_width=True, hide_index=True)

    # CSV (Mobills usa ';')
    csv_bytes = df_mobills.to_csv(index=False, sep=";", encoding="utf-8-sig").encode("utf-8-sig")
    st.download_button(
        "⬇️ Baixar CSV (Mobills)",
        data=csv_bytes,
        file_name=f"Mobills_{dia_selecionado.strftime('%d-%m-%Y')}.csv",
        mime="text/csv",
        type="primary"
    )

    # XLSX – aba 'Mobills'
    xlsx_bytes = _to_xlsx_bytes({"Mobills": df_mobills})
    if xlsx_bytes:
        st.download_button(
            "⬇️ Baixar XLSX (Mobills)",
            data=xlsx_bytes,
            file_name=f"Mobills_{dia_selecionado.strftime('%d-%m-%Y')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.warning("Para gerar Excel, instale 'xlsxwriter' ou 'openpyxl' no ambiente.")

    # Pós-exportação: marcar como conferidos
    st.markdown("#### Pós-exportação")
    if st.button("✅ Marcar exportados como Conferidos no Sheets"):
        try:
            gc = _conectar_sheets()
            sh = gc.open_by_key(SHEET_ID)
            ws = sh.worksheet(ABA_DADOS)
            updates = [{"row": int(r), "value": True} for r in df_export_base["SheetRow"].tolist()]
            _update_conferido(ws, updates)
            st.success(f"Marcados {len(updates)} registros como Conferidos.")
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            st.error(f"Falha ao marcar como conferidos: {e}")
