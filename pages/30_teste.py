# -*- coding: utf-8 -*-
# 14_Agendamento.py — Agenda Feminino com foto, combos editáveis e cards no Telegram

import streamlit as st
import pandas as pd
import gspread, json, os, pytz, unicodedata, requests, random, string, re
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from gspread.utils import rowcol_to_a1
from datetime import datetime, date, time as dt_time

# =========================
# CONFIG
# =========================
SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"
ABA_DADOS_FEM = "Base de Dados Feminino"
ABA_STATUS_FEM = "clientes_status_feminino"   # se não existir, o código ignora
ABA_AGENDAMENTO = "Agendamento"

TZ = "America/Sao_Paulo"
DATA_FMT = "%d/%m/%Y"; HORA_FMT = "%H:%M:%S"

PHOTO_FALLBACK_URL = "https://res.cloudinary.com/db8ipmete/image/upload/v1752463905/Logo_sal%C3%A3o_kz9y9c.png"
FOTO_COL_CANDIDATES = ["link_foto","foto","imagem","url_foto","foto_link","link","image","foto_url"]

# Telegram
TELEGRAM_TOKEN = st.secrets.get("TELEGRAM_TOKEN", "8257359388:AAGayJElTPT0pQadtamVf8LoL7R6EfWzFGE")
CHAT_ID_JPAULO = st.secrets.get("TELEGRAM_CHAT_ID_JPAULO", "493747253")
CHAT_ID_FEMININO = st.secrets.get("TELEGRAM_CHAT_ID_FEMININO", "-1002965378062")

FUNCIONARIOS_FEM = ["Meire","Daniela"]
FUNCIONARIO_PADRAO = "Meire"

# =========================
# Utils
# =========================
def tz_now():
    return datetime.now(pytz.timezone(TZ))

def norm(s):
    if not isinstance(s, str): return ""
    s = "".join(ch for ch in unicodedata.normalize("NFKD", s.strip().lower()) if not unicodedata.combining(ch))
    return s

def periodo_por_hora(hh):
    return "Manhã" if 5 <= hh < 12 else ("Tarde" if 12 <= hh < 18 else "Noite")

def novo_id(prefix="AG"):
    return f"{prefix}-{tz_now().strftime('%Y%m%d%H%M%S')}-{''.join(random.choices(string.ascii_uppercase+string.digits,k=4))}"

def send_tg_msg(text):
    try:
        url=f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        for chat_id in (CHAT_ID_FEMININO, CHAT_ID_JPAULO):
            requests.post(url, json={"chat_id":chat_id,"text":text,"parse_mode":"HTML","disable_web_page_preview":True}, timeout=10)
    except Exception as e:
        st.warning(f"Falha Telegram: {e}")

# ----------- FOTO: Normalização + Verificação + Envio robusto -----------
def normalize_photo_url(u: str) -> str:
    if not isinstance(u, str) or not u:
        return PHOTO_FALLBACK_URL
    u = u.strip()
    m = re.search(r"drive\.google\.com/file/d/([^/]+)/", u)
    if m:
        file_id = m.group(1)
        return f"https://drive.google.com/uc?export=view&id={file_id}"
    m = re.search(r"drive\.google\.com/(?:open|uc)\?[^#]*id=([^&]+)", u)
    if m:
        file_id = m.group(1)
        return f"https://drive.google.com/uc?export=view&id={file_id}"
    return u

def check_url_ok(url: str) -> bool:
    try:
        r = requests.head(url, timeout=6, allow_redirects=True)
        if r.status_code == 405:
            r = requests.get(url, timeout=8, stream=True)
        return r.status_code == 200
    except Exception:
        return False

def _telegram_photo(chat_id: str, photo_url: str, caption: str):
    send_photo_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    send_text_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        r = requests.post(send_photo_url, json={"chat_id": chat_id, "photo": photo_url, "caption": caption, "parse_mode": "HTML"}, timeout=12)
        if r.status_code == 200:
            return
    except Exception:
        pass
    try:
        if check_url_ok(photo_url):
            img = requests.get(photo_url, timeout=10).content
            files = {"photo": ("foto.jpg", img, "image/jpeg")}
            data = {"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"}
            r2 = requests.post(send_photo_url, data=data, files=files, timeout=15)
            if r2.status_code == 200:
                return
    except Exception:
        pass
    try:
        requests.post(send_text_url, json={"chat_id": chat_id, "text": caption, "parse_mode": "HTML"}, timeout=10)
    except Exception:
        pass

def send_tg_photo(photo_url, caption):
    for chat_id in (CHAT_ID_FEMININO, CHAT_ID_JPAULO):
        _telegram_photo(chat_id, photo_url, caption)

def card_confirmacao(c, s, v, conta, f, d, h, obs, ida, fiado=False, venc=""):
    val = "-" if v in ("", None) else f"R$ {float(v):.2f}".replace(".", ",")
    extra = f"\n🧾 <b>Condição:</b> {'Fiado (venc. '+venc+')' if fiado else 'Pago'}"
    return ("✅ <b>Atendimento confirmado</b>\n"
            f"👤 <b>Cliente:</b> {c}\n"
            f"🧴 <b>Serviço:</b> {s}\n"
            f"💳 <b>Conta:</b> {conta}\n"
            f"💲 <b>Valor:</b> {val}\n"
            f"🧑‍💼 <b>Funcionário:</b> {f}\n"
            f"🗓️ <b>Data/Hora:</b> {d} {h}\n"
            f"📝 <b>Obs.:</b> {obs or '-'}\n"
            f"🏷️ <b>ID:</b> {ida}" + extra)

# =========================
# Google Sheets (com diagnóstico de permissão/ID)
# =========================
SERVICE_EMAIL = None  # preenchido em conectar_sheets()

@st.cache_resource(show_spinner=False)
def conectar_sheets():
    global SERVICE_EMAIL
    cand = (st.secrets.get("gcp_service_account") or st.secrets.get("gcp_service_account_feminino")
            or st.secrets.get("google_credentials") or st.secrets.get("GCP_SERVICE_ACCOUNT")
            or os.environ.get("GCP_SERVICE_ACCOUNT"))
    if cand is None:
        raise KeyError("Credenciais ausentes. Adicione em secrets uma das chaves: gcp_service_account / gcp_service_account_feminino / google_credentials / GCP_SERVICE_ACCOUNT")
    if isinstance(cand, str):
        cand = json.loads(cand)
    SERVICE_EMAIL = cand.get("client_email", "—")
    creds = Credentials.from_service_account_info(cand, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ])
    return gspread.authorize(creds)

# Abre a planilha com tratamento de erro
try:
    gc = conectar_sheets()
    sh = gc.open_by_key(SHEET_ID)
except gspread.exceptions.APIError as e:
    st.error(
        "❌ Não consegui abrir a planilha pelo ID.\n\n"
        "Verifique:\n"
        f"• Compartilhe a planilha com este e-mail (Editor): **{SERVICE_EMAIL}**\n"
        f"• Confirme se o `SHEET_ID` está correto: `{SHEET_ID}`\n"
        "• O arquivo não foi excluído/movido."
    )
    st.stop()
except Exception as e:
    st.error(f"Falha ao conectar no Google Sheets: {e}")
    st.stop()

def abrir_ws(nome):
    try:
        return sh.worksheet(nome)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=nome, rows=3000, cols=60)

COLS_AGENDA = [
    "IDAgenda","Data","Hora","Cliente","Serviço","Valor","Conta","Funcionário",
    "Combo","Observação","Status","Criado_em","Atendido_em","ItensComboJSON"
]

def garantir_estrutura_agenda():
    ws = abrir_ws(ABA_AGENDAMENTO)
    df = get_as_dataframe(ws, header=0)
    if df.empty or any(c not in df.columns for c in COLS_AGENDA):
        ws.clear(); ws.update(rowcol_to_a1(1,1), [COLS_AGENDA])
garantir_estrutura_agenda()

def garantir_estrutura_status_fem():
    ws = abrir_ws(ABA_STATUS_FEM)
    df = get_as_dataframe(ws, header=0, evaluate_formulas=False).dropna(how="all")
    base = ["Cliente","Status","Foto","Observação"]
    if df.empty:
        ws.clear(); ws.update(rowcol_to_a1(1,1), [base]); return
    changed = False
    for c in base:
        if c not in df.columns:
            df[c] = ""
            changed = True
    if changed:
        outros = [c for c in df.columns if c not in base]
        set_with_dataframe(ws, df[base + outros], include_index=False, include_column_header=True, resize=True)

def carregar_df(aba):
    ws = abrir_ws(aba)
    df = get_as_dataframe(ws, header=0, evaluate_formulas=False).dropna(how="all")
    if aba == ABA_AGENDAMENTO and not df.empty:
        for c in COLS_AGENDA:
            if c not in df.columns: df[c] = ""
        df = df[COLS_AGENDA]
    return df

def salvar_df(aba, df):
    ws = abrir_ws(aba)
    if df.empty:
        ws.clear(); ws.update(rowcol_to_a1(1,1), [list(df.columns)])
        return
    ws.clear()
    set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)

# -------------------------
# Auxiliares (Clientes, Serviços, Combos, Foto)
# -------------------------
@st.cache_data(show_spinner=False)
def clientes_existentes():
    nomes = set()
    try:
        df = carregar_df(ABA_DADOS_FEM)
        if "Cliente" in df.columns:
            for x in df["Cliente"].dropna().astype(str): nomes.add(x.strip())
    except: pass
    try:
        df2 = carregar_df(ABA_STATUS_FEM)
        nome_col = None
        for c in df2.columns:
            if norm(c) in ("cliente","nome","nome_cliente"):
                nome_col = c; break
        if nome_col:
            for x in df2[nome_col].dropna().astype(str): nomes.add(x.strip())
    except: pass
    return sorted(nomes, key=lambda s: norm(s))

@st.cache_data(show_spinner=False)
def servicos_e_combos():
    servs, combs = [], []
    try:
        df = carregar_df(ABA_DADOS_FEM)
        if not df.empty:
            if "Serviço" in df.columns:
                servs = [s for s in df["Serviço"].dropna().astype(str) if s.strip()]
            if "Combo" in df.columns:
                combs = [c for c in df["Combo"].dropna().astype(str) if c.strip()]
    except: pass
    servs = [(s[:1].upper() + s[1:]).strip() for s in servs]
    return sorted(set(servs), key=lambda s: norm(s)), sorted(set(combs), key=lambda s: norm(s))

def preco_sugerido(servico):
    try:
        df = carregar_df(ABA_DADOS_FEM)
        if {"Data","Serviço","Valor"}.issubset(df.columns):
            datas = pd.to_datetime(df["Data"], format=DATA_FMT, errors="coerce")
            agosto = (datas.dt.month == 8) & (datas.dt.year == 2025)
            m = df[agosto & (df["Serviço"].astype(str).str.lower() == servico.lower())]
            vals = pd.to_numeric(m["Valor"], errors="coerce").dropna()
            if not vals.empty:
                return round(float(vals.median()), 2)
    except Exception as e:
        print("preco_sugerido erro:", e)
    return None

def foto_do_cliente(cliente: str) -> str:
    def _norm(s: str) -> str:
        if not isinstance(s, str): return ""
        return "".join(ch for ch in unicodedata.normalize("NFKD", s.strip().lower()) if not unicodedata.combining(ch))
    if not cliente:
        return PHOTO_FALLBACK_URL
    try:
        df = carregar_df(ABA_STATUS_FEM)
        if df.empty:
            return PHOTO_FALLBACK_URL
        nome_col = None
        for col in df.columns:
            if _norm(col) in ("cliente","nome","nome_cliente"):
                nome_col = col; break
        if not nome_col:
            return PHOTO_FALLBACK_URL
        foto_col = None
        cand_norm = {_norm(x) for x in FOTO_COL_CANDIDATES}
        for col in df.columns:
            if _norm(col) in cand_norm:
                foto_col = col; break
        if not foto_col:
            return PHOTO_FALLBACK_URL
        df["_k"] = df[nome_col].astype(str).apply(_norm)
        row = df[df["_k"] == _norm(cliente)].head(1)
        if row.empty:
            return PHOTO_FALLBACK_URL
        url = str(row.iloc[0][foto_col]).strip()
        if not url.startswith(("http://","https://")):
            return PHOTO_FALLBACK_URL
        url = normalize_photo_url(url)
        return url or PHOTO_FALLBACK_URL
    except Exception as e:
        print("foto_do_cliente erro:", e)
        return PHOTO_FALLBACK_URL

# =========================
# UI
# =========================
st.title("📅 Agendamento (Feminino)")
acao = st.radio("Ações:", ["➕ Agendar","✅ Confirmar atendimentos","🗂️ Em aberto & exportação"], horizontal=False)

# ---------- 1) AGENDAR ----------
if acao.startswith("➕"):
    st.subheader("Novo agendamento")

    # Data / Hora / Funcionário
    cA, cB, cC = st.columns([1, 1, 2])
    data_ag = cA.date_input("Data", value=date.today())
    hora_ag = cB.time_input("Hora", value=dt_time(9, 0, 0), step=300)
    funcionario = cC.selectbox("Funcionário", options=FUNCIONARIOS_FEM, index=FUNCIONARIOS_FEM.index(FUNCIONARIO_PADRAO))

    # >>> Cadastro rápido de cliente
    garantir_estrutura_status_fem()
    with st.expander("➕ Cadastrar novo cliente"):
        with st.form("cad_cliente_form", clear_on_submit=False):
            nome_novo = st.text_input("Nome do cliente *")
            status_novo = st.selectbox("Status", ["Ativo","Inativo"], index=0)
            foto_nova = st.text_input("Foto (URL) — opcional", placeholder="https://... (Drive/Cloudinary)")
            obs_nova  = st.text_area("Observação (opcional)")
            bt_cad = st.form_submit_button("Salvar cliente")

        if bt_cad:
            if not nome_novo.strip():
                st.error("Informe o nome do cliente.")
            else:
                df_status = carregar_df(ABA_STATUS_FEM)
                if df_status.empty:
                    df_status = pd.DataFrame(columns=["Cliente","Status","Foto","Observação"])
                for c in ["Cliente","Status","Foto","Observação"]:
                    if c not in df_status.columns:
                        df_status[c] = ""
                chave = df_status["Cliente"].astype(str).apply(norm)
                m = chave == norm(nome_novo)
                if m.any():
                    idx = m.idxmax()
                    df_status.loc[idx, "Status"] = status_novo
                    df_status.loc[idx, "Foto"] = foto_nova.strip()
                    df_status.loc[idx, "Observação"] = obs_nova.strip()
                else:
                    df_status = pd.concat([df_status, pd.DataFrame([{
                        "Cliente": nome_novo.strip(),
                        "Status": status_novo,
                        "Foto": foto_nova.strip(),
                        "Observação": obs_nova.strip()
                    }])], ignore_index=True)
                salvar_df(ABA_STATUS_FEM, df_status)
                st.success(f"Cliente '{nome_novo.strip()}' salvo com sucesso!")
                try:
                    clientes_existentes.clear()
                except Exception:
                    pass
                st.session_state["cliente_recem_cadastrado"] = nome_novo.strip()
                _rerun = getattr(st, "rerun", None) or getattr(st, "experimental_rerun", None)
                if callable(_rerun):
                    _rerun()
                else:
                    st.info("Atualize a página para ver o cliente na lista.")

    # Cliente: somente cadastrados (base + status feminino)
    clientes_opts = clientes_existentes()
    if not clientes_opts:
        st.error("Nenhum cliente encontrado. Cadastre clientes em 'clientes_status_feminino' ou na Base.")
        st.stop()
    idx_default = 0
    novo = st.session_state.get("cliente_recem_cadastrado")
    if novo and novo in clientes_opts:
        idx_default = clientes_opts.index(novo)
    cliente_final = st.selectbox("Cliente", clientes_opts, index=idx_default)

    # Serviços / Combos
    _servs, _combs = servicos_e_combos()
    if not _servs:
        st.error("Nenhum serviço encontrado na Base de Dados Feminino.")
        st.stop()
    c1, c2 = st.columns([2, 1])
    servico = c1.selectbox("Serviço", _servs)
    valor_sugerido = preco_sugerido(servico)
    valor_txt = c2.text_input("Valor (R$)", value=("" if valor_sugerido is None else f"{valor_sugerido:.2f}".replace(".", ",")), placeholder="Ex.: 35,00")

    c3, c4 = st.columns([1, 1])
    conta = c3.text_input("Conta / Forma de pagamento", value="Carteira")
    combo = c4.selectbox("Combo (opcional)", [""] + _combs, index=0)

    obs = st.text_area("Observação (opcional)")

    # Itens do combo
    itens_combo = []
    if combo:
        raw = [x.strip() for x in combo.split("+") if x.strip()]
        rows = []
        for s in raw:
            nome = s[:1].upper() + s[1:]
            sug = preco_sugerido(nome)
            rows.append({"Serviço": nome, "Valor (R$)": sug})
        df_edit = pd.DataFrame(rows)
        st.markdown("**Itens do combo (edite os valores antes de agendar):**")
        df_edit = st.data_editor(
            df_edit,
            column_config={"Valor (R$)": st.column_config.NumberColumn("Valor (R$)", step=0.5, format="%.2f")},
            disabled=["Serviço"], key="editor_itens_combo",
            use_container_width=True,
            height=min(420, 120 + 30*len(rows))
        )
        for _, r in df_edit.iterrows():
            v = None if pd.isna(r["Valor (R$)"]) else float(r["Valor (R$)"])
            itens_combo.append({"servico": r["Serviço"], "valor": v})

    # Salvar + Telegram
    if st.button("Agendar e notificar", type="primary", use_container_width=True):
        garantir_estrutura_agenda()
        df_ag = carregar_df(ABA_AGENDAMENTO)

        valor_total = None
        if combo and itens_combo:
            soma = [i["valor"] for i in itens_combo if i["valor"] not in (None, "")]
            if soma:
                valor_total = round(float(sum(soma)), 2)
        if valor_total is None:
            try:
                valor_total = round(float(str(valor_txt).replace(",", ".")), 2)
            except Exception:
                valor_total = ""

        ida = novo_id("AG")
        criado_em = tz_now().strftime(f"{DATA_FMT} {HORA_FMT}")
        linha = {
            "IDAgenda": ida, "Data": data_ag.strftime(DATA_FMT), "Hora": hora_ag.strftime(HORA_FMT),
            "Cliente": cliente_final, "Serviço": servico, "Valor": valor_total, "Conta": conta,
            "Funcionário": funcionario, "Combo": combo, "Observação": obs,
            "Status": "Agendado", "Criado_em": criado_em, "Atendido_em": "",
            "ItensComboJSON": json.dumps(itens_combo, ensure_ascii=False) if itens_combo else ""
        }
        df_ag = pd.concat([df_ag, pd.DataFrame([linha])], ignore_index=True)
        salvar_df(ABA_AGENDAMENTO, df_ag)

        foto_url = foto_do_cliente(cliente_final) or PHOTO_FALLBACK_URL
        det = ""
        if itens_combo:
            linhas = [f"   • {it['servico']}: R$ {0 if (it['valor'] in (None,'')) else it['valor']:.2f}".replace(".", ",")
                      for it in itens_combo]
            det = "\n🧾 <b>Itens:</b>\n" + "\n".join(linhas)

        caption = (
            "📅 <b>Novo agendamento</b>\n"
            f"👤 <b>Cliente:</b> {cliente_final}\n"
            f"🧴 <b>Serviço:</b> {servico or '-'}\n"
            f"💳 <b>Conta:</b> {conta}\n"
            f"💲 <b>Total:</b> {('-' if valor_total in ('', None) else ('R$ ' + str(f'{valor_total:.2f}'.replace('.',','))))}\n"
            f"🧑‍💼 <b>Funcionário:</b> {funcionario}\n"
            f"🗓️ <b>Data/Hora:</b> {linha['Data']} {linha['Hora']}\n"
            f"📝 <b>Obs.:</b> {obs or '-'}\n"
            f"🏷️ <b>ID:</b> {ida}"
            f"{det}"
        )
        send_tg_photo(foto_url, caption)
        if "cliente_recem_cadastrado" in st.session_state:
            del st.session_state["cliente_recem_cadastrado"]
        st.success("Agendado e notificado com sucesso ✅")

# ---------- 2) CONFIRMAR ----------
elif acao.startswith("✅"):
    st.subheader("Confirmar atendimentos (lote) — com fiado e revisão de serviços")
    df_ag = carregar_df(ABA_AGENDAMENTO)
    if df_ag.empty or not (df_ag["Status"] == "Agendado").any():
        st.info("Nenhum agendamento em aberto.")
    else:
        em_aberto = df_ag[df_ag["Status"] == "Agendado"].copy()
        for col in ["IDAgenda","Data","Hora","Cliente","Serviço","Funcionário","Conta","Combo","Observação","Status","Criado_em","Atendido_em","ItensComboJSON"]:
            if col in em_aberto.columns: em_aberto[col] = em_aberto[col].astype(str)
        em_aberto["Valor"] = pd.to_numeric(em_aberto.get("Valor", pd.Series(dtype=float)), errors="coerce").fillna(0.0)

        # Colunas auxiliáres para confirmação
        if "Fiado?" not in em_aberto.columns: em_aberto["Fiado?"] = False
        if "VencimentoFiado" not in em_aberto.columns: em_aberto["VencimentoFiado"] = ""
        if "ContaConf" not in em_aberto.columns: em_aberto["ContaConf"] = em_aberto["Conta"]
        if "Selecionar" not in em_aberto.columns: em_aberto.insert(0, "Selecionar", False)

        st.caption("Selecione, edite campos básicos (Conta/Fiado) e abra a revisão por atendimento para ajustar itens/serviços e valores.")
        edit = st.data_editor(
            em_aberto[["Selecionar","IDAgenda","Data","Hora","Cliente","Serviço","Valor","Funcionário","ContaConf","Fiado?","VencimentoFiado","Combo","Observação","ItensComboJSON"]],
            column_config={
                "Selecionar": st.column_config.CheckboxColumn("Selecionar"),
                "Valor": st.column_config.NumberColumn("Total (R$)", step=0.5, format="%.2f"),
                "ContaConf": st.column_config.TextColumn("Conta / Forma de pagamento"),
                "Fiado?": st.column_config.CheckboxColumn("Fiado?"),
                "VencimentoFiado": st.column_config.DateColumn("Vencimento do fiado"),
            },
            disabled=["IDAgenda","Cliente","Funcionário","Combo","Observação","ItensComboJSON","Data","Hora","Serviço"],
            use_container_width=True, height=420, key="editor_confirm"
        )

        # ===== Blocos de revisão por agendamento selecionado =====
        selecionar = edit[edit["Selecionar"] == True].copy()
        if not selecionar.empty:
            st.divider()
            st.markdown("### Revisão detalhada por atendimento selecionado")
            for _, row in selecionar.iterrows():
                ida = str(row["IDAgenda"])
                with st.expander(f"🔧 Ajustar itens/serviços — ID {ida} | {row['Cliente']} • {row['Data']} {row['Hora']}"):
                    # Inicializa itens do editor dinâmico por ID
                    key_items = f"items_{ida}"
                    key_obs   = f"obs_{ida}"
                    key_conta = f"conta_{ida}"
                    key_fiado = f"fiado_{ida}"
                    key_venc  = f"venc_{ida}"

                    if key_items not in st.session_state:
                        # Base inicial: ItensComboJSON se existir, senão o serviço único
                        base_items = []
                        try:
                            if str(row["ItensComboJSON"]).strip():
                                for it in json.loads(row["ItensComboJSON"]):
                                    s = str(it.get("servico","")).strip()
                                    if s: s = s[:1].upper() + s[1:]
                                    v = it.get("valor", 0.0) or 0.0
                                    base_items.append({"Serviço": s, "Valor (R$)": float(v)})
                            else:
                                s = str(row["Serviço"]).strip()
                                if s: s = s[:1].upper() + s[1:]
                                v = float(str(row["Valor"]))
                                base_items.append({"Serviço": s, "Valor (R$)": float(v)})
                        except Exception:
                            pass
                        if not base_items:
                            base_items = [{"Serviço": "", "Valor (R$)": 0.0}]
                        st.session_state[key_items] = pd.DataFrame(base_items)

                    if key_obs not in st.session_state:
                        st.session_state[key_obs] = str(row["Observação"]).strip()

                    if key_conta not in st.session_state:
                        st.session_state[key_conta] = str(row.get("ContaConf") or row.get("Conta") or "Carteira")

                    if key_fiado not in st.session_state:
                        st.session_state[key_fiado] = bool(row.get("Fiado?") is True)

                    if key_venc not in st.session_state:
                        # Se já vier vencimento do editor de cima, usa; senão hoje + 7
                        try:
                            if pd.notna(row.get("VencimentoFiado")) and str(row.get("VencimentoFiado")).strip():
                                venc_d = pd.to_datetime(row["VencimentoFiado"]).date()
                            else:
                                venc_d = date.today()
                        except Exception:
                            venc_d = date.today()
                        st.session_state[key_venc] = venc_d

                    # Campos editáveis
                    st.write("Edite os itens abaixo (adicione/remova linhas conforme necessário):")
                    df_items = st.data_editor(
                        st.session_state[key_items],
                        key=f"editor_items_{ida}",
                        num_rows="dynamic",
                        column_config={
                            "Serviço": st.column_config.TextColumn("Serviço"),
                            "Valor (R$)": st.column_config.NumberColumn("Valor (R$)", step=0.5, format="%.2f"),
                        },
                        use_container_width=True
                    )
                    st.session_state[key_items] = df_items

                    cA, cB, cC = st.columns([1,1,2])
                    st.session_state[key_conta] = cA.text_input("Conta / Forma de pagamento", value=st.session_state[key_conta], key=f"conta_input_{ida}")
                    st.session_state[key_fiado] = cB.checkbox("Fiado?", value=st.session_state[key_fiado], key=f"fiado_chk_{ida}")
                    if st.session_state[key_fiado]:
                        st.session_state[key_venc] = cC.date_input("Vencimento do fiado", value=st.session_state[key_venc], key=f"venc_input_{ida}")
                    else:
                        cC.markdown("<small>Pagamento à vista (será gravado com DataPagamento = hoje)</small>", unsafe_allow_html=True)

                    st.session_state[key_obs] = st.text_area("Observação", value=st.session_state[key_obs], key=f"obs_input_{ida}")

        c1, c2, c3 = st.columns([1,1,2])
        if c1.checkbox("Marcar todos visíveis"):
            edit["Selecionar"] = True
            selecionar = edit.copy()

        salvar_btn = c2.button("Confirmar selecionados e lançar na Base", type="primary", use_container_width=True)

        if salvar_btn:
            selecionar = edit[edit["Selecionar"] == True].copy()
            if selecionar.empty:
                st.warning("Selecione pelo menos um agendamento.")
            else:
                # Carrega/garante Base de Dados Feminino
                df_base = carregar_df(ABA_DADOS_FEM)
                cols_base = list(df_base.columns) if not df_base.empty else [
                    "Data","Serviço","Valor","Conta","Cliente","Combo","Funcionário",
                    "Fase","Tipo","Período","StatusFiado","IDLancFiado","VencimentoFiado",
                    "DataPagamento","Fiado_Vencimento","Fiado_Status","Quitado_em","Observação"
                ]
                if df_base.empty:
                    df_base = pd.DataFrame(columns=cols_base)

                novos, ids = [], []
                hoje_txt = tz_now().strftime(DATA_FMT)
                agora_txt = tz_now().strftime(f"{DATA_FMT} {HORA_FMT}")

                for _, row in selecionar.iterrows():
                    ida = str(row["IDAgenda"])
                    data_txt = str(row["Data"]); hora_txt = str(row["Hora"])
                    try:
                        hh = int(hora_txt.split(":")[0])
                    except Exception:
                        hh = 9
                    periodo = periodo_por_hora(hh)

                    # Recupera ajustes feitos no bloco de revisão (se aberto)
                    key_items = f"items_{ida}"
                    key_obs   = f"obs_{ida}"
                    key_conta = f"conta_{ida}"
                    key_fiado = f"fiado_{ida}"
                    key_venc  = f"venc_{ida}"

                    # Fallbacks: se não revisou manualmente, usa o que está na linha (combo itens ou serviço único)
                    if key_items in st.session_state:
                        df_items = st.session_state[key_items].copy()
                    else:
                        base_items = []
                        try:
                            if str(row["ItensComboJSON"]).strip():
                                for it in json.loads(row["ItensComboJSON"]):
                                    s = str(it.get("servico","")).strip()
                                    if s: s = s[:1].upper() + s[1:]
                                    v = it.get("valor", 0.0) or 0.0
                                    base_items.append({"Serviço": s, "Valor (R$)": float(v)})
                            else:
                                s = str(row["Serviço"]).strip()
                                if s: s = s[:1].upper() + s[1:]
                                v = float(str(row["Valor"]))
                                base_items.append({"Serviço": s, "Valor (R$)": float(v)})
                        except Exception:
                            base_items = [{"Serviço":"", "Valor (R$)":0.0}]
                        df_items = pd.DataFrame(base_items)

                    # Normaliza itens (remove linhas vazias)
                    df_items["Serviço"] = df_items["Serviço"].astype(str).str.strip()
                    df_items["Serviço"] = df_items["Serviço"].apply(lambda s: (s[:1].upper() + s[1:]) if s else s)
                    df_items["Valor (R$)"] = pd.to_numeric(df_items["Valor (R$)"], errors="coerce").fillna(0.0)
                    df_items = df_items[df_items["Serviço"] != ""].reset_index(drop=True)

                    conta_escolhida = st.session_state.get(key_conta, str(row.get("ContaConf") or row.get("Conta") or "Carteira")).strip() or "Carteira"
                    fiado_flag = bool(st.session_state.get(key_fiado, bool(row.get("Fiado?") is True)))
                    try:
                        venc_dt = st.session_state.get(key_venc, date.today())
                        venc_txt = venc_dt.strftime(DATA_FMT) if isinstance(venc_dt, date) else str(venc_dt)
                    except Exception:
                        venc_txt = hoje_txt
                    obs_txt = st.session_state.get(key_obs, str(row["Observação"]).strip())

                    # Gera linhas para Base
                    if df_items.empty:
                        # fallback mínimo para não perder o atendimento
                        s = str(row["Serviço"]).strip()
                        if s: s = s[:1].upper() + s[1:]
                        v = float(str(row["Valor"])) if str(row["Valor"]).strip() else 0.0
                        df_items = pd.DataFrame([{"Serviço": s, "Valor (R$)": v}])

                    for _, it in df_items.iterrows():
                        s_item = str(it["Serviço"]).strip()
                        v_item = float(it["Valor (R$)"]) if it["Valor (R$)"] not in (None, "") else 0.0

                        novo = {
                            "Data": data_txt,
                            "Serviço": s_item,
                            "Valor": v_item,
                            "Conta": conta_escolhida,
                            "Cliente": str(row["Cliente"]).strip(),
                            "Combo": str(row["Combo"]).strip(),
                            "Funcionário": str(row["Funcionário"]).strip() or FUNCIONARIO_PADRAO,
                            "Fase": "Dono + funcionário",
                            "Tipo": "Serviço",
                            "Período": periodo,
                            "StatusFiado": ("Aberto" if fiado_flag else ""),
                            "IDLancFiado": "",
                            "VencimentoFiado": (venc_txt if fiado_flag else ""),
                            "DataPagamento": ("" if fiado_flag else hoje_txt),
                            "Fiado_Vencimento": (venc_txt if fiado_flag else ""),
                            "Fiado_Status": ("A receber" if fiado_flag else ""),
                            "Quitado_em": "",
                            "Observação": obs_txt
                        }
                        # completa colunas faltantes
                        for c in cols_base:
                            if c not in novo: novo[c] = ""
                        novos.append(novo)

                    ids.append(ida)

                # Persiste na Base
                df_base = pd.concat([df_base, pd.DataFrame(novos)], ignore_index=True)
                salvar_df(ABA_DADOS_FEM, df_base)

                # Atualiza agendamentos
                df_ag = carregar_df(ABA_AGENDAMENTO)
                df_ag.loc[df_ag["IDAgenda"].isin(ids), "Status"] = "Atendido"
                df_ag.loc[df_ag["IDAgenda"].isin(ids), "Atendido_em"] = agora_txt
                salvar_df(ABA_AGENDAMENTO, df_ag)

                # Telegram por atendimento
                for _, row in selecionar.iterrows():
                    ida = str(row["IDAgenda"])
                    key_items = f"items_{ida}"
                    key_conta = f"conta_{ida}"
                    key_fiado = f"fiado_{ida}"
                    key_venc  = f"venc_{ida}"

                    conta_escolhida = st.session_state.get(key_conta, str(row.get("ContaConf") or row.get("Conta") or "Carteira")).strip() or "Carteira"
                    fiado_flag = bool(st.session_state.get(key_fiado, bool(row.get("Fiado?") is True)))
                    try:
                        venc_dt = st.session_state.get(key_venc, date.today())
                        venc_txt = venc_dt.strftime(DATA_FMT) if isinstance(venc_dt, date) else str(venc_dt)
                    except Exception:
                        venc_txt = tz_now().strftime(DATA_FMT)

                    # total p/ card
                    if key_items in st.session_state:
                        df_items = st.session_state[key_items].copy()
                    else:
                        df_items = pd.DataFrame([{"Serviço": str(row["Serviço"]).strip(), "Valor (R$)": float(str(row["Valor"]))}])
                    df_items["Valor (R$)"] = pd.to_numeric(df_items["Valor (R$)"], errors="coerce").fillna(0.0)
                    total_val = float(df_items["Valor (R$)"].sum())

                    foto = foto_do_cliente(str(row["Cliente"]).strip()) or PHOTO_FALLBACK_URL
                    caption = card_confirmacao(
                        c=str(row["Cliente"]).strip(),
                        s=(str(row["Serviço"]).strip() or f"Combo: {row['Combo']}"),
                        v=total_val, conta=conta_escolhida,
                        f=str(row["Funcionário"]).strip() or FUNCIONARIO_PADRAO,
                        d=str(row["Data"]), h=str(row["Hora"]),
                        obs=str(row["Observação"]).strip(), ida=ida,
                        fiado=fiado_flag, venc=venc_txt
                    )
                    send_tg_photo(foto, caption)

                send_tg_msg(f"🧾 <b>Resumo</b>: {len(ids)} atendimento(s) confirmado(s) (fiado/à vista) e lançados na Base de Dados Feminino.")
                st.success(f"{len(ids)} atendimento(s) confirmados com sucesso. Registros gravados e cards enviados.")

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
            def dt_key(r):
                try:
                    d = datetime.strptime(str(r["Data"]), DATA_FMT)
                    h = datetime.strptime(str(r["Hora"]), HORA_FMT).time()
                    return datetime.combine(d.date(), h)
                except Exception:
                    return datetime.max
            abertos["__ord"] = abertos.apply(dt_key, axis=1)
            abertos = abertos.sort_values("__ord").drop(columns="__ord")
            st.dataframe(
                abertos[["IDAgenda","Data","Hora","Cliente","Serviço","Valor","Funcionário","Conta","Combo","Observação"]],
                use_container_width=True, hide_index=True
            )
            st.download_button(
                "Baixar CSV",
                abertos.to_csv(index=False).encode("utf-8-sig"),
                file_name="agendamentos_em_aberto.csv",
                mime="text/csv"
            )
