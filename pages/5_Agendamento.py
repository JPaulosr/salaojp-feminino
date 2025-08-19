# -*- coding: utf-8 -*-
# 14_Agendamento.py — Agenda Feminino com foto, combos editáveis e cards no Telegram

import streamlit as st
import pandas as pd
import gspread, json, os, pytz, unicodedata, requests, random, string
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

# Telegram (pode mover para secrets; há fallback)
TELEGRAM_TOKEN = st.secrets.get("TELEGRAM_TOKEN", "8257359388:AAGayJElTPT0pQadtamVf8LoL7R6EfWzFGE")
CHAT_ID_JPAULO = st.secrets.get("TELEGRAM_CHAT_ID_JPAULO", "493747253")
CHAT_ID_FEMININO = st.secrets.get("TELEGRAM_CHAT_ID_FEMININO", "-1002965378062")

FUNCIONARIOS_FEM = ["Meire","Daniela"]
FUNCIONARIO_PADRAO = "Meire"

# =========================
# Utils
# =========================
def tz_now(): return datetime.now(pytz.timezone(TZ))

def norm(s):
    if not isinstance(s,str): return ""
    s = "".join(ch for ch in unicodedata.normalize("NFKD", s.strip().lower()) if not unicodedata.combining(ch))
    return s

def periodo_por_hora(hh): return "Manhã" if 5<=hh<12 else ("Tarde" if 12<=hh<18 else "Noite")

def novo_id(prefix="AG"):
    return f"{prefix}-{tz_now().strftime('%Y%m%d%H%M%S')}-{''.join(random.choices(string.ascii_uppercase+string.digits,k=4))}"

def send_tg_msg(text):
    try:
        url=f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        for chat_id in (CHAT_ID_FEMININO, CHAT_ID_JPAULO):
            requests.post(url, json={"chat_id":chat_id,"text":text,"parse_mode":"HTML","disable_web_page_preview":True}, timeout=10)
    except Exception as e:
        st.warning(f"Falha Telegram: {e}")

def send_tg_photo(photo_url, caption):
    try:
        url=f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
        for chat_id in (CHAT_ID_FEMININO, CHAT_ID_JPAULO):
            r=requests.post(url, data={"chat_id":chat_id,"photo":photo_url,"caption":caption,"parse_mode":"HTML"}, timeout=10)
            if r.status_code!=200: send_tg_msg(caption)
    except Exception:
        send_tg_msg(caption)

def card_confirmacao(c, s, v, conta, f, d, h, obs, ida):
    val="-" if v in ("",None) else f"R$ {float(v):.2f}".replace(".",",")
    return ("✅ <b>Atendimento confirmado</b>\n"
            f"👤 <b>Cliente:</b> {c}\n"
            f"🧴 <b>Serviço:</b> {s}\n"
            f"💳 <b>Conta:</b> {conta}\n"
            f"💲 <b>Valor:</b> {val}\n"
            f"🧑‍💼 <b>Funcionário:</b> {f}\n"
            f"🗓️ <b>Data/Hora:</b> {d} {h}\n"
            f"📝 <b>Obs.:</b> {obs or '-'}\n"
            f"🏷️ <b>ID:</b> {ida}")

# =========================
# Google Sheets (robusto)
# =========================
@st.cache_resource(show_spinner=False)
def conectar_sheets():
    cand = (st.secrets.get("gcp_service_account") or st.secrets.get("gcp_service_account_feminino")
            or st.secrets.get("google_credentials") or st.secrets.get("GCP_SERVICE_ACCOUNT")
            or os.environ.get("GCP_SERVICE_ACCOUNT"))
    if cand is None:
        raise KeyError("Credenciais ausentes. Adicione em secrets uma das chaves: gcp_service_account / gcp_service_account_feminino / google_credentials / GCP_SERVICE_ACCOUNT")
    if isinstance(cand,str): cand=json.loads(cand)
    creds = Credentials.from_service_account_info(cand, scopes=[
        "https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"
    ])
    return gspread.authorize(creds)

gc = conectar_sheets(); sh = gc.open_by_key(SHEET_ID)

def abrir_ws(nome):
    try: return sh.worksheet(nome)
    except gspread.WorksheetNotFound: return sh.add_worksheet(title=nome, rows=3000, cols=60)

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

def carregar_df(aba):
    ws=abrir_ws(aba)
    df=get_as_dataframe(ws, header=0, evaluate_formulas=False).dropna(how="all")
    if aba==ABA_AGENDAMENTO and not df.empty:
        for c in COLS_AGENDA:
            if c not in df.columns: df[c]=""
        df=df[COLS_AGENDA]
    return df

def salvar_df(aba, df):
    ws=abrir_ws(aba)
    if df.empty:
        ws.clear(); ws.update(rowcol_to_a1(1,1), [list(df.columns)]); return
    ws.clear(); set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)

# -------------------------
# Dados auxiliares (Clientes, Serviços, Combos, Foto)
# -------------------------
@st.cache_data(show_spinner=False)
def clientes_existentes():
    nomes=set()
    try:
        df=carregar_df(ABA_DADOS_FEM)
        if "Cliente" in df.columns:
            [nomes.add(str(x).strip()) for x in df["Cliente"].dropna().astype(str)]
    except: pass
    try:
        df2=carregar_df(ABA_STATUS_FEM)
        nome_col=None
        for c in df2.columns:
            if norm(c) in ("cliente","nome","nome_cliente"): nome_col=c; break
        if nome_col:
            [nomes.add(str(x).strip()) for x in df2[nome_col].dropna().astype(str)]
    except: pass
    return sorted(nomes, key=lambda s: norm(s))

@st.cache_data(show_spinner=False)
def servicos_e_combos():
    servs, combs = [], []
    try:
        df = carregar_df(ABA_DADOS_FEM)
        if not df.empty:
            if "Serviço" in df.columns: servs=[s for s in df["Serviço"].dropna().astype(str) if s.strip()]
            if "Combo"   in df.columns: combs=[c for c in df["Combo"].dropna().astype(str)   if c.strip()]
    except: pass
    servs=[(s[:1].upper()+s[1:]).strip() for s in servs]
    return sorted(set(servs), key=lambda s:norm(s)), sorted(set(combs), key=lambda s:norm(s))

def preco_sugerido(servico):
    try:
        df=carregar_df(ABA_DADOS_FEM)
        if "Serviço" in df.columns and "Valor" in df.columns:
            m=df[df["Serviço"].astype(str).str.lower()==servico.lower()]
            vals=pd.to_numeric(m["Valor"], errors="coerce").dropna()
            if not vals.empty: return round(float(vals.median()),2)
    except: pass
    return None

def foto_do_cliente(cliente):
    if not cliente: return PHOTO_FALLBACK_URL
    try:
        df=carregar_df(ABA_STATUS_FEM)
        if df.empty: return PHOTO_FALLBACK_URL
        nome_col=None
        for c in df.columns:
            if norm(c) in ("cliente","nome","nome_cliente"): nome_col=c; break
        if not nome_col: return PHOTO_FALLBACK_URL
        df["_k"]=df[nome_col].astype(str).apply(norm)
        linha=df[df["_k"]==norm(cliente)].head(1)
        if not linha.empty:
            row=linha.iloc[0]
            for c in FOTO_COL_CANDIDATES:
                if c in df.columns:
                    u=str(row.get(c,"")).strip()
                    if u.startswith(("http://","https://")): return u
    except: pass
    return PHOTO_FALLBACK_URL

# --------- helpers para listas grandes (filtro simples) ----------
def filter_options(options, query, limit=200):
    if not query: 
        return options[:limit]
    q=norm(query)
    return [o for o in options if q in norm(o)][:limit]

# =========================
# UI
# =========================
st.title("📅 Agendamento (Feminino)")
acao = st.radio("Ações:", ["➕ Agendar","✅ Confirmar atendimentos","🗂️ Em aberto & exportação"], horizontal=False)

# ---------- 1) AGENDAR ----------
if acao.startswith("➕"):
    st.subheader("Novo agendamento")
    cA,cB,cC=st.columns([1,1,2])
    data_ag=cA.date_input("Data", value=date.today())
    hora_ag=cB.time_input("Hora", value=dt_time(9,0,0), step=300)
    funcionario=cC.selectbox("Funcionário", options=FUNCIONARIOS_FEM, index=FUNCIONARIOS_FEM.index(FUNCIONARIO_PADRAO))

    # Cliente (com filtro para não mostrar milhares de opções)
    clientes=clientes_existentes()
    fcli=st.text_input("🔎 Filtrar clientes", placeholder="Digite parte do nome…")
    clientes_filtrados=filter_options(clientes, fcli, limit=200)
    op_cli=["(digite novo cliente)"]+clientes_filtrados
    cli_sel=st.selectbox("Cliente", op_cli, index=1 if clientes_filtrados else 0)
    cli_txt=st.text_input("Novo cliente") if cli_sel=="(digite novo cliente)" else ""
    cliente_final=(cli_txt or cli_sel).strip()

    # Serviço (com filtro)
    servs, combs = servicos_e_combos()
    fserv=st.text_input("🔎 Filtrar serviços", placeholder="Ex.: escova, unha…")
    servs_filtrados = filter_options(servs, fserv, limit=200)

    c1,c2=st.columns([2,1])
    serv_sel=c1.selectbox("Serviço", ["(Outro)"]+servs_filtrados)
    serv_txt=c1.text_input("Digite o serviço") if serv_sel=="(Outro)" else ""
    servico=(serv_txt or serv_sel).strip()
    if servico: servico=servico[:1].upper()+servico[1:]
    valor=c2.text_input("Valor (R$)", placeholder="Ex.: 35,00")

    # Conta / Combo (com filtro)
    c3,c4=st.columns([1,1])
    conta=c3.text_input("Conta / Forma de pagamento", value="Carteira")

    fcombo=st.text_input("🔎 Filtrar combos", placeholder="Ex.: unha+mão…")
    combs_filtrados = filter_options(combs, fcombo, limit=200)
    combo_sel=c4.selectbox("Combo", ["(Sem combo)"]+combs_filtrados, index=0)
    combo_txt=c4.text_input("Digite o combo (opcional)", placeholder="Ex.: corte+barba") if combo_sel=="(Sem combo)" else ""
    combo=(combo_txt or ("" if combo_sel=="(Sem combo)" else combo_sel)).strip()

    obs=st.text_area("Observação (opcional)", placeholder="Preferências, referências, etc.")

    # Itens do combo (mini‑tabela)
    itens_combo=[]
    if combo:
        raw=[x.strip() for x in combo.split("+") if x.strip()]
        rows=[]
        for s in raw:
            nome=s[:1].upper()+s[1:]
            sug=preco_sugerido(nome)
            rows.append({"Serviço": nome, "Valor (R$)": sug})
        df_edit=pd.DataFrame(rows)
        st.markdown("**Itens do combo (edite os valores antes de agendar):**")
        df_edit=st.data_editor(
            df_edit,
            column_config={"Valor (R$)": st.column_config.NumberColumn("Valor (R$)", step=0.5, format="%.2f")},
            disabled=["Serviço"],
            key="editor_itens_combo",
            use_container_width=True,
            height=min(420, 120 + 30*len(rows))
        )
        for _,r in df_edit.iterrows():
            v=None if pd.isna(r["Valor (R$)"]) else float(r["Valor (R$)"])
            itens_combo.append({"servico": r["Serviço"], "valor": v})

    if st.button("Agendar e notificar", type="primary", use_container_width=True):
        if not cliente_final:
            st.error("Informe o cliente.")
        elif not (servico or combo):
            st.error("Informe o serviço ou um combo.")
        else:
            garantir_estrutura_agenda()
            df_ag=carregar_df(ABA_AGENDAMENTO)

            # total = soma itens do combo (quando houver) ou valor do campo
            valor_total=None
            if combo and itens_combo:
                soma=[i["valor"] for i in itens_combo if i["valor"] not in (None,"")]
                if soma: valor_total=round(float(sum(soma)),2)
            if valor_total is None:
                try: valor_total=round(float(str(valor).replace(",",".")),2)
                except: valor_total=""

            ida=novo_id("AG"); criado_em=tz_now().strftime(f"{DATA_FMT} {HORA_FMT}")
            linha={
                "IDAgenda": ida, "Data": data_ag.strftime(DATA_FMT), "Hora": hora_ag.strftime(HORA_FMT),
                "Cliente": cliente_final, "Serviço": servico, "Valor": valor_total, "Conta": conta,
                "Funcionário": funcionario, "Combo": combo, "Observação": obs,
                "Status": "Agendado", "Criado_em": criado_em, "Atendido_em": "",
                "ItensComboJSON": json.dumps(itens_combo, ensure_ascii=False) if itens_combo else ""
            }
            df_ag=pd.concat([df_ag, pd.DataFrame([linha])], ignore_index=True)
            salvar_df(ABA_AGENDAMENTO, df_ag)

            # Telegram com foto + detalhamento
            foto=foto_do_cliente(cliente_final)
            det=""
            if itens_combo:
                linhas=[f"   • {it['servico']}: R$ {0 if (it['valor'] in (None,'')) else it['valor']:.2f}".replace(".",",")
                        for it in itens_combo]
                det="\n🧾 <b>Itens:</b>\n" + "\n".join(linhas)
            caption=("📅 <b>Novo agendamento</b>\n"
                     f"👤 <b>Cliente:</b> {cliente_final}\n"
                     f"🧴 <b>Serviço:</b> {servico or '-'}\n"
                     f"💳 <b>Conta:</b> {conta}\n"
                     f"💲 <b>Total:</b> {('-' if valor_total in ('',None) else ('R$ '+str(f'{valor_total:.2f}'.replace('.',',')) ))}\n"
                     f"🧑‍💼 <b>Funcionário:</b> {funcionario}\n"
                     f"🗓️ <b>Data/Hora:</b> {linha['Data']} {linha['Hora']}\n"
                     f"📝 <b>Obs.:</b> {obs or '-'}\n"
                     f"🏷️ <b>ID:</b> {ida}{det}")
            send_tg_photo(foto, caption)
            st.success("Agendado e notificado com sucesso ✅")

# ---------- 2) CONFIRMAR ----------
elif acao.startswith("✅"):
    st.subheader("Confirmar atendimentos (lote)")
    df_ag=carregar_df(ABA_AGENDAMENTO)
    if df_ag.empty or not (df_ag["Status"]=="Agendado").any():
        st.info("Nenhum agendamento em aberto.")
    else:
        em_aberto=df_ag[df_ag["Status"]=="Agendado"].copy()
        em_aberto["Selecionar"]=False
        def fix_val(v):
            try: return round(float(str(v).replace(",",".")),2)
            except: return ""
        em_aberto["Valor"]=em_aberto["Valor"].apply(fix_val)

        st.caption("Edite antes de confirmar. (Quando houver combo, os itens gravados serão usados.)")
        edit=st.data_editor(
            em_aberto,
            column_config={
                "Selecionar": st.column_config.CheckboxColumn("Selecionar"),
                "Valor": st.column_config.NumberColumn("Total (R$)", step=0.5, format="%.2f"),
                "Serviço": st.column_config.TextColumn("Serviço"),
                "Conta": st.column_config.TextColumn("Conta"),
                "Combo": st.column_config.TextColumn("Combo"),
                "Observação": st.column_config.TextColumn("Observação"),
            },
            disabled=["IDAgenda","Data","Hora","Cliente","Funcionário","Status","Criado_em","Atendido_em","ItensComboJSON"],
            use_container_width=True, height=460, key="editor_confirm"
        )

        c1,c2=st.columns([1,1])
        if c1.checkbox("Marcar todos visíveis"): edit["Selecionar"]=True
        if c2.button("Confirmar selecionados e lançar na Base", type="primary", use_container_width=True):
            selecionar=edit[edit["Selecionar"]==True]
            if selecionar.empty:
                st.warning("Selecione pelo menos um agendamento.")
            else:
                df_base=carregar_df(ABA_DADOS_FEM)
                cols_base=list(df_base.columns) if not df_base.empty else [
                    "Data","Serviço","Valor","Conta","Cliente","Combo","Funcionário",
                    "Fase","Tipo","Período","StatusFiado","IDLancFiado","VencimentoFiado",
                    "DataPagamento","Fiado_Vencimento","Fiado_Status","Quitado_em","Observação"
                ]
                if df_base.empty: df_base=pd.DataFrame(columns=cols_base)

                novos=[]; ids=[]
                for _,row in selecionar.iterrows():
                    data_txt=str(row["Data"]); hora_txt=str(row["Hora"])
                    try: hh=int(hora_txt.split(":")[0])
                    except: hh=9
                    periodo=periodo_por_hora(hh)

                    # Se tiver ItensComboJSON => lançar 1 linha por item
                    itens=[]
                    try:
                        if str(row["ItensComboJSON"]).strip():
                            itens=json.loads(row["ItensComboJSON"])
                    except: itens=[]

                    if itens:
                        for it in itens:
                            s=str(it.get("servico","")).strip()
                            if s: s=s[:1].upper()+s[1:]
                            v=it.get("valor",0.0) or 0.0
                            novo={
                                "Data": data_txt,"Serviço": s,"Valor": float(v),"Conta": str(row["Conta"]).strip() or "Carteira",
                                "Cliente": str(row["Cliente"]).strip(),"Combo": str(row["Combo"]).strip(),
                                "Funcionário": str(row["Funcionário"]).strip() or FUNCIONARIO_PADRAO,
                                "Fase":"Dono + funcionário","Tipo":"Serviço","Período":periodo,
                                "StatusFiado":"","IDLancFiado":"","VencimentoFiado":"","DataPagamento":"",
                                "Fiado_Vencimento":"","Fiado_Status":"","Quitado_em":"","Observação": str(row["Observação"]).strip()
                            }
                            for c in cols_base:
                                if c not in novo: novo[c]=""
                            novos.append(novo)
                    else:
                        s=str(row["Serviço"]).strip()
                        if s: s=s[:1].upper()+s[1:]
                        try: v=float(str(row["Valor"]).replace(",",".")); 
                        except: v=0.0
                        novo={
                            "Data": data_txt,"Serviço": s,"Valor": v,"Conta": str(row["Conta"]).strip() or "Carteira",
                            "Cliente": str(row["Cliente"]).strip(),"Combo": str(row["Combo"]).strip(),
                            "Funcionário": str(row["Funcionário"]).strip() or FUNCIONARIO_PADRAO,
                            "Fase":"Dono + funcionário","Tipo":"Serviço","Período":periodo,
                            "StatusFiado":"","IDLancFiado":"","VencimentoFiado":"","DataPagamento":"",
                            "Fiado_Vencimento":"","Fiado_Status":"","Quitado_em":"","Observação": str(row["Observação"]).strip()
                        }
                        for c in cols_base:
                            if c not in novo: novo[c]=""
                        novos.append(novo)

                    ids.append(row["IDAgenda"])

                # append base
                df_base=pd.concat([df_base, pd.DataFrame(novos)], ignore_index=True)
                salvar_df(ABA_DADOS_FEM, df_base)

                # atualizar agenda
                df_ag=carregar_df(ABA_AGENDAMENTO)
                agora=tz_now().strftime(f"{DATA_FMT} {HORA_FMT}")
                df_ag.loc[df_ag["IDAgenda"].isin(ids), "Status"]="Atendido"
                df_ag.loc[df_ag["IDAgenda"].isin(ids), "Atendido_em"]=agora
                salvar_df(ABA_AGENDAMENTO, df_ag)

                # notificar: card por confirmação
                for _,row in selecionar.iterrows():
                    foto=foto_do_cliente(str(row["Cliente"]).strip())
                    caption=card_confirmacao(
                        c=str(row["Cliente"]).strip(),
                        s=(str(row["Serviço"]).strip() or f"Combo: {row['Combo']}"),
                        v=row["Valor"], conta=str(row["Conta"]).strip() or "Carteira",
                        f=str(row["Funcionário"]).strip() or FUNCIONARIO_PADRAO,
                        d=str(row["Data"]), h=str(row["Hora"]),
                        obs=str(row["Observação"]).strip(), ida=str(row["IDAgenda"])
                    )
                    send_tg_photo(foto, caption)

                send_tg_msg(f"🧾 <b>Resumo</b>: {len(ids)} atendimento(s) confirmado(s) e lançados na Base de Dados Feminino.")
                st.success(f"{len(ids)} atendimento(s) confirmados, linhas geradas (combo) e cards enviados.")

# ---------- 3) EM ABERTO ----------
else:
    st.subheader("Agendamentos em aberto")
    df_ag=carregar_df(ABA_AGENDAMENTO)
    if df_ag.empty: st.info("Nenhum agendamento cadastrado.")
    else:
        abertos=df_ag[df_ag["Status"]=="Agendado"].copy()
        if abertos.empty: st.success("Sem agendamentos em aberto 🎉")
        else:
            def dt_key(r):
                try:
                    d=datetime.strptime(str(r["Data"]), DATA_FMT)
                    h=datetime.strptime(str(r["Hora"]), HORA_FMT).time()
                    return datetime.combine(d.date(), h)
                except: return datetime.max
            abertos["__ord"]=abertos.apply(dt_key, axis=1)
            abertos=abertos.sort_values("__ord").drop(columns="__ord")
            st.dataframe(
                abertos[["IDAgenda","Data","Hora","Cliente","Serviço","Valor","Funcionário","Conta","Combo","Observação"]],
                use_container_width=True, hide_index=True
            )
            st.download_button("Baixar CSV", abertos.to_csv(index=False).encode("utf-8-sig"),
                               file_name="agendamentos_em_aberto.csv", mime="text/csv")
