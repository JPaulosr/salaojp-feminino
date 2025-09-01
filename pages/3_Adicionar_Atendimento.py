# 11_Adicionar_Atendimento.py — VERSÃO FEMININO COMPLETA (atualizada)
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
TELEGRAM_CHAT_ID_FEMININO = "-1002965378062"
TELEGRAM_CHAT_ID_DANIELA = "-1003039502089"

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

def _calc_payout_daniela(valor_total: float, pct: float | None) -> str:
    if pct is None:
        return ""
    try:
        pctf = max(0.0, min(100.0, float(pct)))
    except Exception:
        pctf = 0.0
    valor_receber = round(float(valor_total or 0.0) * (pctf / 100.0), 2)
    return f"💰 Daniela recebe: <b>{_fmt_brl(valor_receber)}</b> ({pctf:.0f}%)"

# =========================
# TELEGRAM – envio
# =========================
def tg_send(text: str, chat_id: str) -> bool:
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        r = requests.post(url, json=payload, timeout=30)
        return r.ok
    except Exception:
        return False

def tg_send_photo(photo_url: str, caption: str, chat_id: str) -> bool:
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
        payload = {"chat_id": chat_id, "photo": photo_url, "caption": caption, "parse_mode": "HTML"}
        r = requests.post(url, data=payload, timeout=30)
        if r.ok: return True
        return tg_send(caption, chat_id=chat_id)
    except Exception:
        return tg_send(caption, chat_id=chat_id)

# =========================
# CARDS
# =========================
def make_card_caption(df_all, cliente, data_str, funcionario, servico_label, valor_total, periodo_label,
                      pct_daniela=None, append_sections=None, conta_pag: str | None = None):
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

def _conta_do_dia(df_all: pd.DataFrame, cliente: str, data_str: str) -> str | None:
    d = df_all[
        (df_all["Cliente"].astype(str).str.strip()==cliente) &
        (df_all["Data"].astype(str).str.strip()==data_str)
    ]
    if d.empty or "Conta" not in d.columns: return None
    s = d["Conta"].astype(str).str.strip()
    s = s[s!=""]
    return s.mode().iat[0] if not s.empty else None

def enviar_card(df_all, cliente, funcionario, data_str, servico=None, valor=None, combo=None,
                pct_daniela=None, conta_pag: str | None = None):
    servico_label = servico or "-"
    valor_total = float(valor or 0.0)
    periodo_label = "-"

    if not conta_pag:
        conta_pag = _conta_do_dia(df_all, cliente, data_str)

    extras = []  # ex: cartão
    caption = make_card_caption(df_all, cliente, data_str, funcionario, servico_label, valor_total, periodo_label,
                                pct_daniela=(pct_daniela if funcionario=="Daniela" else None),
                                append_sections=extras, conta_pag=conta_pag)

    foto = None

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
# VALORES (exemplo)
# =========================
VALORES={"Corte":35.0,"Escova":25.0,"Unha mão":25.0,"Unha pé":25.0,"Sobrancelhas":25.0,
         "Designer de Henna":30.0,"Manicure":25.0,"Pedicure":30.0,"Progressiva":150.0}
def obter_valor_servico(servico):
    for k,v in VALORES.items():
        if k.lower()==str(servico).lower(): return v
    return 0.0

def _preencher_fiado_vazio(l): 
    for c in [*COLS_FIADO,*COLS_PAG_EXTRAS]: l.setdefault(c,"")
    return l

def ja_existe_atendimento(cliente, data, servico, combo=""):
    df,_=carregar_base(); df["Combo"]=df["Combo"].fillna("")
    servico_norm=_cap_first(servico); df_serv_norm=df["Serviço"].astype(str).map(_cap_first)
    f=((df["Cliente"].astype(str).str.strip()==cliente) &
       (df["Data"].astype(str).str.strip()==data) &
       (df_serv_norm==servico_norm) &
       (df["Combo"].astype(str).str.strip()==str(combo).strip()))
    return not df[f].empty

def sugestoes_do_cliente(df_all, cli, conta_default, periodo_default, funcionario_default):
    d=df_all[df_all["Cliente"].astype(str).str.strip()==cli].copy()
    if d.empty: return conta_default, periodo_default, funcionario_default
    d["_dt"]=pd.to_datetime(d["Data"], format=DATA_FMT, errors="coerce")
    d=d.dropna(subset=["_dt"]).sort_values("_dt")
    if d.empty: return conta_default, periodo_default, funcionario_default
    ultima=d.iloc[-1]
    conta=(ultima.get("Conta") or "").strip() or conta_default
    periodo=(ultima.get("Período") or "").strip() or periodo_default
    func=(ultima.get("Funcionário") or "").strip() or funcionario_default
    if periodo not in ["Manhã","Tarde","Noite"]: periodo=periodo_default
    if func not in FUNCIONARIOS_FEM+["JPaulo","Vinicius"]: func=funcionario_default
    return conta, periodo, func

# =========================
# UI
# =========================
st.set_page_config(layout="wide")
st.title("📅 Adicionar Atendimento (Feminino)")

df_existente,_=carregar_base()
df_existente["_dt"]=pd.to_datetime(df_existente["Data"], format=DATA_FMT, errors="coerce")
df_2025=df_existente[df_existente["_dt"].dt.year==2025]

clientes_existentes=sorted(df_2025["Cliente"].dropna().unique())
df_2025=df_2025[df_2025["Serviço"].notna()].copy()
servicos_existentes=sorted(df_2025["Serviço"].str.strip().unique())
contas_existentes=sorted([c for c in df_2025["Conta"].dropna().astype(str).str.strip().unique() if c])
combos_existentes=sorted([c for c in df_2025["Combo"].dropna().astype(str).str.strip().unique() if c])

modo_lote=st.toggle("📦 Cadastro em Lote (vários clientes de uma vez)", value=False)
data=st.date_input("Data", value=datetime.today()).strftime("%d/%m/%Y")

if modo_lote:
    col1,col2=st.columns(2)
    with col1:
        conta_global=st.selectbox("Forma de Pagamento (padrão)",
            list(dict.fromkeys(contas_existentes+["Carteira","Pix","Transferência","Nubank CNPJ","Nubank","Pagseguro","Mercado Pago"])))
    with col2:
        funcionario_global=st.selectbox("Funcionário (padrão)", FUNCIONARIOS_FEM, index=0)
    periodo_global=st.selectbox("Período do Atendimento (padrão)", ["Manhã","Tarde","Noite"])
    tipo=st.selectbox("Tipo", ["Serviço","Produto"])
else:
    conta_global=None; funcionario_global=None; periodo_global=None; tipo="Serviço"
fase="Dono + funcionário"

# =========================
# MODO UM POR VEZ
# =========================
if not modo_lote:
    cA,cB=st.columns(2)
    with cA:
        cliente=st.selectbox("Nome do Cliente", clientes_existentes)
    with cB:
        novo_nome=st.text_input("Ou digite um novo nome de cliente")
        cliente=novo_nome if novo_nome else cliente

    conta_fallback=(contas_existentes[0] if contas_existentes else "Carteira")
    periodo_fallback="Manhã"
    func_fallback=(FUNCIONARIOS_FEM[0] if FUNCIONARIOS_FEM else "Daniela")

    sug_conta,sug_periodo,sug_func=sugestoes_do_cliente(
        df_existente, cliente,
        conta_global or conta_fallback,
        periodo_global or periodo_fallback,
        funcionario_global or func_fallback
    )

    conta=st.selectbox("Forma de Pagamento",
        list(dict.fromkeys([sug_conta]+contas_existentes+["Carteira","Pix","Transferência","Nubank CNPJ","Nubank","Pagseguro","Mercado Pago"])))

    force_off=is_nao_cartao(conta)
    usar_cartao=st.checkbox("Tratar como cartão (com taxa)?",
        value=(False if force_off else default_card_flag(conta)),
        key="flag_card_um", disabled=force_off,
        help=("Desabilitado para PIX/Dinheiro/Transferência." if force_off else None))

    funcionario=st.selectbox("Funcionário", FUNCIONARIOS_FEM,
        index=(FUNCIONARIOS_FEM.index(sug_func) if sug_func in FUNCIONARIOS_FEM else 0))
    periodo_opcao=st.selectbox("Período do Atendimento", ["Manhã","Tarde","Noite"],
        index=["Manhã","Tarde","Noite"].index(sug_periodo))

    # % Daniela (só mostra quando selecionada)
    pct_daniela = None
    if funcionario=="Daniela":
        pct_daniela = st.number_input("Percentual da Daniela (%)", min_value=0.0, max_value=100.0, value=50.0, step=1.0)

    ultimo=df_existente[df_existente["Cliente"]==cliente]
    ultimo=ultimo.sort_values("Data", ascending=False).iloc[0] if not ultimo.empty else None
    combo=""
    if ultimo is not None:
        ult_combo=ultimo.get("Combo","")
        combo=st.selectbox("Combo (último primeiro)", [""]+list(dict.fromkeys([ult_combo]+combos_existentes)))

    def bloco_cartao_ui(total_bruto_padrao: float):
        with st.expander("💳 Pagamento no cartão (informe o LÍQUIDO recebido)", expanded=True):
            c1,c2=st.columns(2)
            with c1:
                liquido=st.number_input("Valor recebido (líquido)", value=float(total_bruto_padrao), step=1.0, format="%.2f")
                bandeira=st.selectbox("Bandeira", ["","Visa","Mastercard","Elo","Hipercard","Amex","Outros"], index=0)
            with c2:
                tipo_cartao=st.selectbox("Tipo", ["Débito","Crédito"], index=1)
                parcelas=st.number_input("Parcelas (se crédito)", min_value=1, max_value=12, value=1, step=1)
            taxa_val=max(0.0, float(total_bruto_padrao)-float(liquido or 0.0))
            taxa_pct=(taxa_val/float(total_bruto_padrao)*100.0) if total_bruto_padrao>0 else 0.0
            st.caption(f"Taxa estimada: {_fmt_brl(taxa_val)} ({taxa_pct:.2f}%)")
            return float(liquido or 0.0), str(bandeira), str(tipo_cartao), int(parcelas)

    if "combo_salvo" not in st.session_state: st.session_state.combo_salvo=False
    if "simples_salvo" not in st.session_state: st.session_state.simples_salvo=False
    if st.button("🧹 Limpar formulário"):
        st.session_state.combo_salvo=False; st.session_state.simples_salvo=False; st.rerun()

    # -------- COMBO
    if combo:
        st.subheader("💰 Edite os valores do combo antes de salvar:")
        valores_customizados={}
        for s in combo.split("+"):
            s2=s.strip()
            valores_customizados[s2]=st.number_input(f"{s2} (padrão: R$ {obter_valor_servico(s2)})",
                                                     value=obter_valor_servico(s2), step=1.0, key=f"valor_{s2}")

        liquido_total=None; bandeira=""; tipo_cartao="Crédito"; parcelas=1
        dist_modo="Proporcional (padrão)"; alvo_servico=None

        if usar_cartao and not is_nao_cartao(conta):
            with st.expander("💳 Pagamento no cartão (informe o LÍQUIDO recebido)", expanded=True):
                c1,c2=st.columns(2)
                with c1:
                    total_bruto_combo=float(sum(valores_customizados.values()))
                    liquido_total=st.number_input("Valor recebido (líquido)", value=total_bruto_combo, step=1.0, format="%.2f")
                    bandeira=st.selectbox("Bandeira", ["","Visa","Mastercard","Elo","Hipercard","Amex","Outros"], index=0)
                with c2:
                    tipo_cartao=st.selectbox("Tipo", ["Débito","Crédito"], index=1)
                    parcelas=st.number_input("Parcelas (se crédito)", min_value=1, max_value=12, value=1, step=1)
                dist_modo=st.radio("Distribuição do desconto/taxa", ["Proporcional (padrão)","Concentrar em um serviço"])
                if dist_modo=="Concentrar em um serviço":
                    alvo_servico=st.selectbox("Aplicar TODO o desconto/taxa em", list(valores_customizados.keys()))

        if not st.session_state.combo_salvo and st.button("✅ Confirmar e Salvar Combo"):
            duplicado=any(ja_existe_atendimento(cliente, data, _cap_first(s), combo) for s in combo.split("+"))
            if duplicado:
                st.warning("⚠️ Combo já registrado para este cliente e data.")
            else:
                df_all,_=carregar_base()
                novas=[]
                total_bruto=float(sum(valores_customizados.values()))
                usar_cartao_ef=usar_cartao and not is_nao_cartao(conta)
                id_pag=gerar_pag_id("A") if usar_cartao_ef else ""

                soma_outros=None
                if usar_cartao_ef and dist_modo=="Concentrar em um serviço" and alvo_servico:
                    soma_outros=sum(v for k,v in valores_customizados.items() if k!=alvo_servico)

                for s in combo.split("+"):
                    s_raw=s.strip(); s_norm=_cap_first(s_raw)
                    bruto_i=float(valores_customizados.get(s_raw, obter_valor_servico(s_norm)))
                    if usar_cartao_ef and total_bruto>0:
                        if dist_modo=="Concentrar em um serviço" and alvo_servico:
                            if s_raw==alvo_servico:
                                liq_i=float(liquido_total or 0.0)-float(soma_outros or 0.0); liq_i=round(max(0.0, liq_i),2)
                            else:
                                liq_i=round(bruto_i,2)
                        else:
                            liq_i=round(float(liquido_total or 0.0)*(bruto_i/total_bruto),2)
                        taxa_i=round(bruto_i-liq_i,2); taxa_pct_i=(taxa_i/bruto_i*100.0) if bruto_i>0 else 0.0
                        valor_para_base=liq_i
                        extras={"ValorBrutoRecebido":bruto_i,"ValorLiquidoRecebido":liq_i,
                                "TaxaCartaoValor":taxa_i,"TaxaCartaoPct":round(taxa_pct_i,4),
                                "FormaPagDetalhe":f"{bandeira or '-'} | {tipo_cartao} | {int(parcelas)}x",
                                "PagamentoID":id_pag}
                    else:
                        valor_para_base=bruto_i; extras={}
                    novas.append(_preencher_fiado_vazio({
                        "Data":data,"Serviço":s_norm,"Valor":valor_para_base,"Conta":conta,
                        "Cliente":cliente,"Combo":combo,"Funcionário":funcionario,
                        "Fase":"Dono + funcionário","Tipo":tipo,"Período":periodo_opcao, **extras
                    }))

                if usar_cartao_ef and novas:
                    soma_liq=sum(float(n.get("Valor",0) or 0) for n in novas)
                    delta=round(float(liquido_total or 0.0)-soma_liq,2)
                    if abs(delta)>=0.01:
                        idx=len(novas)-1
                        if dist_modo=="Concentrar em um serviço" and alvo_servico:
                            for i,n in enumerate(novas):
                                if _norm_key(n.get("Serviço",""))==_norm_key(_cap_first(alvo_servico)): idx=i; break
                        novas[idx]["Valor"]=float(novas[idx]["Valor"])+delta
                        bsel=float(novas[idx].get("ValorBrutoRecebido",0) or 0)
                        lsel=float(novas[idx]["Valor"])
                        tsel=round(bsel-lsel,2); psel=(tsel/bsel*100.0) if bsel>0 else 0.0
                        novas[idx]["ValorLiquidoRecebido"]=lsel
                        novas[idx]["TaxaCartaoValor"]=tsel
                        novas[idx]["TaxaCartaoPct"]=round(psel,4)

                df_final=pd.concat([df_all, pd.DataFrame(novas)], ignore_index=True)
                salvar_base(df_final)
                st.session_state.combo_salvo=True
                st.success(f"✅ Atendimento salvo com sucesso para {cliente} no dia {data}.")
                enviar_card(
                    df_final, cliente, funcionario, data,
                    servico=combo.replace("+"," + "),
                    valor=sum(float(n["Valor"]) for n in novas),
                    combo=combo, pct_daniela=pct_daniela
                )

    # -------- SIMPLES
    else:
        st.subheader("✂️ Selecione o serviço e valor:")
        servico=st.selectbox("Serviço", servicos_existentes)
        valor=st.number_input("Valor", value=obter_valor_servico(servico), step=1.0)

        if usar_cartao and not is_nao_cartao(conta):
            liquido_total,bandeira,tipo_cartao,parcelas=bloco_cartao_ui(valor)
        else:
            liquido_total,bandeira,tipo_cartao,parcelas=None,"","Crédito",1

        if not st.session_state.simples_salvo and st.button("📁 Salvar Atendimento"):
            servico_norm=_cap_first(servico)
            if ja_existe_atendimento(cliente, data, servico_norm):
                st.warning("⚠️ Atendimento já registrado para este cliente, data e serviço.")
            else:
                df_all,_=carregar_base()
                usar_cartao_ef=usar_cartao and not is_nao_cartao(conta)
                if usar_cartao_ef:
                    id_pag=gerar_pag_id("A")
                    bruto=float(valor); liq=float(liquido_total or 0.0)
                    taxa_v=round(max(0.0, bruto-liq),2)
                    taxa_pct=round((taxa_v/bruto*100.0),4) if bruto>0 else 0.0
                    nova=_preencher_fiado_vazio({
                        "Data":data,"Serviço":servico_norm,"Valor":liq,"Conta":conta,
                        "Cliente":cliente,"Combo":"","Funcionário":funcionario,
                        "Fase":fase,"Tipo":tipo,"Período":periodo_opcao,
                        "ValorBrutoRecebido":bruto,"ValorLiquidoRecebido":liq,
                        "TaxaCartaoValor":taxa_v,"TaxaCartaoPct":taxa_pct,
                        "FormaPagDetalhe":f"{bandeira or '-'} | {tipo_cartao} | {int(parcelas)}x",
                        "PagamentoID":id_pag
                    })
                else:
                    nova=_preencher_fiado_vazio({
                        "Data":data,"Serviço":servico_norm,"Valor":valor,"Conta":conta,
                        "Cliente":cliente,"Combo":"","Funcionário":funcionario,
                        "Fase":fase,"Tipo":tipo,"Período":periodo_opcao
                    })
                df_final=pd.concat([df_all, pd.DataFrame([nova])], ignore_index=True)
                salvar_base(df_final)
                st.session_state.simples_salvo=True
                st.success(f"✅ Atendimento salvo com sucesso para {cliente} no dia {data}.")
                enviar_card(df_final, cliente, funcionario, data,
                            servico=servico_norm, valor=float(nova["Valor"]), combo="", pct_daniela=pct_daniela)

# =========================
# MODO LOTE
# =========================
else:
    st.info("Defina por cliente; escolha forma de pagamento, período, funcionário e % da Daniela quando for o caso.")
    clientes_multi=st.multiselect("Clientes existentes", clientes_existentes)
    novos_nomes_raw=st.text_area("Ou cole novos nomes (um por linha)", value="")
    novos_nomes=[n.strip() for n in novos_nomes_raw.splitlines() if n.strip()]
    lista_final=list(dict.fromkeys(clientes_multi+novos_nomes))
    st.write(f"Total selecionados: **{len(lista_final)}**")

    enviar_cards=st.checkbox("Enviar card no Telegram após salvar", value=True)

    for cli in lista_final:
        with st.container(border=True):
            st.subheader(f"⚙️ Atendimento para {cli}")
            sug_conta,sug_periodo,sug_func=sugestoes_do_cliente(df_existente, cli, conta_global, periodo_global, funcionario_global)

            tipo_at=st.radio(f"Tipo de atendimento para {cli}", ["Simples","Combo"], horizontal=True, key=f"tipo_{cli}")

            st.selectbox(f"Forma de Pagamento de {cli}",
                list(dict.fromkeys([sug_conta]+contas_existentes+["Carteira","Pix","Transferência","Nubank CNPJ","Nubank","Pagseguro","Mercado Pago"])),
                key=f"conta_{cli}")

            force_off_cli=is_nao_cartao(st.session_state.get(f"conta_{cli}",""))
            st.checkbox(f"{cli} - Tratar como cartão (com taxa)?",
                        value=(False if force_off_cli else default_card_flag(st.session_state.get(f"conta_{cli}",""))),
                        key=f"flag_card_{cli}", disabled=force_off_cli)

            use_card_cli=(not force_off_cli) and bool(st.session_state.get(f"flag_card_{cli}", False))

            st.selectbox(f"Período do Atendimento de {cli}", ["Manhã","Tarde","Noite"],
                         index=["Manhã","Tarde","Noite"].index(sug_periodo), key=f"periodo_{cli}")

            st.selectbox(f"Funcionário de {cli}", FUNCIONARIOS_FEM,
                         index=(FUNCIONARIOS_FEM.index(sug_func) if sug_func in FUNCIONARIOS_FEM else 0),
                         key=f"func_{cli}")

            # % Daniela por cliente (aparece apenas quando escolhida)
            if st.session_state.get(f"func_{cli}", "")=="Daniela":
                st.number_input(f"{cli} - Percentual da Daniela (%)", min_value=0.0, max_value=100.0,
                                value=50.0, step=1.0, key=f"pct_dan_{cli}")

            if tipo_at=="Combo":
                st.selectbox(f"Combo para {cli} (formato corte+escova, etc.)", [""]+combos_existentes, key=f"combo_{cli}")
                combo_cli=st.session_state.get(f"combo_{cli}","")
                if combo_cli:
                    total_padrao=0.0; itens=[]
                    for s in combo_cli.split("+"):
                        s2=s.strip()
                        val=st.number_input(f"{cli} - {s2} (padrão: R$ {obter_valor_servico(s2)})",
                                            value=obter_valor_servico(s2), step=1.0, key=f"valor_{cli}_{s2}")
                        itens.append((s2,val)); total_padrao+=float(val)
                    if use_card_cli and not is_nao_cartao(st.session_state.get(f"conta_{cli}","")):
                        with st.expander(f"💳 {cli} - Pagamento no cartão", expanded=True):
                            c1,c2=st.columns(2)
                            with c1:
                                st.number_input(f"{cli} - Valor recebido (líquido)", value=float(total_padrao), step=1.0, key=f"liq_{cli}")
                                st.selectbox(f"{cli} - Bandeira", ["","Visa","Mastercard","Elo","Hipercard","Amex","Outros"], index=0, key=f"bandeira_{cli}")
                            with c2:
                                st.selectbox(f"{cli} - Tipo", ["Débito","Crédito"], index=1, key=f"tipo_cartao_{cli}")
                                st.number_input(f"{cli} - Parcelas", min_value=1, max_value=12, value=1, step=1, key=f"parc_{cli}")
                        st.radio(f"{cli} - Distribuição do desconto/taxa", ["Proporcional (padrão)","Concentrar em um serviço"],
                                 horizontal=False, key=f"dist_{cli}")
                        if st.session_state.get(f"dist_{cli}","Proporcional (padrão)")=="Concentrar em um serviço":
                            st.selectbox(f"{cli} - Aplicar TODO o desconto/taxa em", [nm for (nm,_) in itens], key=f"alvo_{cli}")
            else:
                st.selectbox(f"Serviço simples para {cli}", servicos_existentes, key=f"servico_{cli}")
                serv_cli=st.session_state.get(f"servico_{cli}", None)
                st.number_input(f"{cli} - Valor do serviço",
                                value=(obter_valor_servico(serv_cli) if serv_cli else 0.0),
                                step=1.0, key=f"valor_{cli}_simples")
                if use_card_cli and not is_nao_cartao(st.session_state.get(f"conta_{cli}","")):
                    with st.expander(f"💳 {cli} - Pagamento no cartão", expanded=True):
                        c1,c2=st.columns(2)
                        with c1:
                            st.number_input(f"{cli} - Valor recebido (líquido)", value=float(st.session_state.get(f"valor_{cli}_simples",0.0)), step=1.0, key=f"liq_{cli}")
                            st.selectbox(f"{cli} - Bandeira", ["","Visa","Mastercard","Elo","Hipercard","Amex","Outros"], index=0, key=f"bandeira_{cli}")
                        with c2:
                            st.selectbox(f"{cli} - Tipo", ["Débito","Crédito"], index=1, key=f"tipo_cartao_{cli}")
                            st.number_input(f"{cli} - Parcelas", min_value=1, max_value=12, value=1, step=1, key=f"parc_{cli}")

    if st.button("💾 Salvar TODOS atendimentos"):
        if not lista_final:
            st.warning("Selecione ou informe ao menos um cliente.")
        else:
            df_all,_=carregar_base()
            novas, clientes_salvos = [], set()
            funcionario_por_cliente, pct_por_cliente = {}, {}

            for cli in lista_final:
                tipo_at=st.session_state.get(f"tipo_{cli}","Simples")
                conta_cli=st.session_state.get(f"conta_{cli}", conta_global)
                use_card_cli=bool(st.session_state.get(f"flag_card_{cli}", False)) and not is_nao_cartao(conta_cli)
                periodo_cli=st.session_state.get(f"periodo_{cli}", periodo_global)
                func_cli=st.session_state.get(f"func_{cli}", funcionario_global)
                pct_cli=None
                if func_cli=="Daniela":
                    pct_cli=float(st.session_state.get(f"pct_dan_{cli}", 50.0))
                pct_por_cliente[cli]=pct_cli

                if tipo_at=="Combo":
                    combo_cli=st.session_state.get(f"combo_{cli}","")
                    if not combo_cli: st.warning(f"⚠️ {cli}: combo não definido. Pulando."); continue
                    if any(ja_existe_atendimento(cli, data, _cap_first(s), combo_cli) for s in str(combo_cli).split("+")):
                        st.warning(f"⚠️ {cli}: já existia COMBO em {data}. Pulando."); continue

                    itens=[]; total_bruto=0.0
                    for s in str(combo_cli).split("+"):
                        s_raw=s.strip(); s_norm=_cap_first(s_raw)
                        val=float(st.session_state.get(f"valor_{cli}_{s_raw}", obter_valor_servico(s_norm)))
                        itens.append((s_raw,s_norm,val)); total_bruto+=val

                    id_pag=gerar_pag_id("A") if use_card_cli else ""
                    liq_total_cli=float(st.session_state.get(f"liq_{cli}", total_bruto)) if use_card_cli else total_bruto
                    dist_modo=st.session_state.get(f"dist_{cli}","Proporcional (padrão)")
                    alvo=st.session_state.get(f"alvo_{cli}", None)
                    soma_outros=None
                    if use_card_cli and dist_modo=="Concentrar em um serviço" and alvo:
                        soma_outros=sum(val for (r,_,val) in itens if r!=alvo)

                    for (s_raw,s_norm,bruto_i) in itens:
                        if use_card_cli and total_bruto>0:
                            if dist_modo=="Concentrar em um serviço" and alvo:
                                if s_raw==alvo:
                                    liq_i=liq_total_cli-float(soma_outros or 0.0); liq_i=round(max(0.0, liq_i),2)
                                else:
                                    liq_i=round(bruto_i,2)
                            else:
                                liq_i=round(liq_total_cli*(bruto_i/total_bruto),2)
                            taxa_i=round(bruto_i-liq_i,2); taxa_pct_i=(taxa_i/bruto_i*100.0) if bruto_i>0 else 0.0
                            extras={"ValorBrutoRecebido":bruto_i,"ValorLiquidoRecebido":liq_i,
                                    "TaxaCartaoValor":taxa_i,"TaxaCartaoPct":round(taxa_pct_i,4),
                                    "FormaPagDetalhe":f"{st.session_state.get(f'bandeira_{cli}','-')} | {st.session_state.get(f'tipo_cartao_{cli}','Crédito')} | {int(st.session_state.get(f'parc_{cli}',1))}x",
                                    "PagamentoID":id_pag}
                            valor_para_base=liq_i
                        else:
                            extras={}; valor_para_base=bruto_i
                        novas.append(_preencher_fiado_vazio({
                            "Data":data,"Serviço":s_norm,"Valor":valor_para_base,"Conta":conta_cli,
                            "Cliente":cli,"Combo":combo_cli,"Funcionário":func_cli,"Fase":"Dono + funcionário",
                            "Tipo":tipo,"Período":periodo_cli, **extras
                        }))

                    if use_card_cli:
                        idxs=[i for i,n in enumerate(novas) if n["Cliente"]==cli and n["Combo"]==combo_cli]
                        soma_liq=sum(float(novas[i]["Valor"]) for i in idxs)
                        delta=round(liq_total_cli-soma_liq,2)
                        if abs(delta)>=0.01 and idxs:
                            idx=idxs[-1]
                            if dist_modo=="Concentrar em um serviço" and alvo:
                                for i in idxs:
                                    if _norm_key(novas[i]["Serviço"])==_norm_key(_cap_first(alvo)): idx=i; break
                            novas[idx]["Valor"]=float(novas[idx]["Valor"])+delta
                            bsel=float(novas[idx].get("ValorBrutoRecebido",0) or 0)
                            lsel=float(novas[idx]["Valor"])
                            tsel=round(bsel-lsel,2); psel=(tsel/bsel*100.0) if bsel>0 else 0.0
                            novas[idx]["ValorLiquidoRecebido"]=lsel
                            novas[idx]["TaxaCartaoValor"]=tsel
                            novas[idx]["TaxaCartaoPct"]=round(psel,4)

                    clientes_salvos.add(cli); funcionario_por_cliente[cli]=func_cli

                else:
                    serv_cli=st.session_state.get(f"servico_{cli}", None)
                    serv_norm=_cap_first(serv_cli) if serv_cli else ""
                    if not serv_norm: st.warning(f"⚠️ {cli}: serviço simples não definido. Pulando."); continue
                    if ja_existe_atendimento(cli, data, serv_norm):
                        st.warning(f"⚠️ {cli}: já existia atendimento simples ({serv_norm}) em {data}. Pulando."); continue
                    bruto=float(st.session_state.get(f"valor_{cli}_simples", obter_valor_servico(serv_norm)))
                    if use_card_cli:
                        liq=float(st.session_state.get(f"liq_{cli}", bruto))
                        taxa_v=round(max(0.0, bruto-liq),2)
                        taxa_pct=round((taxa_v/bruto*100.0),4) if bruto>0 else 0.0
                        novas.append(_preencher_fiado_vazio({
                            "Data":data,"Serviço":serv_norm,"Valor":liq,"Conta":conta_cli,
                            "Cliente":cli,"Combo":"","Funcionário":func_cli,"Fase":"Dono + funcionário","Tipo":tipo,"Período":periodo_cli,
                            "ValorBrutoRecebido":bruto,"ValorLiquidoRecebido":liq,
                            "TaxaCartaoValor":taxa_v,"TaxaCartaoPct":taxa_pct,
                            "FormaPagDetalhe":f"{st.session_state.get(f'bandeira_{cli}','-')} | {st.session_state.get(f'tipo_cartao_{cli}','Crédito')} | {int(st.session_state.get(f'parc_{cli}',1))}x",
                            "PagamentoID":gerar_pag_id("A")
                        }))
                    else:
                        novas.append(_preencher_fiado_vazio({
                            "Data":data,"Serviço":serv_norm,"Valor":bruto,"Conta":conta_cli,
                            "Cliente":cli,"Combo":"","Funcionário":func_cli,"Fase":"Dono + funcionário","Tipo":tipo,"Período":periodo_cli
                        }))
                    clientes_salvos.add(cli); funcionario_por_cliente[cli]=func_cli

            if not novas:
                st.warning("Nenhuma linha válida para inserir.")
            else:
                df_final=pd.concat([df_all, pd.DataFrame(novas)], ignore_index=True)
                salvar_base(df_final)
                st.success(f"✅ {len(novas)} linhas inseridas para {len(clientes_salvos)} cliente(s).")

                if enviar_cards:
                    for cli in sorted(clientes_salvos):
                        enviar_card(df_final, cli, funcionario_por_cliente.get(cli, FUNCIONARIOS_FEM[0]), data,
                                    pct_daniela=pct_por_cliente.get(cli))
