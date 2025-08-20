# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import urllib.parse
import unicodedata

st.set_page_config(page_title="Clientes sem Foto (Feminino)", page_icon="🖼", layout="wide")
st.title("🖼 Clientes sem Foto — Feminino")

# CONFIG
SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"   # planilha principal
ABA_STATUS = "clientes_status_feminino"                    # aba feminina

FOTO_COL_CANDIDATES = ["foto", "link_foto", "imagem", "url_foto", "foto_link", "link", "image", "foto_url"]

def norm(s):
    if not isinstance(s, str): return ""
    return "".join(ch for ch in unicodedata.normalize("NFKD", s.strip().lower())
                   if not unicodedata.combining(ch))

@st.cache_data(ttl=300)
def carregar_clientes_status(sheet_id: str, aba: str) -> pd.DataFrame:
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={urllib.parse.quote(aba)}"
    df = pd.read_csv(url, dtype=str)
    df.columns = [c.strip() for c in df.columns]
    return df

def descobrir_coluna_foto(cols):
    cand_norm = {norm(c) for c in FOTO_COL_CANDIDATES}
    for c in cols:
        if norm(c) in cand_norm:
            return c
    # fallback comum
    if "Foto" in cols: return "Foto"
    return None

df_status = carregar_clientes_status(SHEET_ID, ABA_STATUS)

# Descobre colunas
nome_col   = next((c for c in df_status.columns if norm(c) in ("cliente", "nome", "nome_cliente")), None)
foto_col   = descobrir_coluna_foto(df_status.columns)
status_col = next((c for c in df_status.columns if norm(c) == "status"), None)

if not nome_col:
    st.error("Não encontrei a coluna de nome (ex.: 'Cliente' ou 'Nome') na aba feminina.")
elif not foto_col:
    st.error("Não encontrei a coluna de foto (ex.: 'Foto', 'link_foto', 'imagem'...) na aba feminina.")
else:
    df = df_status.copy()
    df = df.rename(columns={nome_col: "Cliente", foto_col: "Foto"})
    if status_col: df = df.rename(columns={status_col: "Status"})
    else: df["Status"] = ""

    # Regra: sem foto se vazio/NaN/None ou link sem http(s)
    def sem_foto_fn(x):
        if not isinstance(x, str): return True
        s = x.strip()
        if s == "" or s.lower() in ("nan", "none", "null"): return True
        return not s.startswith(("http://", "https://"))

    df["SemFoto"] = df["Foto"].apply(sem_foto_fn)

    apenas_ativos = st.toggle("Mostrar apenas ativos", value=True)
    if apenas_ativos and "Status" in df.columns:
        df = df[df["Status"].astype(str).str.strip().str.lower().isin(["ativo", "ativa", "1", "true", "sim"])]

    faltantes = df[df["SemFoto"]].copy().drop(columns=["SemFoto"])

    c1, c2, c3 = st.columns(3)
    c1.metric("Total de clientes (fem.)", len(df))
    c2.metric("Sem foto", len(faltantes))
    c3.metric("Com foto", len(df) - len(faltantes))

    if faltantes.empty:
        st.success("✅ Todas as clientes do feminino possuem foto cadastrada.")
    else:
        q = st.text_input("🔎 Buscar cliente", "")
        if q:
            qn = norm(q)
            faltantes = faltantes[faltantes["Cliente"].astype(str).apply(norm).str.contains(qn)]

        st.warning(f"⚠ {len(faltantes)} cliente(s) sem foto cadastrada:")
        st.dataframe(faltantes[["Cliente", "Status"]], use_container_width=True, hide_index=True)

        st.download_button(
            "⬇️ Baixar CSV dos faltantes",
            faltantes[["Cliente", "Status"]].to_csv(index=False).encode("utf-8-sig"),
            file_name="clientes_feminino_sem_foto.csv",
            mime="text/csv"
        )
