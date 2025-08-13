# pages/upload_foto_feminino.py
import streamlit as st
import pandas as pd
import cloudinary
import cloudinary.uploader
import cloudinary.api
import gspread
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Upload Imagem Cliente (Feminino)")
st.markdown("<h1 style='text-align:center'>📸 Upload Imagem Cliente — Feminino</h1>", unsafe_allow_html=True)

# ====== Cloudinary ======
cloudinary.config(
    cloud_name=st.secrets["CLOUDINARY"]["cloud_name"],
    api_key=st.secrets["CLOUDINARY"]["api_key"],
    api_secret=st.secrets["CLOUDINARY"]["api_secret"],
)
PASTA_CLOUD = "Salao feminino"  # ← nova pasta

# ====== Planilha (aba feminina) ======
def carregar_clientes_status_feminino():
    creds = Credentials.from_service_account_info(
        st.secrets["GCP_SERVICE_ACCOUNT"],
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_url(st.secrets["PLANILHA_URL"])
    aba = spreadsheet.worksheet("clientes_status_feminino")
    dados = aba.get_all_records()
    return pd.DataFrame(dados), aba

df_status, aba_status = carregar_clientes_status_feminino()
df_status.columns = df_status.columns.str.strip()

if "Cliente" not in df_status.columns or "Foto" not in df_status.columns:
    st.error("A aba 'clientes_status_feminino' precisa ter as colunas 'Cliente' e 'Foto'.")
    st.stop()

def nrm(s: str) -> str:
    return str(s).strip().lower()

df_status["ClienteKey"] = df_status["Cliente"].map(nrm)
nomes_clientes = sorted([c for c in df_status["Cliente"].dropna().astype(str).str.strip().unique() if c])

# ====== Seleção ======
nome_cliente = st.selectbox("👩 Selecione a cliente", nomes_clientes, placeholder="Digite para buscar...")
public_id = nome_cliente.strip().lower().replace(" ", "_")
pid_path = f"{PASTA_CLOUD}/{public_id}"

# ====== Ver imagem existente ======
def imagem_existe_e_url():
    try:
        resp = cloudinary.api.resource(pid_path)
        return True, resp.get("secure_url")
    except Exception:
        row = df_status[df_status["ClienteKey"] == nrm(nome_cliente)]
        if not row.empty:
            url = str(row.iloc[0]["Foto"]).strip()
            if url:
                if "drive.google.com" in url and "id=" in url:
                    file_id = url.split("id=")[-1].split("&")[0]
                    url = f"https://drive.google.com/uc?id={file_id}"
                return True, url
        return False, None

existe, url_existente = imagem_existe_e_url()

if existe and url_existente:
    st.image(url_existente, width=250, caption=f"Imagem atual — {nome_cliente}")
    st.warning("Esta cliente já possui imagem.")
else:
    st.info("Esta cliente ainda não possui imagem cadastrada.")

# ====== Upload ======
arquivo = st.file_uploader("📤 Envie a nova imagem", type=["jpg", "jpeg", "png"])

if arquivo is not None:
    if existe and not st.checkbox("Confirmo que desejo substituir a imagem existente."):
        st.stop()
    if st.button("Enviar imagem agora"):
        try:
            up = cloudinary.uploader.upload(
                arquivo,
                folder=PASTA_CLOUD,
                public_id=public_id,
                overwrite=True,
                resource_type="image",
            )
            url_nova = up["secure_url"]
            mask = df_status["ClienteKey"] == nrm(nome_cliente)
            if not mask.any():
                st.error("Cliente não encontrado na planilha ao salvar o link da foto.")
            else:
                idx0 = df_status.index[mask][0]   # 0-based
                linha = idx0 + 2                  # +2 por causa do cabeçalho
                col_foto = df_status.columns.get_loc("Foto") + 1
                aba_status.update_cell(linha, col_foto, url_nova)
                st.success("✅ Imagem enviada e link salvo na planilha!")
                st.image(url_nova, width=300)
        except Exception as e:
            st.error(f"Erro ao enviar imagem: {e}")

# ====== Deletar ======
if existe and st.button("🗑️ Deletar imagem"):
    try:
        try:
            cloudinary.uploader.destroy(pid_path, resource_type="image")
            st.success("Imagem deletada do Cloudinary.")
        except Exception:
            pass
        mask = df_status["ClienteKey"] == nrm(nome_cliente)
        if mask.any():
            idx0 = df_status.index[mask][0]
            linha = idx0 + 2
            col_foto = df_status.columns.get_loc("Foto") + 1
            aba_status.update_cell(linha, col_foto, "")
            st.success("Link removido da planilha.")
        else:
            st.warning("Cliente não encontrado na planilha para limpar o link.")
        st.experimental_rerun()
    except Exception as e:
        st.error(f"Erro ao deletar imagem: {e}")

# ====== Galeria ======
st.markdown("---")
st.subheader("🖼️ Galeria — Salão feminino")

cols = st.columns(5)
i = 0
for nome in nomes_clientes:
    pid = nome.strip().lower().replace(" ", "_")
    path = f"{PASTA_CLOUD}/{pid}"
    url = None
    try:
        resp = cloudinary.api.resource(path)
        url = resp.get("secure_url")
    except Exception:
        row = df_status[df_status["ClienteKey"] == nrm(nome)]
        if not row.empty:
            url = str(row.iloc[0]["Foto"]).strip()
            if url and "drive.google.com" in url and "id=" in url:
                file_id = url.split("id=")[-1].split("&")[0]
                url = f"https://drive.google.com/uc?id={file_id}"
    if url:
        with cols[i % 5]:
            st.image(url, width=110, caption=nome)
        i += 1
