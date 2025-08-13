import streamlit as st
import pandas as pd
import cloudinary
import cloudinary.uploader
import cloudinary.api
import gspread
from io import BytesIO
from PIL import Image
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Upload Imagem Cliente (Feminino)")
st.markdown("<h1 style='text-align:center'>📸 Upload Imagem Cliente — Feminino</h1>", unsafe_allow_html=True)

# =============== CONFIGURAR CLOUDINARY ===============
cloudinary.config(
    cloud_name=st.secrets["CLOUDINARY"]["cloud_name"],
    api_key=st.secrets["CLOUDINARY"]["api_key"],
    api_secret=st.secrets["CLOUDINARY"]["api_secret"]
)

PASTA_CLOUD = "Fotos clientes"   # ajuste se quiser separar por pasta

# =============== CONECTAR À PLANILHA (FEMININO) =================
def carregar_clientes_status_feminino():
    creds = Credentials.from_service_account_info(
        st.secrets["GCP_SERVICE_ACCOUNT"],
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_url(st.secrets["PLANILHA_URL"])
    aba = spreadsheet.worksheet("clientes_status_feminino")  # <<< FEMININO
    dados = aba.get_all_records()
    return pd.DataFrame(dados), aba

df_status, aba_status = carregar_clientes_status_feminino()
df_status.columns = df_status.columns.str.strip()

# Valida colunas essenciais
if 'Cliente' not in df_status.columns:
    st.error("A coluna 'Cliente' não foi encontrada na aba **clientes_status_feminino**.")
    st.stop()
if 'Foto' not in df_status.columns:
    st.error("A coluna 'Foto' não foi encontrada na aba **clientes_status_feminino**.")
    st.stop()

# Helpers de normalização
def nrm(s: str) -> str:
    return str(s).strip().lower()

# Remove clientes vazios e prepara lista
df_status["ClienteKey"] = df_status["Cliente"].map(nrm)
nomes_clientes = sorted([c for c in df_status["Cliente"].dropna().astype(str).str.strip().unique() if c])

# =============== SELEÇÃO DO CLIENTE ===============
nome_cliente = st.selectbox("👩 Selecione a cliente", nomes_clientes, placeholder="Digite para buscar...")

# public_id “bonito” (sem extensão)
public_id = nome_cliente.strip().lower().replace(" ", "_")
public_id_path = f"{PASTA_CLOUD}/{public_id}"  # usado em resource/destroy/upload

# =============== VERIFICAR SE IMAGEM EXISTE ===============
def imagem_existe_e_url():
    # 1) Tenta Cloudinary pelo public_id (sem .jpg)
    try:
        resp = cloudinary.api.resource(public_id_path)
        return True, resp.get("secure_url")
    except Exception:
        # 2) Fallback: link na coluna Foto da planilha
        row = df_status[df_status["ClienteKey"] == nrm(nome_cliente)]
        if not row.empty:
            url = str(row.iloc[0]["Foto"]).strip()
            if url:
                if "drive.google.com" in url and "id=" in url:  # converte para link direto
                    file_id = url.split("id=")[-1].split("&")[0]
                    url = f"https://drive.google.com/uc?id={file_id}"
                return True, url
        return False, None

existe, url_existente = imagem_existe_e_url()

# =============== MOSTRAR IMAGEM SE EXISTIR ===============
if existe and url_existente:
    st.image(url_existente, width=250, caption=f"Imagem atual: {nome_cliente}")
    st.warning("Esta cliente já possui uma imagem cadastrada.")
else:
    st.info("Esta cliente ainda não possui imagem cadastrada.")

# =============== UPLOAD DE NOVA IMAGEM ===============
arquivo = st.file_uploader("📤 Envie a nova imagem", type=["jpg", "jpeg", "png"])

if arquivo is not None:
    if existe and not st.checkbox("Confirmo que desejo substituir a imagem existente."):
        st.stop()

    if st.button("Enviar imagem agora"):
        try:
            # Faz upload sobrescrevendo o public_id
            up = cloudinary.uploader.upload(
                arquivo,
                folder=PASTA_CLOUD,
                public_id=public_id,
                overwrite=True,
                resource_type="image"
            )
            url_nova = up["secure_url"]

            # Localiza linha (case-insensitive) e atualiza a coluna Foto
            mask = df_status["ClienteKey"] == nrm(nome_cliente)
            if not mask.any():
                st.error("Cliente não encontrado na planilha ao salvar o link da foto.")
            else:
                # índice visual da planilha (linha 1 = cabeçalho)
                idx0 = df_status.index[mask][0]      # 0-based no DataFrame
                linha_planilha = idx0 + 2            # +2 por causa do header

                col_foto = df_status.columns.get_loc("Foto") + 1  # 1-based
                aba_status.update_cell(linha_planilha, col_foto, url_nova)

                st.success("✅ Imagem enviada e link salvo na planilha!")
                st.image(url_nova, width=300)

        except Exception as e:
            st.error(f"Erro ao enviar imagem: {e}")

# =============== BOTÃO DELETAR ===============
if existe and st.button("🗑️ Deletar imagem"):
    try:
        # 1) Remove do Cloudinary (se existir lá)
        try:
            cloudinary.uploader.destroy(public_id_path, resource_type="image")
            st.success("Imagem deletada do Cloudinary com sucesso.")
        except Exception:
            # se não existir no cloudinary, segue para limpar planilha
            pass

        # 2) Limpa link na planilha (case-insensitive)
        mask = df_status["ClienteKey"] == nrm(nome_cliente)
        if mask.any():
            idx0 = df_status.index[mask][0]
            linha_planilha = idx0 + 2
            col_foto = df_status.columns.get_loc("Foto") + 1
            aba_status.update_cell(linha_planilha, col_foto, "")
            st.success("Link da imagem removido da planilha com sucesso.")
        else:
            st.warning("Cliente não encontrado na planilha para limpar o link.")

        st.experimental_rerun()
    except Exception as e:
        st.error(f"Erro ao deletar imagem: {e}")

# =============== GALERIA ===============
st.markdown("---")
st.subheader("🖼️ Galeria de imagens salvas (Feminino)")

colunas = st.columns(5)
contador = 0

for nome in nomes_clientes:
    pid = nome.strip().lower().replace(" ", "_")
    pid_path = f"{PASTA_CLOUD}/{pid}"

    url = None
    # Tenta Cloudinary primeiro
    try:
        resp = cloudinary.api.resource(pid_path)
        url = resp.get("secure_url")
    except Exception:
        # Fallback: pega link da coluna Foto
        row = df_status[df_status["ClienteKey"] == nrm(nome)]
        if not row.empty:
            url = str(row.iloc[0]["Foto"]).strip()
            if url and "drive.google.com" in url and "id=" in url:
                file_id = url.split("id=")[-1].split("&")[0]
                url = f"https://drive.google.com/uc?id={file_id}"

    if url:
        with colunas[contador % 5]:
            st.image(url, width=110, caption=nome)
        contador += 1
