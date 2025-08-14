@st.cache_data(ttl=300, show_spinner=True)
def carregar_dados():
    """
    1) Tenta via Service Account (GCP_SERVICE_ACCOUNT ou gcp_service_account)
    2) Fallback: CSV público da aba feminina
    3) Normaliza/renomeia colunas p/ garantir: Data, Cliente, Valor
    """
    # ---------- Tentativa 1: Service Account ----------
    try:
        sa_info = st.secrets.get("GCP_SERVICE_ACCOUNT") or st.secrets.get("gcp_service_account")
        if not sa_info:
            raise KeyError("Service account não encontrado em st.secrets")
        creds = Credentials.from_service_account_info(
            sa_info,
            scopes=["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"],
        )
        client = gspread.authorize(creds)
        ws = client.open_by_key(SHEET_ID).worksheet(ABA_FEMININO)
        df = get_as_dataframe(ws, evaluate_formulas=False).dropna(how="all")
        fonte = "service_account"
    except Exception:
        # ---------- Fallback CSV (planilha deve estar pública p/ leitura) ----------
        url_csv = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID_FEMININO}"
        df = pd.read_csv(url_csv)
        fonte = "csv"

    # ------- Normaliza cabeçalhos -------
    df.columns = [str(c).strip() for c in df.columns]

    # Mapeia aliases comuns -> oficial
    aliases = {
        "valor (r$)": "Valor",
        "valor r$": "Valor",
        "preço": "Valor",
        "preco": "Valor",
        "price": "Valor",
        "nome": "Cliente",
        "cliente(a)": "Cliente",
        "data do atendimento": "Data",
        "dt": "Data",
    }
    # renomeia pelos aliases existentes
    rename_map = {}
    lower_map = {c.lower(): c for c in df.columns}
    for k_lower, target in aliases.items():
        if k_lower in lower_map and target not in df.columns:
            rename_map[lower_map[k_lower]] = target
    if rename_map:
        df = df.rename(columns=rename_map)

    # Garante as 3 mínimas (sem travar o app)
    if "Data" not in df.columns:
        st.warning("Coluna 'Data' não encontrada — criando vazia.")
        df["Data"] = pd.NaT
    if "Cliente" not in df.columns:
        st.warning("Coluna 'Cliente' não encontrada — criando vazia.")
        df["Cliente"] = ""
    if "Valor" not in df.columns:
        st.warning("Coluna 'Valor' não encontrada — criando com 0.0 (renomeie na planilha para melhorar).")
        df["Valor"] = 0.0

    # Tipos básicos
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce", dayfirst=True)
    df = df.dropna(subset=["Data"]).copy()
    df["Ano"] = df["Data"].dt.year

    if "Conta" in df.columns:
        df["Conta"] = df["Conta"].astype(str).str.strip()
    else:
        df["Conta"] = "Indefinido"

    df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce").fillna(0.0)

    return df, fonte
