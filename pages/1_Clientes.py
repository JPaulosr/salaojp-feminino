# =============================
# 🔄 Status automático (execução automática 1x por sessão)
# =============================
st.markdown("### 🔄 Status automático de clientes (90 dias)")

# roda apenas 1x por sessão para evitar reprocesso/loops
if "_status_auto_ok_fem" not in st.session_state:
    st.session_state["_status_auto_ok_fem"] = False

if not st.session_state["_status_auto_ok_fem"]:
    try:
        # Base completa para cálculo do último atendimento (sem filtro de ano)
        df_full = df.copy()
        hoje = pd.Timestamp.today().normalize()

        ultimos = df_full.groupby("Cliente")["Data"].max().reset_index()
        ultimos["DiasDesde"] = (hoje - ultimos["Data"]).dt.days
        ultimos["StatusNovo"] = ultimos["DiasDesde"].apply(lambda x: "Inativo" if x > 90 else "Ativo")

        # NÃO mexer nos "Ignorado"
        ignorados_set = set()
        if not df_status.empty and "Status" in df_status.columns and "Cliente" in df_status.columns:
            ignorados_set = set(
                df_status.loc[df_status["Status"].str.lower().eq("ignorado"), "Cliente"].astype(str)
            )

        # mapa normalizado (pula ignorados)
        status_map_norm = {}
        for _, r in ultimos.iterrows():
            if r["Cliente"] in ignorados_set:
                continue
            status_map_norm[norm(r["Cliente"])] = r["StatusNovo"]

        # aplica atualização em lote
        alterados = atualizar_status_clientes_batch(status_map_norm)

        # se houve alteração, limpa caches e recarrega status
        if alterados > 0:
            st.cache_data.clear()
            df_status = carregar_status_df()
            st.success(f"Status atualizado automaticamente ({alterados} linha(s) alterada(s)).")
        else:
            st.info("Status já estava atualizado (ou clientes marcados como 'Ignorado').")

        # marca como concluído nesta sessão
        st.session_state["_status_auto_ok_fem"] = True

    except Exception as e:
        st.warning(f"Não foi possível atualizar status automaticamente agora: {e}")
        # mesmo com erro, evita loop — marque como feito para não tentar a cada rerun
        st.session_state["_status_auto_ok_fem"] = True
