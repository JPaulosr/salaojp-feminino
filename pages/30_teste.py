                    # ======== Adicionar serviços/combos da Base (picker) ========
                    # Catálogo vindo da Base
                    _servs_all, _combs_all = servicos_e_combos()

                    st.markdown("##### Adicionar da Base")
                    c_add1, c_add2, c_add3, c_add4 = st.columns([2, 1, 1, 1])

                    # Escolhe serviço; sugere preço; adiciona
                    serv_sel = c_add1.selectbox(
                        "Serviço da Base",
                        options=["(selecionar)"] + _servs_all,
                        index=0,
                        key=f"add_serv_sel_{ida}"
                    )

                    # Sugestão automática ao trocar o serviço
                    def _preco_padrao():
                        if serv_sel and serv_sel != "(selecionar)":
                            p = preco_sugerido(serv_sel)
                            if p is not None:
                                return float(p)
                        return 0.0

                    preco_input = c_add2.number_input(
                        "Valor (R$)",
                        min_value=0.0, step=0.5,
                        value=_preco_padrao(),
                        key=f"add_serv_preco_{ida}"
                    )

                    add_serv_btn = c_add3.button("➕ Adicionar serviço", key=f"add_serv_btn_{ida}")

                    # Adicionar um combo inteiro (explode em linhas)
                    combo_sel = c_add4.selectbox(
                        "Combo",
                        options=["(opcional)"] + _combs_all,
                        index=0,
                        key=f"add_combo_sel_{ida}"
                    )
                    add_combo_btn = st.button("➕ Adicionar itens do combo", key=f"add_combo_btn_{ida}")

                    # Lógica de adicionar serviço único
                    if add_serv_btn and serv_sel and serv_sel != "(selecionar)":
                        df_temp = st.session_state[key_items].copy()
                        # se o usuário não ajustou o preço manualmente, tenta sugerir aqui também
                        val = float(preco_input if preco_input is not None else _preco_padrao())
                        novo_row = {"Serviço": serv_sel, "Valor (R$)": val}
                        df_temp = pd.concat([df_temp, pd.DataFrame([novo_row])], ignore_index=True)
                        st.session_state[key_items] = df_temp
                        st.success(f"Serviço '{serv_sel}' adicionado.")

                    # Lógica de adicionar itens de um combo
                    if add_combo_btn and combo_sel and combo_sel != "(opcional)":
                        # itens do combo são separados por "+" (mesmo padrão da tela de agendar)
                        raw = [x.strip() for x in combo_sel.split("+") if x.strip()]
                        novos = []
                        for s in raw:
                            nome = s[:1].upper() + s[1:]
                            sug = preco_sugerido(nome)
                            novos.append({"Serviço": nome, "Valor (R$)": 0.0 if sug is None else float(sug)})
                        if novos:
                            df_temp = st.session_state[key_items].copy()
                            df_temp = pd.concat([df_temp, pd.DataFrame(novos)], ignore_index=True)
                            st.session_state[key_items] = df_temp
                            st.success(f"{len(novos)} item(ns) do combo adicionados.")

                    # Mostra um total atualizado (só visual)
                    try:
                        _tot = pd.to_numeric(st.session_state[key_items]["Valor (R$)"], errors="coerce").fillna(0.0).sum()
                        st.caption(f"**Total atual dos itens**: R$ {float(_tot):.2f}".replace(".", ","))
                    except Exception:
                        pass
