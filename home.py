import streamlit as st
import app  # ✅ import app.py as module (make sure app.py is in same folder)

def show_home():
    username = st.session_state.get("username", "Guest")
    #st.sidebar.write("DEBUG: Authenticated =", st.session_state.get("authenticated", None))

        # --- Sidebar ---
    with st.sidebar:
        st.markdown(f"### 👋 Welcome, {username}")
        # Navigation
        page = st.radio("📂 Navigate", ["Chat with Genie", "View Profile","Connection Settings"])

        st.markdown("---")
        if st.button("🚪 Logout", key="logout_sidebar"):
            st.session_state["authenticated"] = False
            st.session_state.pop("username", None)
            st.query_params.clear()
            st.rerun()

    # --- Logout button ---
    col1, col2, col3 = st.columns([4, 1, 1])
    with col3:
        if st.button("Logout", key="logout_button", type="secondary", help="Click to logout"):
            st.session_state["authenticated"] = False
            st.session_state.pop("username", None)
            st.query_params.clear()   # ✅ clear query params
            st.rerun()

    # --- Home Page Header ---
    # st.title(f"Welcome to Genieverse Home, {username}!")
    # st.write("This is the secured homepage. You are logged in.")
    # st.markdown("---")

    # --- Embed Chat Assistant from app.py ---
    app.main()
