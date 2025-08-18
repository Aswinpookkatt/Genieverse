import streamlit as st
import app,base64  # ✅ import app.py as module (make sure app.py is in same folder)

def get_base64_image(image_path):
    with open(image_path, "rb") as f:
        return base64.b64encode(f.read()).decode()

logo_base64 = get_base64_image("static/genie.png")

def show_home():
    username = st.session_state.get("username", "Guest")
    #st.sidebar.write("DEBUG: Authenticated =", st.session_state.get("authenticated", None))

        # --- Sidebar ---
    with st.sidebar:
        
        st.markdown(
        f"""
        <div style="display: flex; align-items: center; margin-bottom:30px;">
            <img src="data:image/png;base64,{logo_base64}" width="65" style="margin-right:12px;">
            <h1 style="margin:0; font-size: 1.8em;">Genieverse</h1>
        </div>
        
        """,
        unsafe_allow_html=True
         )
       
        #st.markdown(f"### 👋 Welcome, {username}")
        # st.markdown(
        #     "<h3 style='font-size:20px; margin-top:30px;'>Options ⚙️ </h3>",
        #     unsafe_allow_html=True
        # )
        # st.markdown("""
        #     <style>
        #     div[data-baseweb="radio"] > div {
        #         font-size: 8px;   /* adjust size (12px–14px looks clean) */
        #     }
        #     </style>
        # """, unsafe_allow_html=True)


        # Navigation
        page = st.radio("Choose Action 🕹️ ", ["Chat with Genie", "Data Profiler","Connection Settings"])

        st.markdown("---")


        # create 3 columns, put button in the middle one
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            if st.button("Logout", key="logout_button"):
                st.session_state["authenticated"] = False
                st.session_state.pop("username", None)
                st.query_params.clear()
                st.rerun()





    # --- Home Page Header ---
    # st.title(f"Welcome to Genieverse Home, {username}!")
    # st.write("This is the secured homepage. You are logged in.")
    # st.markdown("---")

    # --- Embed Chat Assistant from app.py ---
    app.main()
