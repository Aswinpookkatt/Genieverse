import streamlit as st
import app,base64  # ✅ import app.py as module (make sure app.py is in same folder)
from data_scanner.profiler_page import show_data_profiler
from connection_settings import show_connection_settings

def get_base64_image(image_path):
    with open(image_path, "rb") as f:
        return base64.b64encode(f.read()).decode()

logo_base64 = get_base64_image("static/genie.png")

def restore_connection_state():
    """Restore connection state from query parameters"""
    params = st.query_params
    
    if params.get("db_connected") == "true" and "connection" not in st.session_state:
        # Restore connection state from query params
        st.session_state.connection = {
            "active": True,
            "type": params.get("db_type", "DuckDB"),
            "details": {
                "database": params.get("db_path", "./data/amazon.duckdb")
            },
            "last_connected": params.get("db_connected_at", ""),
            "connection_obj": None
        }
    elif "connection" not in st.session_state:
        # Initialize default connection state
        st.session_state.connection = {
            "active": False,
            "type": None,
            "details": {},
            "last_connected": None,
            "connection_obj": None
        }

def show_home():
    # Restore connection state on page load
    restore_connection_state()
    
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
        # Handle redirect to connection settings
        if hasattr(st.session_state, 'redirect_to_connection') and st.session_state.redirect_to_connection:
            page = "Weave Connections"
            st.session_state.redirect_to_connection = False
        else:
            page = st.radio("Choose Action 🕹️ ", ["Chat with Genie", "Genie Profiler","Weave Connections"])

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

    # --- Page Navigation ---
    if page == "Chat with Genie":
        # Check if database is connected before allowing chat
        if hasattr(st.session_state, 'connection') and st.session_state.connection.get("active"):
            # --- Embed Chat Assistant from app.py ---
            app.main()
        else:
            st.warning("🔴 Database Not Connected")
            st.info("Please connect to a database in 'Weave Connections' before using the chat feature.")
                    
    elif page == "Genie Profiler":
        # Check if database is connected before allowing profiling
        if hasattr(st.session_state, 'connection') and st.session_state.connection.get("active"):
            show_data_profiler()
        else:
            st.warning("🔴 Database Not Connected")
            st.info("Please connect to a database first to use the Genie Profiler.")
            
    elif page == "Weave Connections":
        show_connection_settings()
