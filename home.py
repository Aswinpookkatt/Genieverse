import streamlit as st

def show_home():
    # Inject custom HTML/CSS for absolute-positioned logout
    st.markdown("""
        <style>
        .logout-button {
            position: absolute;
            top: -20px;
            right: -250px;
            z-index: 9999;
        }
        .logout-button form { display:inline; }
        .logout-btn {
            background-color: transparent;
            border: 1px solid #E43636;
            color: #E43636;
            padding: 8px 24px;
            font-size: 14px;
            border-radius: 6px;
            cursor: pointer;
            font-weight: normal;
            box-shadow: 1px 1px 4px rgba(0,0,0,0.11);
            transition: background 0.2s;
        }
        .logout-btn:hover {
            background-color: #E43636;
            color: #ffffff;
        }
        </style>
        <div class="logout-button">
            <form action="" method="post">
                <button name="logout" class="logout-btn" type="submit">Logout</button>
            </form>
        </div>
    """, unsafe_allow_html=True)

    # Main app content
    st.title("Welcome to Genieverse Home!")
    st.write("This is the secured homepage. You are logged in.")

    #Check for POST to detect logout button submit
    if st.session_state.get('authenticated', False):
        if 'logout' in st.experimental_get_query_params():
            st.session_state['authenticated'] = False
            st.rerun()

    # Fallback: Streamlit can't natively "catch" HTML form POST, so also offer:
    if st.button("Logout (for accessibility)", key="sidebar_logout"):
        st.session_state['authenticated'] = False
        st.rerun()