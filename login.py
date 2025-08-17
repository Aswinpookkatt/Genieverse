import streamlit as st
import duckdb
import hashlib

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

if 'authenticated' not in st.session_state:
    st.session_state['authenticated'] = False

if not st.session_state['authenticated']:
    st.title("Login to Genieverse")

    with st.form("login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Login")

        if submitted:
            # Authenticate against DB
            conn = duckdb.connect("users_db.duckdb")
            pw_hash = hash_password(password)
            user = conn.execute(
                "SELECT * FROM users WHERE username=? AND password_hash=?",
                (username, pw_hash)
            ).fetchone()
            conn.close()
            if user:
                st.session_state['authenticated'] = True
                st.success("Login successful! Redirecting...")
                st.rerun()
            else:
                st.error("Invalid username or password")
else:
    # After authentication, render home page
    import home
    home.show_home()  # You'll define this function in home.py

# --- End of login.py ---
