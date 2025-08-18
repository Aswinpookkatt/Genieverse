import streamlit as st
import duckdb
import hashlib
import home  

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

# --- Restore login state from query params ---
params = st.query_params
if params.get("auth") == "true":
    st.session_state["authenticated"] = True
    if "user" in params:
        st.session_state["username"] = params["user"]
else:
    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False

# Debug
#st.sidebar.write("DEBUG: Authenticated =", st.session_state.get("authenticated", None))

# Unified logic
if st.session_state["authenticated"]:
    # keep query param set so refresh stays logged in
    st.query_params.update({"auth": "true", "user": st.session_state.get("username", "")})
    home.show_home()
    st.stop()
else:
    st.title("Login to Genieverse")

    with st.form("login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Login")

        if submitted:
            # Authenticate against DB
            conn = duckdb.connect("./data/users_db.duckdb")
            pw_hash = hash_password(password)
            user = conn.execute(
                "SELECT * FROM users WHERE username=? AND password_hash=?",
                (username, pw_hash)
            ).fetchone()
            conn.close()
            if user:
                st.session_state["authenticated"] = True
                st.session_state["username"] = username
                st.query_params.update({"auth": "true", "user": username})  # ✅ persist login
                st.success("Login successful! Redirecting...")
                st.rerun()
            else:
                st.error("Invalid username or password")
