
import toml
import pandas as pd
import duckdb
import sqlite3
import streamlit as st

# Load config once
_cfg = toml.load("config.toml")
_conn = None
_schema_cache = None    # ← Add a module-level cache

# ---------------------------- Dynamic Database Connection ----------------------------

def get_connection():
    """Get database connection based on session state or config"""
    global _conn
    
    # Check if we have an active connection in session state
    if hasattr(st.session_state, 'connection') and st.session_state.connection.get("active"):
        conn_info = st.session_state.connection
        db_type = conn_info["type"]
        details = conn_info["details"]
        
        try:
            if db_type == "DuckDB":
                db_path = details.get("database", ":memory:")
                _conn = duckdb.connect(database=db_path)
            elif db_type == "SQLite":
                db_path = details.get("database")
                _conn = sqlite3.connect(db_path)
            # Add support for PostgreSQL and MySQL if needed
            # elif db_type == "PostgreSQL":
            #     import psycopg2
            #     _conn = psycopg2.connect(**details)
            # elif db_type == "MySQL":
            #     import mysql.connector
            #     _conn = mysql.connector.connect(**details)
                
        except Exception as e:
            st.error(f"Failed to connect to database: {e}")
            return None
    
    # Fallback to config file (for backward compatibility)
    elif _conn is None:
        db_config = _cfg.get("database", {})
        if db_config:
            db_type = db_config.get("type", "DuckDB")
            connection_params = db_config.get("connection", {})
            
            if db_type == "DuckDB":
                db_path = connection_params.get("database", _cfg.get("duckdb_path", "./data/amazon.duckdb"))
                _conn = duckdb.connect(database=db_path)
            elif db_type == "SQLite":
                db_path = connection_params.get("database")
                if db_path:
                    _conn = sqlite3.connect(db_path)
        else:
            # Default fallback to DuckDB with amazon.duckdb
            db_path = _cfg.get("duckdb_path", "./data/amazon.duckdb")
            _conn = duckdb.connect(database=db_path)
    
    return _conn

def get_schema() -> dict:
    """Get database schema with support for different database types"""
    global _schema_cache
    if _schema_cache is not None:
        return _schema_cache

    conn = get_connection()
    if conn is None:
        return {}
    
    schema = {}
    
    try:
        # Determine database type
        db_type = "DuckDB"  # Default
        if hasattr(st.session_state, 'connection') and st.session_state.connection.get("active"):
            db_type = st.session_state.connection["type"]
        
        if db_type == "DuckDB":
            # DuckDB schema query
            tables_df = conn.execute("SHOW TABLES").fetchdf()
            for t in tables_df['name']:
                cols_df = conn.execute(f"DESCRIBE {t}").fetchdf()
                schema[t] = cols_df['column_name'].tolist()
                
        elif db_type == "SQLite":
            # SQLite schema query
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            for table in tables:
                cursor.execute(f"PRAGMA table_info({table})")
                columns = [col[1] for col in cursor.fetchall()]
                schema[table] = columns
        
        _schema_cache = schema
        
    except Exception as e:
        st.error(f"Error loading schema: {e}")
        schema = {}
    
    return schema

def run_query(query: str) -> pd.DataFrame:
    """Execute SQL query and return results as DataFrame"""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        # Determine database type for proper query execution
        db_type = "DuckDB"  # Default
        if hasattr(st.session_state, 'connection') and st.session_state.connection.get("active"):
            db_type = st.session_state.connection["type"]
        
        if db_type == "DuckDB":
            result_df = conn.execute(query).fetchdf()
        elif db_type == "SQLite":
            result_df = pd.read_sql_query(query, conn)
        else:
            # Fallback to pandas read_sql
            result_df = pd.read_sql_query(query, conn)
            
        return result_df
        
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return pd.DataFrame()


def is_modification_query(sql_query: str) -> bool:
    """Check if the SQL query is a data modification statement."""
    sql = sql_query.strip().lower()
    return sql.startswith("update") or sql.startswith("delete") or sql.startswith("insert") or " drop " in sql

def render_sql_result(sql_query, df, st):
    ai_content = f"**GENERATED SQL**\n"
    st.session_state.chat_history.extend([
        {"role": "assistant", "content": ai_content, "avatar": "static/genie.png"},
        {"role": "assistant", "content": df.to_markdown(), "avatar": "static/genie.png"}
    ])
    with st.chat_message("assistant", avatar='static/genie.png'):
        st.markdown(ai_content)
        st.code(sql_query)
        st.subheader("Results")
        st.dataframe(df)


# ---------------------------- DuckDB Connection Ends ----------------------------

#------------------------------ Databricks Connection starts ------------------------

# def get_connection():
#     global _conn, _datagenie_selected
#     if _conn is None:
#         _conn = sql.connect(
#             server_hostname=_cfg["host"],
#             http_path=_cfg["http_path"],
#             access_token=_cfg["token"],
#         )

#     # Run USE datagenie once per session
#     if not _datagenie_selected:
#         with _conn.cursor() as cursor:
#             cursor.execute("USE datagenie")
#         _datagenie_selected = True

#     return _conn

# def get_schema() -> dict:
#     global _schema_cache
#     # Return cached schema if already loaded
#     if _schema_cache is not None:
#         return _schema_cache

#     conn = get_connection()
#     tables = pd.read_sql("SHOW TABLES", conn)
#     schema = {}
#     for t in tables["tableName"]:
#         cols = pd.read_sql(f"DESCRIBE TABLE {t}", conn)["col_name"].tolist()
#         schema[t] = cols

#     _schema_cache = schema  # ← Cache it
#     return schema

# def run_query(query: str) -> pd.DataFrame:
#     return pd.read_sql(query, get_connection())


#----------------------- databricks connection ends----------------------------------