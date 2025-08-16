
import toml
import pandas as pd
#from databricks import sql
import duckdb

# Load config once
_cfg = toml.load("config.toml")
_conn = None
_schema_cache = None    # ← Add a module-level cache
#_datagenie_selected = False   
# ---------------------------- DuckDB Connection Starts ----------------------------

def get_connection():
    global _conn
    if _conn is None:
        # You can use ':memory:' for in-memory DB, or a file path for persistent DB
        db_path = _cfg.get("duckdb_path", ":memory:")
        _conn = duckdb.connect(database=db_path)
    return _conn

def get_schema() -> dict:
    global _schema_cache
    if _schema_cache is not None:
        return _schema_cache

    conn = get_connection()
    # Get list of tables
    tables_df = conn.execute("SHOW TABLES").fetchdf()
    schema = {}
    for t in tables_df['name']:
        # Get columns for each table
        cols_df = conn.execute(f"DESCRIBE {t}").fetchdf()
        schema[t] = cols_df['column_name'].tolist()
    _schema_cache = schema
    return schema

def run_query(query: str) -> pd.DataFrame:
    conn = get_connection()
    result_df = conn.execute(query).fetchdf()
    return result_df


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