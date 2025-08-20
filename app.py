# === CHANGED / ADDITIVE UPDATE STARTS HERE ===
import logging
import pandas as pd
import plotly.io as pio
import streamlit as st
from agents import DataEngineerAgent, VisualizationAgent
from utils import get_schema
# === NEW ===
from router import Router
from streamlit_mic_recorder import mic_recorder, speech_to_text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ASSISTANT_AVATAR = "static/genie.png"

def get_greeting_message():
    """Get personalized greeting message based on logged-in user"""
    username = st.session_state.get("username", "Guest")
    return f"Hello {username}, I am your Data Genie. 🔥 away with your question!"

# --- Session Initialization ---
def initialize_session_state():
    if "schema" not in st.session_state:
        st.session_state.schema = get_schema()
    if "data_engineer" not in st.session_state:
        st.session_state.data_engineer = DataEngineerAgent("data_engineer", st.session_state.schema)
    if "visualization_agent" not in st.session_state:
        st.session_state.visualization_agent = VisualizationAgent("visualization_agent")
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []
        st.session_state.chat_history.append({
            "role": "assistant",
            "content": get_greeting_message(),
            "avatar": ASSISTANT_AVATAR
        })   
    if "last_sql_query" not in st.session_state:
        st.session_state.last_sql_query = None
    if "last_df" not in st.session_state:
        st.session_state.last_df = None
    # === NEW ===
    if "router" not in st.session_state:
        st.session_state.router = Router()
    if "pending_clarification" not in st.session_state:
        # shape: {"context": "data"|"visualization", "original_prompt": str, "history": [str]}
        st.session_state.pending_clarification = None

    # Speech to text    
    if "is_recording" not in st.session_state:
        st.session_state.is_recording = False
    if "recorded_text" not in st.session_state:
        st.session_state.recorded_text = ""
    if "last_query_completed" not in st.session_state:
        st.session_state.last_query_completed = False

# --- History Rendering ---
def render_history():
    """Render all stored messages with any associated SQL, data, or visualization."""
    for entry in st.session_state.chat_history:
        if "user" in entry and entry["user"]:
            with st.chat_message("user"):
                st.write(entry["user"])
        if "assistant" in entry and entry["assistant"]:
            with st.chat_message("assistant",avatar=entry.get("avatar", ASSISTANT_AVATAR)):
                st.write(entry["assistant"])

        data = entry.get("data", {})
        if data:
            # if "sql_query" in data and data["sql_query"]:
            #     st.subheader("Generated SQL")
            #     st.code(data["sql_query"], language="sql")
            if "dataframe" in data and isinstance(data["dataframe"], pd.DataFrame):
                st.subheader("Query Results")
                st.dataframe(data["dataframe"])
            if "fig_json" in data and data["fig_json"]:
                try:
                    fig = pio.from_json(data["fig_json"])
                    st.subheader(fig['data'][0]['type'].capitalize()+" Chart")
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    logger.error(f"Failed to render stored fig_json: {e}")

# --- Clarification placeholder ---
# === CHANGED: now persists and sets a pending state ===
def ask_clarification(question, context, prompt, extra=None):
    # store in chat history
    st.session_state.chat_history.append({
        "user": None,
        "assistant": question,
        "avatar": ASSISTANT_AVATAR,
        "data": {}
    })
    with st.chat_message("assistant",avatar=ASSISTANT_AVATAR):
        st.write(question)
    # mark pending so next user reply will be treated as an answer
    st.session_state.pending_clarification = {
        "context": context,                 # "data" or "visualization"
        "original_prompt": prompt,
        "history": [question]
    }

# --- Visualization Flow ---
def handle_visualization(prompt, clarification_history=None):
    clarification_history = clarification_history or []
    
    # Use existing dataframe if available, otherwise fetch new data
    if st.session_state.last_df is not None and not st.session_state.last_df.empty:
        df = st.session_state.last_df
        logger.info("Using existing dataframe for visualization with %d rows", len(df))
    else:
        with st.spinner("Fetching data for visualization..."):
            # ask the DE agent to produce a small preview dataset for chart inference
            preview = st.session_state.data_engineer.process({
                "text": f"Preview 20 rows for: {prompt}",
                "clarification_history": clarification_history
            })
        if preview.get("clarification_needed"):
            ask_clarification(preview["clarification_question"], "visualization", prompt, preview)
            return

        df = preview.get("dataframe")
    vis_result = st.session_state.visualization_agent.process({
        "user_query": prompt,
        "dataframe": df,
        "clarification_history": clarification_history
    })
    
    # Log the data being used for visualization
    if df is not None and not df.empty:
        logger.info("Visualization using dataframe with columns: %s", list(df.columns))
        logger.info("First few rows for visualization: %s", df.head().to_dict('records') if len(df) > 0 else [])
    
    if vis_result.get("clarification_needed"):
        ask_clarification(vis_result["clarification_question"], "visualization", prompt, df)
        return

    fig = vis_result.get("fig")
    fig_json = fig.to_json() if fig is not None else None

    # Store in chat history
    st.session_state.chat_history.append({
        "user": prompt,
        "assistant": "Visualization generated.",
        "avatar": ASSISTANT_AVATAR,
        "data": {"fig_json": fig_json}
    })

    # Show now
    with st.chat_message("assistant",avatar=ASSISTANT_AVATAR):
        st.write("Visual generated.")
    if fig is not None:
        st.subheader(fig['data'][0]['type'].capitalize()+" Chart")
        st.plotly_chart(fig, use_container_width=True)
        logger.info("Display Fig :\n%s",fig['data'][0]['type'].capitalize())

    st.session_state.last_query_completed = True

# --- Data Flow ---
def handle_data(prompt, clarification_history=None):
    clarification_history = clarification_history or []
    with st.spinner("Generating SQL and fetching data..."):
        result = st.session_state.data_engineer.process({
            "text": prompt,
            "clarification_history": clarification_history
        })

    if result.get("clarification_needed"):
        ask_clarification(result["clarification_question"], "data", prompt)
        return

    sql_query = result.get("sql_query")
    df = result.get("dataframe")

    # Save for persistence
    st.session_state.last_sql_query = sql_query
    st.session_state.last_df = df

    # Store in chat history so old results remain visible
    st.session_state.chat_history.append({
        "user": prompt,
        "assistant": "Generated SQL and fetched results.",
        "avatar": ASSISTANT_AVATAR,
        "data": {
            "sql_query": sql_query,
            "dataframe": df.copy() if df is not None else None,
            "fig_json": None
        }
    })

    # Show now
    with st.chat_message("assistant",avatar=ASSISTANT_AVATAR):
        st.write("Fetched results.")

    logger.info("Display SQL Query:\n%s", sql_query)

    # if sql_query:
    #     st.subheader("Generated SQL")
    #     st.code(sql_query, language="sql")
    # else:
    #     st.warning("No SQL query generated.")

    if df is not None and not df.empty:
        st.subheader("Query Results")
        st.dataframe(df)
    else:
        st.warning("No data returned.")

    st.session_state.last_query_completed = True

# === NEW === Reasoning Flow (explain/query summary)
def handle_reasoning(user_query):
    df = st.session_state.last_df
    sql = st.session_state.last_sql_query or ""
    result = st.session_state.reasoning_agent.process({
        "user_query": user_query,
        "dataframe": df,
        "sql_query": sql
    })
    explanation = result.get("explanation", "No explanation generated.")
    st.session_state.chat_history.append({
        "user": user_query,
        "assistant": explanation,
        "data": {}
    })
    with st.chat_message("assistant"):
        st.write(explanation)

# === NEW === Pending clarification resolver
def handle_pending_clarification(answer_text):
    pend = st.session_state.pending_clarification
    if not pend:
        return False

    st.session_state.chat_history.append({
        "user": answer_text,
        "assistant": None,
        "avatar": None,
        "data": {}
    })

    with st.chat_message("user"):
        st.write(answer_text)

    # add the answer to history we pass back to the agent(s)
    clar_hist = pend.get("history", []) + [answer_text]
    original = pend.get("original_prompt", "")
    context = pend.get("context")

    # clear the pending state up front to avoid loops
    st.session_state.pending_clarification = None

    combined_prompt = f"{original} — Clarification: {answer_text}"

    # continue the original request with the clarification
    if context == "data":
        # combine: original intent + extra detail
        handle_data(combined_prompt, clarification_history=clar_hist)
    elif context == "visualization":
        handle_visualization(combined_prompt, clarification_history=clar_hist)
    else:
        # fallback to data if unknown
        handle_data(combined_prompt, clarification_history=clar_hist)
    return True


def render_recording_section(position="top"):
    key_suffix = "_top" if position == "top" else "_bottom"
    if st.session_state.is_recording:
        text = speech_to_text(
            language="en",
            just_once=False,
            key=f"voice_input{key_suffix}",
            use_container_width=False
        )
        
        if text:
            st.session_state.recorded_text = text
            user_input = text
            logger.info("Voice Generated Text:\n%s", user_input)
            st.session_state.is_recording = False
            st.rerun()

# --- Main App ---
def main():
    #st.set_page_config(page_title="Data Assistant", layout="wide")
    # st.title("🤖 Data Assistant")

    # col1, col2 = st.columns([1, 5])
    # with col1:
    #     st.image("static/genie.png", width=120)  # Adjust width as needed
    # with col2:
    #     st.markdown("<h1>Genieverse</h1>", unsafe_allow_html=True)

    initialize_session_state()
    with st.chat_message("assistant", avatar=ASSISTANT_AVATAR):
        st.write(get_greeting_message())

    render_history()

    # --- Top recorder (only before first query)
    if not st.session_state.last_query_completed:
        if not st.session_state.is_recording:
            if st.button("🎙️ Try Voice instead", key="top_voice_btn"):
                st.session_state.is_recording = True
                st.rerun()
        render_recording_section(position="top")


    typed_input = st.chat_input("Ask about your data.")

    # Prioritize voice text if present
    if st.session_state.recorded_text:
        user_input = st.session_state.recorded_text
        st.session_state.recorded_text = ""  # Clear after using
    elif typed_input:
        user_input = typed_input
    else:
        user_input = None


    
    

    if not user_input:
        if st.session_state.last_query_completed:
            if not st.session_state.is_recording:
                if st.button("🎙️ Try Voice instead (again)", key="bottom_voice_btn"):
                    st.session_state.is_recording = True
                    st.rerun()
            render_recording_section(position="bottom")
        return


    # If there is a pending clarification, treat this message as the answer
    if handle_pending_clarification(user_input):
        # the resolver already handled messaging & history
        return

    # Show user message
    with st.chat_message("user"):
        st.write(user_input)

    # === CHANGED: use the Router for consistent routing
    last_cols = list(st.session_state.last_df.columns) if isinstance(st.session_state.last_df, pd.DataFrame) else None
    routing = st.session_state.router.predict(user_input, df_columns=last_cols)
    route = routing.get("route", "multi")
    
    
    if route == "data_engineer":
        handle_data(user_input)
    elif route == "visualization":
        handle_visualization(user_input)
    else:
        # "multi": run data first, then attempt a visualization on the result
        handle_data(user_input)
        if st.session_state.last_df is not None and not st.session_state.last_df.empty:
            handle_visualization(user_input)

    
    if st.session_state.last_query_completed:
        if not st.session_state.is_recording:
            if st.button("🎙️ Try Voice instead (again)", key="bottom_voice_btn"):
                st.session_state.is_recording = True
                st.rerun()
        render_recording_section(position="bottom")

if __name__ == "__main__":
    main()
# === CHANGED / ADDITIVE UPDATE ENDS HERE ===
