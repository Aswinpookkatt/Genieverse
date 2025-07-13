# app.py

import streamlit as st
import google.generativeai as genai
import json
import toml
import logging
from prompts import SYSTEM_PROMPT
from utils import get_schema, run_query


# Configure the logger
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Load config
config = toml.load("config.toml")
genai.configure(api_key=config["gemini_api_key"])

# Define function for Gemini
def execute_sql(query: str):
    """Execute SQL query on the database"""
    return run_query(query)

# Initialize Gemini model with function calling
model = genai.GenerativeModel('gemini-2.5-pro')

# # Initialize a chat history list if not already present
# if "chat_history" not in st.session_state:
#     st.session_state.chat_history = []

st.title("🔍 NL-to-SQL on Databricks (Gemini)")

# # Show past chat messages
# for entry in st.session_state.chat_history:
#     role = entry["role"]      # "user" or "assistant"
#     content = entry["content"]
#     with st.chat_message(role):
#         st.markdown(content)

# Load schema once at startup
if "schema" not in st.session_state:
    st.session_state.schema = get_schema()

question = st.text_input("Ask a question about your data:")
if st.button("Go"):
    # Create schema context
    schema_text = json.dumps(st.session_state.schema, indent=2)
    logger.info("Loaded database schema:\n%s", schema_text)
    
    # Create the prompt
    prompt = f"""
    {SYSTEM_PROMPT}
    
    Available database schema:
    {schema_text}
    
    User question: {question}
    """
    logger.info("Prompt :\n%s", prompt)
    try:
        # Generate content with function calling
        chat = model.start_chat()
        logger.info("Chat :\n%s", chat)
        response = chat.send_message(prompt)
        
        # Extract SQL from response
        sql_query = response.text.strip()
        
        # Clean up the response
        if sql_query.startswith("```"):
            sql_query = sql_query[6:-3].strip()
        elif sql_query.startswith("```"):
            sql_query = sql_query[3:-3].strip()
        
        # Execute and display
        df = run_query(sql_query)
        st.subheader("Generated SQL")
        st.code(sql_query)
        st.subheader("Results")
        st.dataframe(df)
        
    except Exception as e:
        st.error(f"Error: {str(e)}")
