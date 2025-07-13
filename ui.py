# app.py

import streamlit as st
import google.generativeai as genai
import json
import toml
import logging
from prompts import SYSTEM_PROMPT
from utils import get_schema, run_query

from data_scanner.data_profiler import DataProfiler
#from data_scanner.data_quality import DataQualityChecker


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


# Initialize chat history and add greeting only once
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
    # Add greeting as the very first message
    st.session_state.chat_history.append({
        "role": "assistant",
        "content": "Hello, I am your Data Genie. 🔥 away with your question!",
        "avatar": "static/genie.png"
    })

col1, col2 = st.columns([1, 5])
with col1:
    st.image("static/genie.png", width=120)  # Adjust width as needed
with col2:
    st.markdown("<h1>Genieverse</h1>", unsafe_allow_html=True)
# st.title("🔍 NL-to-SQL on Databricks (Gemini)")
# st.markdown("![Logo](app/static/genie.png)")

# Show past chat messages
for entry in st.session_state.chat_history:
    role = entry["role"]      # "user" or "assistant"
    content = entry["content"]
    avatar = entry.get("avatar", None)  # Use avatar if present
    if avatar:
        with st.chat_message(role, avatar=avatar):
            st.markdown(content)
    else:
        with st.chat_message(role):
            st.markdown(content)

# Load schema once at startup
if "schema" not in st.session_state:
    st.session_state.schema = get_schema()


#---------------------  Handle Anomaly related questions -----------------


def handle_anomaly_questions(prompt, scan_results):
    """Handle questions about anomalies and data quality"""
    prompt_lower = prompt.lower()
    
    if any(keyword in prompt_lower for keyword in ['anomaly', 'anomalies', 'issues', 'problems']):
        if not scan_results:
            return "I haven't scanned the data yet. Please click 'Scan for Anomalies' in the sidebar first."
        
        # Generate summary of findings
        response = "**Data Quality Summary:**\n\n"
        
        # Profile anomalies
        total_anomalies = sum(len(p['anomalies']) for p in scan_results['profiles'].values())
        if total_anomalies > 0:
            response += f"🔍 **{total_anomalies} Statistical Anomalies Found:**\n"
            for table, profile in scan_results['profiles'].items():
                if profile['anomalies']:
                    response += f"\n**{table}:**\n"
                    for anomaly in profile['anomalies']:
                        response += f"• {anomaly['message']}\n"
        
        # # Quality issues
        # if scan_results['quality_issues']:
        #     response += f"\n⚠️ **{len(scan_results['quality_issues'])} Quality Issues:**\n"
        #     for issue in scan_results['quality_issues']:
        #         response += f"• {issue['message']}\n"
        
        if total_anomalies == 0 : #and len(scan_results['quality_issues']) == 0:
            response += "✅ No significant anomalies or quality issues detected!"
        
        return response
    
    return None


#------------------------- Data Profiler Starts----------------------------


if "data_analysis" not in st.session_state:
    st.session_state.data_analysis = {
        'profiler': DataProfiler(st.session_state.schema),
        #'quality_checker': DataQualityChecker(st.session_state.schema),
        'scan_results': None
    }

# Add a sidebar for data analysis controls
with st.sidebar:
    st.header("Data Analysis")
    
    if st.button("🔍 Scan for Anomalies"):
        with st.spinner("Scanning dataset for anomalies..."):
            # Run data profiling
            profile_results = {}
            for table in st.session_state.schema.keys():
                profile_results[table] = st.session_state.data_analysis['profiler'].profile_table(table)
            
            # Run quality checks
            #quality_issues = st.session_state.data_analysis['quality_checker'].run_quality_checks()
            
            # Store results
            st.session_state.data_analysis['scan_results'] = {
                'profiles': profile_results,
                #'quality_issues': quality_issues
            }
            
            # Add findings to chat
            findings_summary = f"**Data Scan Complete!**\n\n"
            findings_summary += f"🔍 **Anomalies Found:** {sum(len(p['anomalies']) for p in profile_results.values())}\n"
            #findings_summary += f"⚠️ **Quality Issues:** {len(quality_issues)}\n\n"
            
            # if quality_issues:
            #     findings_summary += "**Top Issues:**\n"
            #     for issue in quality_issues[:3]:
            #         findings_summary += f"• {issue['message']}\n"
            
            st.session_state.chat_history.append({
                "role": "assistant", 
                "content": findings_summary
            })
    
    # Show scan status
    if st.session_state.data_analysis['scan_results']:
        st.success("✅ Latest scan completed")
        scan_results = st.session_state.data_analysis['scan_results']
        st.metric("Anomalies", sum(len(p['anomalies']) for p in scan_results['profiles'].values()))
        #st.metric("Quality Issues", len(scan_results['quality_issues']))

#------------------------- Data Profiler Ends----------------------------
# Prompt user and handle submission
if prompt := st.chat_input("Ask a question about your data"):
    # 1) Append the user’s message
    st.session_state.chat_history.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)


    # Check for anomaly-related questions first
    anomaly_response = handle_anomaly_questions(prompt, st.session_state.data_analysis['scan_results'])
    if anomaly_response:
        st.session_state.chat_history.append({"role": "assistant", "content": anomaly_response})
        st.rerun()
        
    else:

        # 2) Construct your LLM prompt (including schema) and get SQL
        schema_text = json.dumps(st.session_state.schema, indent=2)
        logger.info("schema_text :\n%s", schema_text)
        full_prompt = f"{SYSTEM_PROMPT}\n\nSchema:\n{schema_text}\n\nQuestion: {prompt}"
        logger.info("full_prompt :\n%s", full_prompt)
        chat = model.start_chat()
        logger.info("Chat :\n%s", chat)
        response = chat.send_message(full_prompt)
        logger.info("Response :\n%s", response)
        sql_query = response.text.strip().strip("```sql")
        logger.info("sql_query :\n%s", sql_query)

        # 3) Execute SQL
        df = run_query(sql_query)

        # 4) Prepare AI’s reply content
        ai_content = f"**GENERATED SQL**\n```sql\n{sql_query}\n```"


        # 5) Append and render AI’s message
        st.session_state.chat_history.extend([
            {"role": "assistant", "content": ai_content, "avatar": "static/genie.png"},
            {"role": "assistant", "content": df.to_markdown(), "avatar": "static/genie.png"}
            ])
        
        with st.chat_message("assistant", avatar='static/genie.png'):
            st.markdown(ai_content)
            st.subheader("Results")
            st.dataframe(df)
