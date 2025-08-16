# # app.py
# import streamlit as st
# import pandas as pd
# import plotly.io as pio
# import logging

# from agents import DataEngineerAgent, ReasoningAgent, VisualizationAgent
# from utils import get_schema
# from router import Router

# # Configure logger
# logging.basicConfig(
#     format="%(asctime)s %(levelname)s %(message)s",
#     level=logging.INFO,
#     datefmt="%Y-%m-%d %H:%M:%S"
# )
# logger = logging.getLogger(__name__)


# def initialize_session_state():
#     # schema + agents
#     if "schema" not in st.session_state:
#         st.session_state.schema = get_schema()

#     if "data_engineer" not in st.session_state:
#         st.session_state.data_engineer = DataEngineerAgent('data_engineer', st.session_state.schema)

#     if "reasoning_agent" not in st.session_state:
#         st.session_state.reasoning_agent = ReasoningAgent('reasoning_agent')

#     if "visualization_agent" not in st.session_state:
#         st.session_state.visualization_agent = VisualizationAgent('visualization_agent')

#     # router
#     if "router" not in st.session_state:
#         st.session_state.router = Router()

#     # chat history and visualization clarification state
#     if "chat_history" not in st.session_state:
#         st.session_state.chat_history = []
#     if "clarification_active" not in st.session_state:
#         st.session_state.clarification_active = False
#     if "clarification_question" not in st.session_state:
#         st.session_state.clarification_question = None
#     if "pending_query" not in st.session_state:
#         st.session_state.pending_query = None
#     if "pending_df" not in st.session_state:
#         st.session_state.pending_df = None
#     if "clarification_history" not in st.session_state:
#         st.session_state.clarification_history = []
#     if "original_shown_in_clarification" not in st.session_state:
#         st.session_state.original_shown_in_clarification = False

#     # DataEngineer-specific clarification state
#     if "de_clarification_active" not in st.session_state:
#         st.session_state.de_clarification_active = False
#     if "de_clarification_question" not in st.session_state:
#         st.session_state.de_clarification_question = None
#     if "de_pending_query" not in st.session_state:
#         st.session_state.de_pending_query = None
#     if "de_clarification_history" not in st.session_state:
#         st.session_state.de_clarification_history = []
#     if "de_original_shown_in_clarification" not in st.session_state:
#         st.session_state.de_original_shown_in_clarification = False

#     # UI mode: Auto / View Raw Data / Visualize Data
#     if "mode" not in st.session_state:
#         st.session_state.mode = "Auto"  # default


# def render_history():
#     """Render saved chat_history (user + assistant messages, plus any attached data)."""
#     for entry in st.session_state.chat_history:
#         if entry.get("user") is not None:
#             with st.chat_message("user"):
#                 st.write(entry["user"])
#         if entry.get("assistant") is not None:
#             with st.chat_message("assistant"):
#                 st.write(entry["assistant"])

#         data = entry.get("data", {})
#         if data:
#             # show SQL if present
#             if "sql_query" in data and data["sql_query"]:
#                 st.subheader("Generated SQL")
#                 st.code(data["sql_query"], language="sql")
#             # show dataframe if present
#             if "dataframe" in data and isinstance(data["dataframe"], pd.DataFrame):
#                 st.subheader("Query Results")
#                 st.dataframe(data["dataframe"])
#             # show visualization if stored
#             if "fig_json" in data and data["fig_json"]:
#                 try:
#                     fig = pio.from_json(data["fig_json"])
#                     st.subheader("Visualization")
#                     st.plotly_chart(fig, use_container_width=True)
#                 except Exception as e:
#                     logger.error("Failed to render stored fig_json: %s", e)


# def start_de_clarification(original_prompt, clar_q):
#     """Persist the initial DE clarification entry (original question + assistant clar Q) and set state."""
#     st.session_state.chat_history.append({
#         "user": original_prompt,
#         "assistant": clar_q,
#         "data": None
#     })
#     st.session_state.de_clarification_active = True
#     st.session_state.de_clarification_question = clar_q
#     st.session_state.de_pending_query = original_prompt
#     st.session_state.de_clarification_history = []
#     st.session_state.de_original_shown_in_clarification = True


# def handle_de_clarification_loop():
#     """Handles DataEngineer clarification loop. Returns True if handled and app should wait/stop further processing."""
#     de_input = st.chat_input("Please clarify your question for SQL generation...")
#     if not de_input:
#         return True  # wait

#     # show and persist user's clarification immediately
#     with st.chat_message("user"):
#         st.write(de_input)
#     st.session_state.chat_history.append({
#         "user": de_input,
#         "assistant": None,
#         "data": None
#     })
#     st.session_state.de_clarification_history.append(de_input)

#     # Re-call DataEngineer with clarification history
#     with st.spinner("Updating SQL from clarifications..."):
#         de_agent = st.session_state.data_engineer
#         de_result = de_agent.process({
#             "text": st.session_state.de_pending_query,
#             "clarification_history": st.session_state.de_clarification_history
#         })

#     # If DataEngineer asks more clarification
#     if de_result.get("clarification_needed"):
#         clar_q = de_result.get("clarification_question", "Could you clarify further?")
#         st.session_state.chat_history.append({
#             "user": None,
#             "assistant": clar_q,
#             "data": None
#         })
#         st.session_state.de_clarification_question = clar_q
#         st.session_state.de_original_shown_in_clarification = True
#         st.rerun()
#         return True

#     # If DataEngineer succeeded -> show SQL + df and stop (View Raw Data mode) or continue (Auto/multi handled elsewhere)
#     if de_result.get("success"):
#         sql_query = de_result.get("sql_query")
#         df = de_result.get("dataframe")
#         st.session_state.chat_history.append({
#             "user": None,
#             "assistant": "Generated SQL and fetched results.",
#             "data": {
#                 "sql_query": sql_query,
#                 "dataframe": df.copy() if df is not None else None,
#                 "fig_json": None
#             }
#         })
#         with st.chat_message("assistant"):
#             st.write("Generated SQL and fetched results.")
#         if sql_query:
#             st.subheader("Generated SQL")
#             st.code(sql_query, language="sql")
#         if df is not None and isinstance(df, pd.DataFrame):
#             st.subheader("Query Results")
#             st.dataframe(df)

#         # Reset DE clarification state
#         st.session_state.de_clarification_active = False
#         st.session_state.de_clarification_question = None
#         st.session_state.de_pending_query = None
#         st.session_state.de_clarification_history = []
#         st.session_state.de_original_shown_in_clarification = False
#         return True

#     # If DataEngineer returned error
#     err = de_result.get("error", "DataEngineer returned an error.")
#     st.session_state.chat_history.append({
#         "user": None,
#         "assistant": err,
#         "data": None
#     })
#     with st.chat_message("assistant"):
#         st.write(err)
#     st.session_state.de_clarification_active = False
#     st.session_state.de_clarification_question = None
#     st.session_state.de_pending_query = None
#     st.session_state.de_clarification_history = []
#     st.session_state.de_original_shown_in_clarification = False
#     return True


# def start_vis_clarification(original_prompt, clar_q, df):
#     """Persist initial visualization clarification (original question + assistant clar Q) and set state."""
#     st.session_state.chat_history.append({
#         "user": original_prompt,
#         "assistant": clar_q,
#         "data": None
#     })
#     st.session_state.clarification_active = True
#     st.session_state.clarification_question = clar_q
#     st.session_state.pending_query = original_prompt
#     st.session_state.pending_df = df
#     st.session_state.clarification_history = []
#     st.session_state.original_shown_in_clarification = True


# def handle_vis_clarification_loop():
#     """Handles visualization clarification loop. Returns True if handled and app should wait/stop further processing."""
#     clarification_input = st.chat_input("Please clarify...")
#     if not clarification_input:
#         return True  # wait

#     # show and persist user's clarification immediately
#     with st.chat_message("user"):
#         st.write(clarification_input)
#     st.session_state.chat_history.append({
#         "user": clarification_input,
#         "assistant": None,
#         "data": None
#     })
#     st.session_state.clarification_history.append(clarification_input)

#     # Re-call visualization agent with clarification history
#     with st.spinner("Updating visualization..."):
#         vis_agent = st.session_state.visualization_agent
#         vis_result = vis_agent.process({
#             "user_query": st.session_state.pending_query,
#             "dataframe": st.session_state.pending_df,
#             "clarification_history": st.session_state.clarification_history
#         })

#     if vis_result.get("clarification_needed"):
#         clar_q = vis_result.get("clarification_question", "Could you clarify?")
#         st.session_state.chat_history.append({
#             "user": None,
#             "assistant": clar_q,
#             "data": None
#         })
#         st.session_state.clarification_question = clar_q
#         st.session_state.original_shown_in_clarification = True
#         st.rerun()
#         return True

#     # final result: show explanation + chart
#     explanation = vis_result.get("explanation", "Here is your chart.")
#     fig = vis_result.get("fig")
#     fig_json = fig.to_json() if fig is not None else None

#     st.session_state.chat_history.append({
#         "user": None,
#         "assistant": explanation,
#         "data": {"fig_json": fig_json}
#     })

#     with st.chat_message("assistant"):
#         st.write(explanation)
#     if fig is not None:
#         st.subheader("Visualization")
#         st.plotly_chart(fig, use_container_width=True)

#     # Reset visualization clarification state
#     st.session_state.clarification_active = False
#     st.session_state.clarification_question = None
#     st.session_state.pending_query = None
#     st.session_state.pending_df = None
#     st.session_state.clarification_history = []
#     st.session_state.original_shown_in_clarification = False
#     return True


# def main():
#     st.set_page_config(page_title="Multi-Agent Data Assistant", layout="wide")
#     st.title("🤖 Multi-Agent Data Assistant")
#     st.markdown("Ask questions about your data and get SQL queries, explanations, and insights!")

#     initialize_session_state()

#     data_engineer = st.session_state.data_engineer
#     reasoning_agent = st.session_state.reasoning_agent
#     visualization_agent = st.session_state.visualization_agent
#     router = st.session_state.router

#     # Sidebar: only mode selector + Clear Chat History
#     with st.sidebar:
#         st.header("Query Mode")
#         mode = st.radio("Choose mode:", ["Auto", "View Raw Data", "Visualize Data"], index=["Auto", "View Raw Data", "Visualize Data"].index(st.session_state.mode) if st.session_state.mode in ["Auto", "View Raw Data", "Visualize Data"] else 0)
#         st.session_state.mode = mode

#         st.write("")  # spacing
#         if st.button("Clear Chat History"):
#             st.session_state.chat_history = []
#             # reset all clarification flags
#             st.session_state.clarification_active = False
#             st.session_state.pending_query = None
#             st.session_state.pending_df = None
#             st.session_state.clarification_history = []
#             st.session_state.original_shown_in_clarification = False
#             st.session_state.de_clarification_active = False
#             st.session_state.de_pending_query = None
#             st.session_state.de_clarification_history = []
#             st.session_state.de_original_shown_in_clarification = False
#             st.rerun()

#     # render history
#     render_history()

#     # Prioritize DataEngineer clarification loop if active
#     if st.session_state.de_clarification_active:
#         handled = handle_de_clarification_loop()
#         if handled:
#             return

#     # Then visualization clarification loop
#     if st.session_state.clarification_active:
#         handled = handle_vis_clarification_loop()
#         if handled:
#             return

#     # Normal chat input
#     user_input = st.chat_input("Ask about your data...")
#     if not user_input:
#         return

#     prompt = user_input.strip()
#     # Immediately show user's message and persist placeholder
#     with st.chat_message("user"):
#         st.write(prompt)
#     st.session_state.chat_history.append({
#         "user": prompt,
#         "assistant": None,
#         "data": None
#     })

#     mode = st.session_state.mode
#     logger.info("Mode: %s", mode)

#     # -----------------------
#     # MODE: View Raw Data (only DataEngineer)
#     # -----------------------
#     if mode == "View Raw Data":
#         with st.spinner("Translating to SQL..."):
#             de_result = data_engineer.process({"text": prompt, "clarification_history": []})

#         if de_result.get("clarification_needed"):
#             clar_q = de_result.get("clarification_question", "Could you clarify the request for SQL?")
#             start_de_clarification(prompt, clar_q)
#             with st.chat_message("assistant"):
#                 st.write(clar_q)
#             st.rerun()
#             return

#         if not de_result.get("success"):
#             error_msg = de_result.get("error", "Failed to generate SQL.")
#             logger.error("DataEngineerAgent error: %s", error_msg)
#             st.session_state.chat_history[-1] = {
#                 "user": prompt,
#                 "assistant": error_msg,
#                 "data": None
#             }
#             with st.chat_message("assistant"):
#                 st.write(error_msg)
#             return

#         sql_query = de_result.get("sql_query")
#         df = de_result.get("dataframe")
#         # Persist final DE result (show SQL + df) and stop (no reasoning/visualization)
#         st.session_state.chat_history[-1] = {
#             "user": prompt,
#             "assistant": "Generated SQL and fetched results.",
#             "data": {
#                 "sql_query": sql_query,
#                 "dataframe": df.copy() if df is not None else None,
#                 "fig_json": None
#             }
#         }
#         with st.chat_message("assistant"):
#             st.write("Generated SQL and fetched results.")
#         if sql_query:
#             st.subheader("Generated SQL")
#             st.code(sql_query, language="sql")
#         if df is not None and isinstance(df, pd.DataFrame):
#             st.subheader("Query Results")
#             st.dataframe(df)
#         return

#     # -----------------------
#     # MODE: Visualize Data (only Visualization)
#     # -----------------------
#     if mode == "Visualize Data":
#         # Obtain a small preview df via DataEngineer to let visualization agent see columns & data
#         preview_prompt = f"Preview: return up to 20 rows relevant to: {prompt}"
#         with st.spinner("Getting preview data..."):
#             preview = data_engineer.process({"text": preview_prompt, "clarification_history": []})

#         if preview.get("clarification_needed"):
#             # Preview requires DE clarification — start DE clarification loop
#             clar_q = preview.get("clarification_question", "Could you clarify the preview request?")
#             start_de_clarification(prompt, clar_q)
#             with st.chat_message("assistant"):
#                 st.write(clar_q)
#             st.rerun()
#             return

#         if not preview.get("success"):
#             # If preview fails, fallback to telling user and stop
#             err = preview.get("error", "Failed to fetch preview data.")
#             st.session_state.chat_history[-1] = {
#                 "user": prompt,
#                 "assistant": err,
#                 "data": None
#             }
#             with st.chat_message("assistant"):
#                 st.write(err)
#             return

#         df = preview.get("dataframe")
#         # Call visualization agent
#         vis_result = visualization_agent.process({
#             "user_query": prompt,
#             "dataframe": df,
#             "clarification_history": []
#         })
#         if vis_result.get("clarification_needed"):
#             clar_q = vis_result.get("clarification_question", "Which column should be used?")
#             start_vis_clarification(prompt, clar_q, df)
#             with st.chat_message("assistant"):
#                 st.write(clar_q)
#             st.rerun()
#             return
#         else:
#             fig = vis_result.get("fig")
#             fig_json = fig.to_json() if fig is not None else None
#             st.session_state.chat_history[-1] = {
#                 "user": prompt,
#                 "assistant": "Visualization generated.",
#                 "data": {"fig_json": fig_json}
#             }
#             with st.chat_message("assistant"):
#                 st.write("Visualization generated.")
#             if fig is not None:
#                 st.subheader("Visualization")
#                 st.plotly_chart(fig, use_container_width=True)
#             return

#     # -----------------------
#     # MODE: Auto (router decides)
#     # -----------------------
#     # Use router to pick route
#     route_info = router.predict(prompt, df_columns=None)
#     route = route_info["route"]
#     logger.info("Router chose %s (conf=%.2f): %s", route, route_info["confidence"], route_info["explanation"])

#     # Reuse the behavior from the multi pipeline implementation for routes
#     # ROUTE: DataEngineer-only
#     if route == "data_engineer":
#         with st.spinner("Translating to SQL..."):
#             de_result = data_engineer.process({"text": prompt, "clarification_history": []})

#         if de_result.get("clarification_needed"):
#             clar_q = de_result.get("clarification_question", "Could you clarify your request for SQL?")
#             start_de_clarification(prompt, clar_q)
#             with st.chat_message("assistant"):
#                 st.write(clar_q)
#             st.rerun()
#             return

#         if not de_result.get("success"):
#             error_msg = de_result.get("error", "Failed to generate SQL.")
#             logger.error("DataEngineerAgent error: %s", error_msg)
#             st.session_state.chat_history[-1] = {
#                 "user": prompt,
#                 "assistant": error_msg,
#                 "data": None
#             }
#             with st.chat_message("assistant"):
#                 st.write(error_msg)
#             return

#         # DE success -> show SQL + df then optionally continue to reasoning & visualization
#         sql_query = de_result.get("sql_query")
#         df = de_result.get("dataframe")
#         st.session_state.chat_history[-1] = {
#             "user": prompt,
#             "assistant": "Generated SQL and fetched results.",
#             "data": {
#                 "sql_query": sql_query,
#                 "dataframe": df.copy() if df is not None else None,
#                 "fig_json": None
#             }
#         }
#         with st.chat_message("assistant"):
#             st.write("Generated SQL and fetched results.")
#         if sql_query:
#             st.subheader("Generated SQL")
#             st.code(sql_query, language="sql")
#         if df is not None and isinstance(df, pd.DataFrame):
#             st.subheader("Query Results")
#             st.dataframe(df)

#         # Then reasoning
#         reasoning_result = reasoning_agent.process({
#             "user_query": prompt,
#             "dataframe": df,
#             "sql_query": sql_query
#         })
#         explanation = reasoning_result.get("explanation", "")
#         st.session_state.chat_history.append({
#             "user": None,
#             "assistant": explanation,
#             "data": None
#         })
#         with st.chat_message("assistant"):
#             st.write(explanation)

#         # Then visualization
#         vis_result = visualization_agent.process({
#             "user_query": prompt,
#             "dataframe": df,
#             "clarification_history": []
#         })
#         if vis_result.get("clarification_needed"):
#             clar_q = vis_result.get("clarification_question", "Which column should be used?")
#             start_vis_clarification(prompt, clar_q, df)
#             with st.chat_message("assistant"):
#                 st.write(clar_q)
#             st.rerun()
#             return
#         else:
#             fig = vis_result.get("fig")
#             fig_json = fig.to_json() if fig is not None else None
#             st.session_state.chat_history.append({
#                 "user": None,
#                 "assistant": "Visualization generated.",
#                 "data": {"fig_json": fig_json}
#             })
#             with st.chat_message("assistant"):
#                 st.write("Visualization generated.")
#             if fig is not None:
#                 st.subheader("Visualization")
#                 st.plotly_chart(fig, use_container_width=True)
#             return

#     # ROUTE: Visualization-first
#     if route == "visualization":
#         preview_prompt = f"Preview: return up to 20 rows relevant to: {prompt}"
#         with st.spinner("Getting preview data..."):
#             preview = data_engineer.process({"text": preview_prompt, "clarification_history": []})

#         if preview.get("clarification_needed"):
#             clar_q = preview.get("clarification_question", "Could you clarify the preview request?")
#             start_de_clarification(prompt, clar_q)
#             with st.chat_message("assistant"):
#                 st.write(clar_q)
#             st.rerun()
#             return

#         if not preview.get("success"):
#             logger.warning("Preview failed; falling back to multi pipeline")
#             # fallback to multi route: set route = multi and fall through
#             route = "multi"
#         else:
#             df = preview.get("dataframe")
#             vis_result = visualization_agent.process({
#                 "user_query": prompt,
#                 "dataframe": df,
#                 "clarification_history": []
#             })
#             if vis_result.get("clarification_needed"):
#                 clar_q = vis_result.get("clarification_question", "Which column should be used?")
#                 start_vis_clarification(prompt, clar_q, df)
#                 with st.chat_message("assistant"):
#                     st.write(clar_q)
#                 st.rerun()
#                 return
#             else:
#                 fig = vis_result.get("fig")
#                 fig_json = fig.to_json() if fig is not None else None
#                 st.session_state.chat_history[-1] = {
#                     "user": prompt,
#                     "assistant": "Visualization generated.",
#                     "data": {"fig_json": fig_json}
#                 }
#                 with st.chat_message("assistant"):
#                     st.write("Visualization generated.")
#                 if fig is not None:
#                     st.subheader("Visualization")
#                     st.plotly_chart(fig, use_container_width=True)
#                 return

#     # ROUTE: Reasoning-only
#     if route == "reasoning":
#         preview_prompt = f"Preview: return up to 20 rows relevant to: {prompt}"
#         with st.spinner("Getting preview data for reasoning..."):
#             preview = data_engineer.process({"text": preview_prompt, "clarification_history": []})

#         if preview.get("clarification_needed"):
#             clar_q = preview.get("clarification_question", "Could you clarify the preview request?")
#             start_de_clarification(prompt, clar_q)
#             with st.chat_message("assistant"):
#                 st.write(clar_q)
#             st.rerun()
#             return

#         if not preview.get("success"):
#             logger.warning("Preview for reasoning failed; falling back to multi pipeline")
#             # fall through to multi handling
#         else:
#             df = preview.get("dataframe")
#             reasoning_result = reasoning_agent.process({
#                 "user_query": prompt,
#                 "dataframe": df,
#                 "sql_query": ""
#             })
#             explanation = reasoning_result.get("explanation", "")
#             st.session_state.chat_history[-1] = {
#                 "user": prompt,
#                 "assistant": explanation,
#                 "data": None
#             }
#             with st.chat_message("assistant"):
#                 st.write(explanation)
#             return

#     # ROUTE: Multi (fallback)
#     if route == "multi":
#         with st.spinner("Translating to SQL..."):
#             de_result = data_engineer.process({"text": prompt, "clarification_history": []})

#         if de_result.get("clarification_needed"):
#             clar_q = de_result.get("clarification_question", "Could you clarify your request for SQL?")
#             start_de_clarification(prompt, clar_q)
#             with st.chat_message("assistant"):
#                 st.write(clar_q)
#             st.rerun()
#             return

#         if not de_result.get("success"):
#             error_msg = de_result.get("error", "Failed to generate SQL.")
#             logger.error("DataEngineerAgent error: %s", error_msg)
#             st.session_state.chat_history[-1] = {
#                 "user": prompt,
#                 "assistant": error_msg,
#                 "data": None
#             }
#             with st.chat_message("assistant"):
#                 st.write(error_msg)
#             return

#         # DE success -> reasoning + visualization
#         sql_query = de_result.get("sql_query")
#         df = de_result.get("dataframe")
#         st.session_state.chat_history[-1] = {
#             "user": prompt,
#             "assistant": "Generated SQL and fetched results.",
#             "data": {
#                 "sql_query": sql_query,
#                 "dataframe": df.copy() if df is not None else None,
#                 "fig_json": None
#             }
#         }
#         with st.chat_message("assistant"):
#             st.write("Generated SQL and fetched results.")
#         if sql_query:
#             st.subheader("Generated SQL")
#             st.code(sql_query, language="sql")
#         if df is not None and isinstance(df, pd.DataFrame):
#             st.subheader("Query Results")
#             st.dataframe(df)

#         # reasoning
#         reasoning_result = reasoning_agent.process({
#             "user_query": prompt,
#             "dataframe": df,
#             "sql_query": sql_query
#         })
#         explanation = reasoning_result.get("explanation", "")
#         st.session_state.chat_history.append({
#             "user": None,
#             "assistant": explanation,
#             "data": None
#         })
#         with st.chat_message("assistant"):
#             st.write(explanation)

#         # visualization
#         vis_result = visualization_agent.process({
#             "user_query": prompt,
#             "dataframe": df,
#             "clarification_history": []
#         })
#         if vis_result.get("clarification_needed"):
#             clar_q = vis_result.get("clarification_question", "Which column should be used?")
#             start_vis_clarification(prompt, clar_q, df)
#             with st.chat_message("assistant"):
#                 st.write(clar_q)
#             st.rerun()
#             return
#         else:
#             fig = vis_result.get("fig")
#             fig_json = fig.to_json() if fig is not None else None
#             st.session_state.chat_history.append({
#                 "user": None,
#                 "assistant": "Visualization generated.",
#                 "data": {"fig_json": fig_json}
#             })
#             with st.chat_message("assistant"):
#                 st.write("Visualization generated.")
#             if fig is not None:
#                 st.subheader("Visualization")
#                 st.plotly_chart(fig, use_container_width=True)
#             return


# if __name__ == "__main__":
#     main()



#-------------------------------- Refactor Logic ----------------------------

import logging
import pandas as pd
import plotly.io as pio
import streamlit as st
from agents import DataEngineerAgent, ReasoningAgent, VisualizationAgent
from utils import get_schema

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Session Initialization ---
def initialize_session_state():
    if "schema" not in st.session_state:
        st.session_state.schema = get_schema()
    if "data_engineer" not in st.session_state:
        st.session_state.data_engineer = DataEngineerAgent("data_engineer", st.session_state.schema)
    if "reasoning_agent" not in st.session_state:
        st.session_state.reasoning_agent = ReasoningAgent("reasoning_agent")
    if "visualization_agent" not in st.session_state:
        st.session_state.visualization_agent = VisualizationAgent("visualization_agent")
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []
    if "last_sql_query" not in st.session_state:
        st.session_state.last_sql_query = None
    if "last_df" not in st.session_state:
        st.session_state.last_df = None


# --- History Rendering ---
def render_history():
    """Render all stored messages with any associated SQL, data, or visualization."""
    for entry in st.session_state.chat_history:
        if "user" in entry and entry["user"]:
            with st.chat_message("user"):
                st.write(entry["user"])
        if "assistant" in entry and entry["assistant"]:
            with st.chat_message("assistant"):
                st.write(entry["assistant"])

        data = entry.get("data", {})
        if data:
            if "sql_query" in data and data["sql_query"]:
                st.subheader("Generated SQL")
                st.code(data["sql_query"], language="sql")
            if "dataframe" in data and isinstance(data["dataframe"], pd.DataFrame):
                st.subheader("Query Results")
                st.dataframe(data["dataframe"])
            if "fig_json" in data and data["fig_json"]:
                try:
                    fig = pio.from_json(data["fig_json"])
                    st.subheader("Visualization")
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    logger.error(f"Failed to render stored fig_json: {e}")


# --- Clarification placeholder ---
def ask_clarification(question, context, prompt, extra=None):
    with st.chat_message("assistant"):
        st.write(question)
    # You can expand this to store and handle follow-up clarifications


# --- Visualization Flow ---
def handle_visualization(prompt):
    with st.spinner("Fetching data for visualization..."):
        preview = st.session_state.data_engineer.process({"text": f"Preview 20 rows for: {prompt}"})
    if preview.get("clarification_needed"):
        ask_clarification(preview["clarification_question"], "visualization", prompt, preview)
        return

    df = preview.get("dataframe")
    vis_result = st.session_state.visualization_agent.process({"user_query": prompt, "dataframe": df})
    if vis_result.get("clarification_needed"):
        ask_clarification(vis_result["clarification_question"], "visualization", prompt, df)
        return

    fig = vis_result.get("fig")
    fig_json = fig.to_json() if fig is not None else None

    # Store in chat history
    st.session_state.chat_history.append({
        "user": prompt,
        "assistant": "Visualization generated.",
        "data": {"fig_json": fig_json}
    })

    # Show now
    with st.chat_message("assistant"):
        st.write("Visualization generated.")
    if fig is not None:
        st.subheader("Visualization")
        st.plotly_chart(fig, use_container_width=True)


# --- Data Flow ---
def handle_data(prompt):
    with st.spinner("Generating SQL and fetching data..."):
        result = st.session_state.data_engineer.process({"text": prompt})

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
        "data": {
            "sql_query": sql_query,
            "dataframe": df.copy() if df is not None else None,
            "fig_json": None
        }
    })

    # Show now
    with st.chat_message("assistant"):
        st.write("Generated SQL and fetched results.")

    if sql_query:
        st.subheader("Generated SQL")
        st.code(sql_query, language="sql")
    else:
        st.warning("No SQL query generated.")

    if df is not None and not df.empty:
        st.subheader("Query Results")
        st.dataframe(df)
    else:
        st.warning("No data returned.")


# --- Main App ---
def main():
    st.set_page_config(page_title="Data Assistant", layout="wide")
    st.title("🤖 Data Assistant")

    initialize_session_state()
    render_history()

    user_input = st.chat_input("Ask about your data.")
    if not user_input:
        return

    # Show user message
    with st.chat_message("user"):
        st.write(user_input)

    # Decide whether to visualize or get SQL
    if any(word in user_input.lower() for word in ["plot", "chart", "graph", "visualize"]):
        handle_visualization(user_input)
    else:
        handle_data(user_input)


if __name__ == "__main__":
    main()
