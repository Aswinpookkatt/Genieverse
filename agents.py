# # agents.py
# import google.generativeai as genai
# from utils import run_query
# import pandas as pd
# import os,toml,logging,io,base64,json,re
# import plotly.express as px




# # Load config and configure Gemini model
# config = toml.load("config.toml")
# api_key = config.get("gemini_api_key")

# # Configure the logger
# logging.basicConfig(
#     format="%(asctime)s %(levelname)s %(message)s",
#     level=logging.INFO,
#     datefmt="%Y-%m-%d %H:%M:%S"
# )
# logger = logging.getLogger(__name__)

# if not api_key:
#     raise ValueError("Google Gemini API key not found in config.toml")
# genai.configure(api_key=api_key)

# import re
# import json
# import logging
# import google.generativeai as genai
# from utils import run_query
# import pandas as pd

# logger = logging.getLogger(__name__)


# class DataEngineerAgent:
#     """
#     NL -> SQL agent that either:
#       - returns {"clarification_needed": True, "clarification_question": "..."}
#       - or returns {"success": True, "sql_query": "...", "dataframe": df}
#       - or returns {"success": False, "error": "..."}
#     """

#     def __init__(self, name, schema, model_name='gemini-2.5-flash'):
#         self.name = name
#         self.schema = schema
#         self.model = genai.GenerativeModel(model_name)

#     def _format_schema(self):
#         # Defensive formatting
#         parts = []
#         if not isinstance(self.schema, dict):
#             return ""
#         for table, cols in self.schema.items():
#             try:
#                 cols_list = list(cols) if cols is not None else []
#                 cols_text = ", ".join(str(c) for c in cols_list)
#             except Exception:
#                 cols_text = str(cols)
#             parts.append(f"Table: {table}\nColumns: {cols_text}")
#         return "\n\n".join(parts)

#     def _clean_llm_output(self, text: str):
#         if not text:
#             return ""
#         # Remove code fences and language tags
#         cleaned = re.sub(r"^```[a-zA-Z0-9]*\n|```$", "", text.strip(), flags=re.MULTILINE).strip()
#         return cleaned

#     def _ask_llm_for_sql_or_clarify(self, user_text: str, clarification_history=None):
#         """
#         Asks the LLM to either:
#         - produce SQL (plain SQL), OR
#         - request clarification by returning a JSON like:
#           {"clarification_needed": true, "clarification_question": "..."}
#         We instruct the model clearly to return JSON if it needs clarification.
#         """
#         schema_text = self._format_schema()
#         clar_hist_text = ""
#         if clarification_history:
#             clar_hist_text = "\n\nPrevious clarifications:\n" + "\n".join(f"- {c}" for c in clarification_history)

#         prompt = f"""
# You are an expert NL-to-SQL assistant. Given a relational schema and a user's question, either:
# 1) Return a single executable ANSI SQL statement (no explanation, no backticks), OR
# 2) If the user's request is ambiguous or missing critical details needed to generate correct SQL, return ONLY a minified JSON object with:
#    {{ "clarification_needed": true, "clarification_question": "..." }}
#    (no other text). The clarification question must be specific and, when possible, list available columns from the schema.

# Database schema:
# {schema_text}

# User question:
# {user_text}

# {clar_hist_text}

# Instructions:
# - If returning SQL: output only the SQL statement (no surrounding text).
# - If returning a clarification request: output ONLY the JSON object described above.
# - Prefer clear, unambiguous SQL. Limit rows with "LIMIT 100" if not specified.
# - If dates are referenced in natural language, use ANSI expressions (e.g., CURRENT_DATE).
# - When working with usernames or names consider case to be insensitive always.
# """
#         try:
#             resp = self.model.generate_content(prompt)
#             text = self._clean_llm_output(resp.text)
#             return text
#         except Exception as e:
#             logger.exception("LLM call failed in DataEngineerAgent: %s", e)
#             return ""

#     def process(self, input_dict):
#         """
#         input_dict keys:
#           - text: user question (string)
#           - clarification_history: optional list of previous clarifications (strings)
#         returns:
#           - if clarification needed:
#               {"clarification_needed": True, "clarification_question": "..."}
#           - if success:
#               {"success": True, "sql_query": "...", "dataframe": pd.DataFrame}
#           - if error:
#               {"success": False, "error": "..."}
#         """
#         user_text = input_dict.get("text", "")
#         clarification_history = input_dict.get("clarification_history", []) or []

#         # Ask LLM
#         llm_out = self._ask_llm_for_sql_or_clarify(user_text, clarification_history)

#         if not llm_out:
#             return {"success": False, "error": "Empty response from SQL generator."}

#         # First try to parse as JSON (clarification)
#         try:
#             # If it looks like JSON, parse
#             maybe_json = llm_out.strip()
#             if maybe_json.startswith("{") and maybe_json.endswith("}"):
#                 parsed = json.loads(maybe_json)
#                 if parsed.get("clarification_needed"):
#                     # Ensure clarification question is present
#                     question = parsed.get("clarification_question") or "Could you clarify your request?"
#                     return {"clarification_needed": True, "clarification_question": question}
#             # If not JSON, treat as SQL
#         except Exception:
#             # Not JSON — continue to treat as SQL
#             pass

#         # Treat output as SQL string
#         sql = llm_out.strip()
#         # If LLM prefixed "sql" or similar, strip it
#         if sql.lower().startswith("sql"):
#             sql = sql[3:].strip()
#         # Sanitize: remove leading/trailing backticks
#         sql = sql.strip("` \n")

#         # If SQL seems suspiciously short, ask for clarification instead of executing
#         if len(sql) < 6 or sql.lower().startswith(("select", "with", "insert", "update", "delete")) is False:
#             # fallback clarification
#             return {"clarification_needed": True, "clarification_question": "I couldn't generate a valid SQL. Could you rephrase or provide more details?"}

#         # Execute SQL
#         try:
#             df = run_query(sql)
#             # Ensure df is a DataFrame
#             if not isinstance(df, pd.DataFrame):
#                 df = pd.DataFrame(df)
#             return {"success": True, "sql_query": sql, "dataframe": df}
#         except Exception as e:
#             logger.exception("SQL execution failed: %s", e)
#             return {"success": False, "error": f"SQL execution failed: {str(e)}"}



# class ReasoningAgent:
#     def __init__(self, name):
#         self.name = name

#     def _summarize_df(self, df: pd.DataFrame) -> str:
#         """Generate a brief summary of the dataframe contents."""
#         if df is None or df.empty:
#             return "The query returned no results."

#         num_rows = len(df)
#         num_cols = len(df.columns)
#         summary = f"The query returned {num_rows} rows and {num_cols} columns."

#         # Briefly summarize numeric columns if present
#         numeric_cols = df.select_dtypes(include="number").columns.tolist()
#         if numeric_cols:
#             stats = df[numeric_cols].describe().iloc[1].round(2)  # mean values
#             stats_summary = ", ".join(f"avg {col} = {val}" for col, val in stats.items())
#             summary += " Key stats: " + stats_summary + "."
        
#         return summary

#     def process(self, input_dict):
#         user_query = input_dict.get('user_query', '')
#         df = input_dict.get('dataframe')
#         sql_query = input_dict.get('sql_query', '')

#         explanation_parts = [f"You asked: \"{user_query.strip()}\"."]

#         # Optionally include SQL query snippet or summary (shortened)
#         if sql_query:
#             sql_summary = sql_query.strip().replace("\n", " ")
#             if len(sql_summary) > 100:
#                 sql_summary = sql_summary[:97] + "..."
#             explanation_parts.append(f"The executed SQL query was: {sql_summary}")

#         # Add dataframe summary
#         explanation_parts.append(self._summarize_df(df))

#         # Combine and keep under 100 words (approximate)
#         explanation = " ".join(explanation_parts)
#         # Limit explanation length roughly: truncate to ~600 characters (~100 words)
#         if len(explanation) > 600:
#             explanation = explanation[:597] + "..."

#         return {"explanation": explanation}




# class VisualizationAgent:
#     def __init__(self, name):
#         self.name = name
#         self.model = genai.GenerativeModel('gemini-2.5-flash') 

#     def _get_schema_text(self, df):
#         """Generates a readable table/column list for the prompt."""
#         cols = ', '.join(list(df.columns))
#         return f"Columns: {cols}"

#     def process(self, input_dict):
#         df = input_dict.get('dataframe')
#         if df is None or df.empty:
#             return {"success": False, "error": "Visualization agent: No data to visualize."}

#         df_schema = self._get_schema_text(df)
#         user_query = input_dict.get('user_query', '')
#         clarification_history = input_dict.get('clarification_history', [])

#         # Combine clarifications into prompt context
#         clarifications_text = ""
#         if clarification_history:
#             clarifications_text = "\nAdditional clarifications provided by the user:\n" + \
#                                    "\n".join(f"- {c}" for c in clarification_history)

#         # Prompt for Gemini
#         prompt = f"""
#         You are a data visualization assistant. Given a user question and a dataset schema, do the following:
#         1. Infer the most appropriate chart type (e.g. line, bar, scatter, pie) and likely X and Y axis columns.
#         2. If the user query is ambiguous or missing details, suggest a clarifying question.
#         3. Output a structured JSON with the fields: 
#               {{
#                 "chart_type": "...",
#                 "x_col": "...",
#                 "y_col": "...",
#                 "clarification_needed": true/false,
#                 "clarification_question": "..." (if applicable)
#               }}
        
#         User query: "{user_query}"
#         Data schema:
#         {df_schema}
#         {clarifications_text}
        
#         Reply ONLY with the valid, minified JSON, NO other text or formatting.
#         """

#         try:
#             response = self.model.generate_content(prompt).text.strip()
#             # Remove triple backticks and language hints like ```json
#             response_clean = re.sub(r"^```[a-zA-Z]*\n|```$", "", response, flags=re.MULTILINE).strip()
#             structured = json.loads(response_clean)
#         except Exception as e:
#             return {
#                 "success": False,
#                 "error": f"Visualization agent: JSON parsing failed ({str(e)}). LLM output: {repr(response)}"
#             }

#         # If clarification is still needed, return without chart
#         if structured.get("clarification_needed"):
#             return structured

#         # Build chart if possible
#         chart_type = structured.get("chart_type")
#         x_col = structured.get("x_col")
#         y_col = structured.get("y_col")

#         # Validate columns
#         if x_col not in df.columns or y_col not in df.columns:
#             return {
#                 "success": False,
#                 "error": f"Visualization agent: Invalid columns chosen (x: {x_col}, y: {y_col}).",
#                 **structured
#             }

#         fig = None
#         try:
#             if chart_type == "bar":
#                 fig = px.bar(df, x=x_col, y=y_col)
#             elif chart_type == "line":
#                 fig = px.line(df, x=x_col, y=y_col)
#             elif chart_type == "scatter":
#                 fig = px.scatter(df, x=x_col, y=y_col)
#             elif chart_type == "pie":
#                 fig = px.pie(df, names=x_col, values=y_col)
#             else:
#                 fig = px.bar(df, x=x_col, y=y_col)  # fallback
#         except Exception as e:
#             return {
#                 "success": False,
#                 "error": f"Visualization agent: Failed to create chart ({str(e)}).",
#                 **structured
#             }

#         structured["fig"] = fig
#         structured["success"] = True
#         return structured


#-------------------------------------------- Refactor Logic --------------------------------------------

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import google.generativeai as genai
import pandas as pd
import plotly.express as px
import toml

from utils import run_query

# --- Configuration & logging ---
config = toml.load("config.toml")
api_key = config.get("gemini_api_key")
if not api_key:
    raise ValueError("Google Gemini API key not found in config.toml")

genai.configure(api_key=api_key)

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------- Helpers ----------
CODE_FENCE_RE = re.compile(r"^```[a-zA-Z0-9]*\n|```$", flags=re.MULTILINE)
SQL_STARTS = ("select", "with", "insert", "update", "delete")


def _strip_code_fences(text: str) -> str:
    if not text:
        return ""
    return CODE_FENCE_RE.sub("", text.strip()).strip()


def _looks_like_sql(s: str) -> bool:
    s = s.strip().lower()
    return len(s) >= 6 and any(s.startswith(k) for k in SQL_STARTS)


@dataclass
class AgentResult:
    success: bool = False
    clarification_needed: bool = False
    clarification_question: Optional[str] = None
    sql_query: Optional[str] = None
    dataframe: Optional[pd.DataFrame] = None
    explanation: Optional[str] = None
    fig: Any = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d = {
            "success": self.success,
            "clarification_needed": self.clarification_needed,
            "clarification_question": self.clarification_question,
            "sql_query": self.sql_query,
            "dataframe": self.dataframe,
            "explanation": self.explanation,
            "fig": self.fig,
            "error": self.error,
        }
        # remove Nones for cleanliness
        return {k: v for k, v in d.items() if v is not None}


# ---------- DataEngineerAgent ----------
class DataEngineerAgent:
    """NL→SQL agent that returns SQL+data or asks for a concrete clarification."""

    def __init__(self, name: str, schema: Dict[str, List[str]], model_name: str = "gemini-2.5-flash"):
        self.name = name
        self.schema = schema or {}
        self.model = genai.GenerativeModel(model_name)

    # -- prompt utils --
    def _format_schema(self) -> str:
        parts = []
        for table, cols in (self.schema or {}).items():
            cols_text = ", ".join(map(str, (cols or [])))
            parts.append(f"Table: {table}\nColumns: {cols_text}")
        return "\n\n".join(parts)

    def _ask_llm_for_sql_or_clarify(self, user_text: str, clar_hist: Optional[List[str]] = None) -> str:
        schema_text = self._format_schema()
        clar_hist_text = "\n\nPrevious clarifications:\n" + "\n".join(f"- {c}" for c in clar_hist) if clar_hist else ""
        prompt = f"""
You are an expert NL-to-SQL assistant. Given a relational schema and a user's question, either:
1) Return a single executable ANSI SQL statement (no explanation, no backticks), OR
2) If the user's request is ambiguous or missing critical details needed to generate correct SQL, return ONLY this minified JSON object:
   {{"clarification_needed":true, "clarification_question":"..."}}
The clarification question must be specific and, when possible, list available columns from the schema.

Schema:\n{schema_text}
User question:\n{user_text}
{clar_hist_text}

Rules:
- If returning SQL: output only the SQL, nothing else.
- Prefer clear, unambiguous SQL. Add "LIMIT 100" if not specified.
- Use ANSI for dates (e.g., CURRENT_DATE).
- Treat names/usernames as case-insensitive.
"""
        try:
            resp = self.model.generate_content(prompt)
            return _strip_code_fences(resp.text)
        except Exception as e:
            logger.exception("LLM call failed in DataEngineerAgent: %s", e)
            return ""

    # -- main --
    def process(self, input_dict: Dict[str, Any]) -> Dict[str, Any]:
        user_text = (input_dict or {}).get("text", "")
        clarification_history = (input_dict or {}).get("clarification_history", []) or []

        llm_out = self._ask_llm_for_sql_or_clarify(user_text, clarification_history)
        if not llm_out:
            return AgentResult(success=False, error="Empty response from SQL generator.").to_dict()

        # Try to interpret as a clarification request
        maybe = llm_out.strip()
        if maybe.startswith("{") and maybe.endswith("}"):
            try:
                parsed = json.loads(maybe)
                if parsed.get("clarification_needed"):
                    question = parsed.get("clarification_question") or "Could you clarify your request?"
                    return AgentResult(clarification_needed=True, clarification_question=question).to_dict()
            except Exception:
                pass  # not valid JSON → fall back to treating as SQL

        # Treat as SQL
        sql = maybe
        if sql.lower().startswith("sql"):
            sql = sql[3:].strip()
        sql = sql.strip("` \n")
        if not _looks_like_sql(sql):
            return AgentResult(clarification_needed=True, clarification_question="I couldn't generate valid SQL. Could you rephrase or provide more details?").to_dict()

        try:
            df = run_query(sql)
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(df)
            return AgentResult(success=True, sql_query=sql, dataframe=df).to_dict()
        except Exception as e:
            logger.exception("SQL execution failed: %s", e)
            return AgentResult(success=False, error=f"SQL execution failed: {e}").to_dict()


# ---------- ReasoningAgent ----------
class ReasoningAgent:
    def __init__(self, name: str):
        self.name = name

    @staticmethod
    def _summarize_df(df: Optional[pd.DataFrame]) -> str:
        if df is None or df.empty:
            return "The query returned no results."
        num_rows, num_cols = df.shape
        summary = f"The query returned {num_rows} rows and {num_cols} columns."
        numeric_cols = df.select_dtypes(include="number").columns.tolist()
        if numeric_cols:
            means = df[numeric_cols].mean(numeric_only=True).round(2).to_dict()
            stats_summary = ", ".join(f"avg {k} = {v}" for k, v in means.items())
            summary += " Key stats: " + stats_summary + "."
        return summary

    def process(self, input_dict: Dict[str, Any]) -> Dict[str, Any]:
        user_query = (input_dict or {}).get("user_query", "").strip()
        df = (input_dict or {}).get("dataframe")
        sql_query = (input_dict or {}).get("sql_query", "").strip()

        parts = [f'You asked: "{user_query}".'] if user_query else []
        if sql_query:
            sql_summary = sql_query.replace("\n", " ")
            if len(sql_summary) > 100:
                sql_summary = sql_summary[:97] + "..."
            parts.append(f"The executed SQL query was: {sql_summary}")
        parts.append(self._summarize_df(df))

        explanation = " ".join(parts)
        if len(explanation) > 600:
            explanation = explanation[:597] + "..."
        return AgentResult(explanation=explanation).to_dict()


# ---------- VisualizationAgent ----------
class VisualizationAgent:
    def __init__(self, name: str, model_name: str = "gemini-2.5-flash"):
        self.name = name
        self.model = genai.GenerativeModel(model_name)

    @staticmethod
    def _schema_from_df(df: pd.DataFrame) -> str:
        return "Columns: " + ", ".join(map(str, list(df.columns)))

    def process(self, input_dict: Dict[str, Any]) -> Dict[str, Any]:
        df: Optional[pd.DataFrame] = (input_dict or {}).get("dataframe")
        if df is None or df.empty:
            return AgentResult(success=False, error="Visualization agent: No data to visualize.").to_dict()

        user_query = (input_dict or {}).get("user_query", "")
        clarification_history: List[str] = (input_dict or {}).get("clarification_history", []) or []

        clar_text = "\nAdditional clarifications provided by the user:\n" + "\n".join(
            f"- {c}" for c in clarification_history
        ) if clarification_history else ""

        prompt = f"""
You are a data visualization assistant. Given a user question and a dataset schema, do the following:
1. Infer the most appropriate chart type (e.g. line, bar, scatter, pie) and likely X and Y axis columns.
2. If the user query is ambiguous or missing details, suggest a clarifying question.
3. Output ONLY this minified JSON object:
   {{"chart_type":"...","x_col":"...","y_col":"...","clarification_needed":false,"clarification_question":""}}

User query: "{user_query}"
Data schema:
{self._schema_from_df(df)}
{clar_text}
"""
        try:
            response = self.model.generate_content(prompt).text.strip()
            response = _strip_code_fences(response)
            structured = json.loads(response)
        except Exception as e:
            return AgentResult(success=False, error=f"Visualization agent: JSON parsing failed ({e}). LLM output: {response if 'response' in locals() else ''}").to_dict()

        if structured.get("clarification_needed"):
            return AgentResult(clarification_needed=True, clarification_question=structured.get("clarification_question") or "Could you clarify?").to_dict()

        chart_type = structured.get("chart_type")
        x_col = structured.get("x_col")
        y_col = structured.get("y_col")
        if x_col not in df.columns or y_col not in df.columns:
            return AgentResult(success=False, error=f"Visualization agent: Invalid columns chosen (x: {x_col}, y: {y_col}).").to_dict()

        try:
            if chart_type == "line":
                fig = px.line(df, x=x_col, y=y_col)
            elif chart_type == "scatter":
                fig = px.scatter(df, x=x_col, y=y_col)
            elif chart_type == "pie":
                fig = px.pie(df, names=x_col, values=y_col)
            else:
                fig = px.bar(df, x=x_col, y=y_col)  # default/fallback
        except Exception as e:
            return AgentResult(success=False, error=f"Visualization agent: Failed to create chart ({e}).").to_dict()

        return AgentResult(success=True, fig=fig).to_dict()

