from __future__ import annotations

import json
import logging
import re
import time
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

    def __init__(self, name: str, schema: Dict[str, List[str]], model_name: str = "gemini-2.5-pro"):
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
⚠️ CRITICAL: When using SUM, COUNT, AVG or any aggregate function with other columns, ALL non-aggregate columns MUST be in GROUP BY clause!

You are an expert NL-to-SQL assistant. Given a relational schema and a user's question, either:
1) Return a single executable ANSI SQL statement (no explanation, no backticks), OR
2) If the user's request is ambiguous or missing critical details needed to generate correct SQL, return ONLY this minified JSON object:
   {{"clarification_needed":true, "clarification_question":"..."}}
The clarification question must be specific and, when possible, list available columns from the schema.
Clarification questions must also be understandable to non-technical users — Never use terms like "schema", "columns", "SQL", or "database". 
Instead, use plain English and refer to them as "fields" or "parts of your data", and whenever possible directly list the available options.
4) When generating SQL queries involving aggregation functions (such as AVG(), SUM(), MIN(), MAX()), if the column used is not of a numeric type, add an explicit cast to DOUBLE. For example, use AVG(CAST(discounted_price AS DOUBLE)) instead of AVG(discounted_price) if discounted_price is stored as VARCHAR.
5) CRITICAL: When using GROUP BY, ensure ALL non-aggregate columns in SELECT appear in GROUP BY clause. If you need a column that varies within groups, use ANY_VALUE(column_name) or appropriate aggregate function.
6) For queries requesting multiple different products or records based on different criteria (like "highest rating AND highest rating count"), use UNION ALL to combine results. Each SELECT should be in parentheses and include a descriptive label column.
7) When dealing with "highest" or "top" queries, use proper ORDER BY and LIMIT clauses.
8) For rating-related queries, remember that rating_count refers to the number of reviews, while rating refers to the average rating score.
9) Common GROUP BY patterns:
   - SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id
   - SELECT category, ANY_VALUE(name), COUNT(*) FROM products GROUP BY category
   - SELECT customer_id, ANY_VALUE(name), SUM(total_amount) FROM orders JOIN customers USING(customer_id) GROUP BY customer_id
   - CUSTOMER SPENDING: SELECT c.customer_id, c.name, c.email, SUM(o.total_amount) as total_spent FROM customers c JOIN orders o ON c.customer_id = o.customer_id GROUP BY c.customer_id, c.name, c.email ORDER BY total_spent DESC

⚠️ NEVER generate: SELECT T1.name, T1.email, SUM(...) FROM customers T1 JOIN orders T2... GROUP BY T1.customer_id
✅ ALWAYS generate: SELECT c.customer_id, c.name, c.email, SUM(...) FROM customers c JOIN orders o... GROUP BY c.customer_id, c.name, c.email

Schema:\n{schema_text}
User question:\n{user_text}
{clar_hist_text}

Example for multiple criteria queries:
User: "Show the product with highest rating and the product with highest rating count"
Expected SQL:
(SELECT product_name, CAST(rating AS DOUBLE) as rating, CAST(rating_count AS DOUBLE) as rating_count, 'Highest Rating' as criteria 
 FROM Sales 
 ORDER BY CAST(rating AS DOUBLE) DESC 
 LIMIT 1)
UNION ALL
(SELECT product_name, CAST(rating AS DOUBLE) as rating, CAST(rating_count AS DOUBLE) as rating_count, 'Most Reviews' as criteria 
 FROM Sales 
 ORDER BY CAST(rating_count AS DOUBLE) DESC 
 LIMIT 1)

Example for customer spending queries:
User: "show the list of users along with email and how much they spend in desc order"
Expected SQL:
SELECT c.customer_id, c.name, c.email, SUM(o.total_amount) as total_spent 
FROM customers c 
JOIN orders o ON c.customer_id = o.customer_id 
GROUP BY c.customer_id, c.name, c.email 
ORDER BY total_spent DESC

IMPORTANT: For customer spending queries, use this EXACT template:
SELECT c.customer_id, c.name, c.email, SUM(o.total_amount) as total_spent
FROM customers c 
JOIN orders o ON c.customer_id = o.customer_id 
GROUP BY c.customer_id, c.name, c.email 
ORDER BY total_spent DESC LIMIT 100

Rules:
- If returning SQL: output only the SQL, nothing else.
- Prefer clear, unambiguous SQL. Add "LIMIT 100" if not specified.
- Use ANSI for dates (e.g., CURRENT_DATE).
- Treat names/usernames or category as case-insensitive.
- MANDATORY GROUP BY RULE: When SELECT contains both aggregate functions (SUM, COUNT, AVG, MAX, MIN) AND non-aggregate columns, you MUST include ALL non-aggregate columns in the GROUP BY clause.
- EXAMPLE ERROR: "SELECT name, email, SUM(amount) FROM ... GROUP BY customer_id" ❌ WRONG - missing name, email in GROUP BY
- EXAMPLE CORRECT: "SELECT name, email, SUM(amount) FROM ... GROUP BY customer_id, name, email" ✅ CORRECT
- For ANY query showing customer details with spending totals, ALWAYS use this exact pattern:
  SELECT c.customer_id, c.name, c.email, SUM(...) as total_spent FROM customers c JOIN orders o ON c.customer_id = o.customer_id GROUP BY c.customer_id, c.name, c.email ORDER BY total_spent DESC
"""
        return self._call_api_with_retry(prompt)
    
    def _call_api_with_retry(self, prompt: str, max_retries: int = 3) -> str:
        """Call Gemini API with retry logic for better reliability"""
        for attempt in range(max_retries):
            try:
                # Add small delay between retries
                if attempt > 0:
                    time.sleep(2 ** attempt)  # Exponential backoff: 2, 4, 8 seconds
                
                resp = self.model.generate_content(prompt)
                
                # Better error handling for Gemini API responses
                if not resp or not resp.candidates:
                    logger.error(f"No response candidates returned from Gemini API (attempt {attempt + 1})")
                    if attempt == max_retries - 1:
                        return "I apologize, but I'm unable to process your request right now. Please try rephrasing your question."
                    continue
                
                candidate = resp.candidates[0]
                
                # Check finish reason
                if candidate.finish_reason == 1:  # STOP - normal completion
                    if candidate.content and candidate.content.parts:
                        return _strip_code_fences(candidate.content.parts[0].text)
                    else:
                        logger.error("Empty content in API response")
                        if attempt == max_retries - 1:
                            return "I need more information to generate a proper SQL query. Could you provide more details about what data you're looking for?"
                        continue
                
                elif candidate.finish_reason == 2:  # MAX_TOKENS
                    logger.error("Response truncated due to max tokens")
                    if attempt == max_retries - 1:
                        return "The query is too complex. Please try breaking it into smaller parts."
                    continue
                
                elif candidate.finish_reason == 3:  # SAFETY
                    logger.error("Response blocked by safety filters")
                    return "I cannot process this request due to safety restrictions. Please rephrase your question."
                
                elif candidate.finish_reason == 4:  # RECITATION
                    logger.error("Response blocked due to recitation")
                    return "Please rephrase your question in a different way."
                
                else:
                    logger.error(f"Unknown finish reason: {candidate.finish_reason}")
                    if attempt == max_retries - 1:
                        return "I encountered an issue processing your request. Please try again."
                    continue
                    
            except Exception as e:
                logger.exception(f"LLM call failed in DataEngineerAgent (attempt {attempt + 1}): %s", e)
                
                # Handle specific error types
                if "500" in str(e) or "Internal" in str(e):
                    if attempt == max_retries - 1:
                        return "The AI service is temporarily unavailable. Please try again in a moment."
                    continue  # Retry for server errors
                elif "quota" in str(e).lower() or "limit" in str(e).lower():
                    return "API quota exceeded. Please try again later."
                elif "safety" in str(e).lower():
                    return "Your request was filtered for safety. Please rephrase your question."
                else:
                    if attempt == max_retries - 1:
                        return "I'm experiencing technical difficulties. Please try again or rephrase your question."
                    continue  # Retry for other errors
        
        return "Unable to process request after multiple attempts. Please try again later."    # -- main --
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
            error_msg = str(e).lower()
            if "rating" in error_msg or "union" in error_msg:
                return AgentResult(
                    clarification_needed=True, 
                    clarification_question="I'm having trouble with the rating query. Could you try asking for either 'the highest rated product' or 'the product with most reviews' separately?"
                ).to_dict()
            return AgentResult(success=False, error=f"SQL execution failed: {e}").to_dict()


# ---------- ReasoningAgent ----------
# class ReasoningAgent:
#     def __init__(self, name: str):
#         self.name = name

#     @staticmethod
#     def _summarize_df(df: Optional[pd.DataFrame]) -> str:
#         if df is None or df.empty:
#             return "The query returned no results."
#         num_rows, num_cols = df.shape
#         summary = f"The query returned {num_rows} rows and {num_cols} columns."
#         numeric_cols = df.select_dtypes(include="number").columns.tolist()
#         if numeric_cols:
#             means = df[numeric_cols].mean(numeric_only=True).round(2).to_dict()
#             stats_summary = ", ".join(f"avg {k} = {v}" for k, v in means.items())
#             summary += " Key stats: " + stats_summary + "."
#         return summary

#     def process(self, input_dict: Dict[str, Any]) -> Dict[str, Any]:
#         user_query = (input_dict or {}).get("user_query", "").strip()
#         df = (input_dict or {}).get("dataframe")
#         sql_query = (input_dict or {}).get("sql_query", "").strip()

#         parts = [f'You asked: "{user_query}".'] if user_query else []
#         if sql_query:
#             sql_summary = sql_query.replace("\n", " ")
#             if len(sql_summary) > 100:
#                 sql_summary = sql_summary[:97] + "..."
#             parts.append(f"The executed SQL query was: {sql_summary}")
#         parts.append(self._summarize_df(df))

#         explanation = " ".join(parts)
#         if len(explanation) > 600:
#             explanation = explanation[:597] + "..."
#         return AgentResult(explanation=explanation).to_dict()


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
1. Infer the most appropriate chart type (e.g. line, bar, scatter, pie, stacked_bar) and likely X and Y axis columns.
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
            logger.info("response json  :\n%s", structured)
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

