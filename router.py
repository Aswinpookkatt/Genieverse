# # router.py
# import re
# from typing import Optional, Dict

# class Router:
#     """
#     Simple deterministic router: returns one of
#     'data_engineer', 'visualization', 'reasoning', 'multi'
#     """
#     def __init__(self):
#         # tweak these keyword sets to match your users' phrasing
#         self.visual_keywords = {
#             "plot", "chart", "visualize", "visualisation", "visualization",
#             "show", "graph", "bar", "line", "scatter", "pie", "hist", "vs","create"
#         }
#         self.sql_keywords = {
#             "show me", "select", "how many", "count", "sum", "total", "avg", "average",
#             "group by", "orders", "sales", "customers", "products", "where", "between",
#             "top", "limit", "rows", "per","display"
#         }
#         self.reason_keywords = {
#             "explain", "what does", "why", "summary", "describe", "insight", "insights",
#             "help me understand", "interpret", "trend"
#         }

#     def _contains_any(self, text: str, keywords: set) -> bool:
#         t = (text or "").lower()
#         for k in keywords:
#             if k in t:
#                 return True
#         return False

#     def predict(self, prompt: str, df_columns: Optional[list] = None) -> Dict:
#         p = (prompt or "").strip().lower()

#         # 1) Strong sql-like phrases -> data_engineer or multi
#         if self._contains_any(p, self.sql_keywords):
#             if self._contains_any(p, self.visual_keywords):
#                 return {"route": "multi", "confidence": 0.9, "explanation": "SQL + visualization keywords"}
#             return {"route": "data_engineer", "confidence": 0.85, "explanation": "SQL/aggregation keywords"}

#         # 2) Visualization-first phrasing
#         if self._contains_any(p, self.visual_keywords):
#             # if also reasoning words, prefer reasoning
#             if self._contains_any(p, self.reason_keywords) and not self._contains_any(p, self.sql_keywords):
#                 return {"route": "reasoning", "confidence": 0.75, "explanation": "visual + reasoning words -> reasoning"}
#             return {"route": "visualization", "confidence": 0.9, "explanation": "visualization keywords"}

#         # 3) Reasoning words (no sql/visual)
#         if self._contains_any(p, self.reason_keywords):
#             return {"route": "reasoning", "confidence": 0.9, "explanation": "reasoning keywords"}

#         # 4) Short/ambiguous prompts: prefer visualization if dataframe exists
#         if df_columns:
#             if len(p.split()) <= 3:
#                 return {"route": "visualization", "confidence": 0.55, "explanation": "short ambiguous -> default to visualization"}

#         # 5) Default fallback: run full pipeline
#         return {"route": "multi", "confidence": 0.45, "explanation": "fallback to multi pipeline"}


#--------------------------------------------- Refactor Logic -------------------------------------------------------------


from typing import Optional, Dict
import re


class Router:
    """Deterministic keyword router: 'data_engineer' | 'visualization' | 'reasoning' | 'multi'"""

    def __init__(self):
        # Keyword lists are matched as word fragments (case-insensitive)
        self.visual_keywords = {
            "plot", "chart", "visualize", "visualisation", "visualization",
            "show", "graph", "bar", "line", "scatter", "pie", "hist", "vs", "create",
        }
        self.sql_keywords = {
            "show me", "select", "how many", "count", "sum", "total", "avg", "average",
            "group by", "orders", "sales", "customers", "products", "where", "between",
            "top", "limit", "rows", "per", "display",
        }
        self.reason_keywords = {
            "explain", "what does", "why", "summary", "describe", "insight", "insights",
            "help me understand", "interpret", "trend",
        }

    @staticmethod
    def _contains_any(text: str, keywords: set) -> bool:
        t = (text or "").lower()
        return any(k in t for k in keywords)

    def predict(self, prompt: str, df_columns: Optional[list] = None) -> Dict:
        p = (prompt or "").strip().lower()

        if self._contains_any(p, self.sql_keywords):
            if self._contains_any(p, self.visual_keywords):
                return {"route": "multi", "confidence": 0.9, "explanation": "SQL + visualization keywords"}
            return {"route": "data_engineer", "confidence": 0.85, "explanation": "SQL/aggregation keywords"}

        if self._contains_any(p, self.visual_keywords):
            if self._contains_any(p, self.reason_keywords) and not self._contains_any(p, self.sql_keywords):
                return {"route": "reasoning", "confidence": 0.75, "explanation": "visual + reasoning words -> reasoning"}
            return {"route": "visualization", "confidence": 0.9, "explanation": "visualization keywords"}

        if self._contains_any(p, self.reason_keywords):
            return {"route": "reasoning", "confidence": 0.9, "explanation": "reasoning keywords"}

        if df_columns and len(p.split()) <= 3:
            return {"route": "visualization", "confidence": 0.55, "explanation": "short ambiguous -> default to visualization"}

        return {"route": "multi", "confidence": 0.45, "explanation": "fallback to multi pipeline"}
