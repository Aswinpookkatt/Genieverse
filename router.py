from typing import Optional, Dict
import re


class Router:
    """Deterministic keyword router: 'data_engineer' | 'visualization' | 'multi'"""

    def __init__(self):
        # Keyword lists are matched as word fragments (case-insensitive)
        self.visual_keywords = {
            "plot", "chart", "visualize",'visualise', "visualisation", "visualization",
            "generate", "graph", "bar", "line", "scatter", "pie", "hist", "vs", "create","scatter"
        }
        self.sql_keywords = {
            "show", "select", "how many", "count", "sum", "total", "avg", "average",
            "group by", "orders", "sales", "customers", "products", "where", "between",
            "top", "limit", "rows", "per", "display"
        }
       
    @staticmethod
    def _contains_any(text: str, keywords: set) -> bool:
        t = (text or "").lower()
        return any(k in t for k in keywords)

    def predict(self, prompt: str, df_columns: Optional[list] = None) -> Dict:
        p = (prompt or "").strip().lower()

        if self._contains_any(p, self.visual_keywords):
            return {
                "route": "visualization", 
                "confidence": 0.95, 
                "explanation": "visualization keywords"
                }


        if self._contains_any(p, self.sql_keywords):
            # if self._contains_any(p, self.visual_keywords):
            #     return {"route": "multi", "confidence": 0.9, "explanation": "SQL + visualization keywords"}
            return {
                "route": "data_engineer",
                "confidence": 0.85, 
                "explanation": "SQL/aggregation keywords"
                }

        
        if df_columns and len(p.split()) <= 3:
            return {
                "route": "visualization", 
                "confidence": 0.55, 
                "explanation": "short ambiguous -> default to visualization"
                }

        return {
            "clarification_needed": True,
            "question": "Do you want to see a visualization (chart) or raw data (table)?",
            "explanation": "Could not confidently route"
        }
