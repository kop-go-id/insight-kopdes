# main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
from dotenv import load_dotenv
from chains.query_chain import run_query_pipeline

load_dotenv()

app = FastAPI(title="LangChain SQL Assistant")

class QueryRequest(BaseModel):
    question: str
    user_id: Optional[int] = None


@app.post("/chat")
async def chat(req: QueryRequest):
    """
    Primary route for structured dashboard output.
    Returns a JSON object with summarized text + structured payload.
    """
    try:
        result = run_query_pipeline(req.question, req.user_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/chat/humanized")
async def chat_humanized(req: QueryRequest):
    """
    Secondary route for plain Indonesian language output.
    Intended for minister-level summaries.
    """
    try:
        result = run_query_pipeline(req.question, req.user_id)

        # Format a more readable, plain-text answer
        if "error" in result:
            return {"answer": "Terjadi kesalahan: " + result["error"]}

        text = result.get("text", "Tidak ada data yang ditemukan.")
        sql = result.get("meta", {}).get("generated_sql", "N/A")
        return {"answer": f"{text}"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
def health():
    """Simple health check route."""
    return {"status": "ok"}