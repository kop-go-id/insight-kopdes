# chains/summarizer.py
from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage, SystemMessage
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")

# Initialize the OpenAI model
llm = ChatOpenAI(model=LLM_MODEL, temperature=0)

def summarize_for_minister(question: str, sample_rows: list | None, top_n: int = 5) -> str:
    """
    Generate a concise summary in Indonesian for high-level reporting.
    The summary must be entirely based on actual database results.
    """
    sample_text = ""
    if sample_rows:
        snippet = sample_rows[:top_n]
        sample_text = f"Contoh data (maks {top_n} baris):\n{snippet}\n\n"

    system = SystemMessage(
        content=(
            "You are an assistant that summarizes factual database results in formal Indonesian. "
            "Always base the summary on the provided data. Never guess, estimate, or assume numbers. "
            "Keep responses short (2–4 sentences) and suitable for ministerial or executive audiences."
        )
    )

    human = HumanMessage(
        content=(
            f"Pertanyaan: {question}\n\n"
            f"{sample_text}"
            "Buat ringkasan singkat dalam bahasa Indonesia berdasarkan data di atas."
        )
    )

    response = llm.invoke([system, human])
    return response.content.strip()