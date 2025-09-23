import psycopg2
import os
from fastapi import FastAPI
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer
import uvicorn


DB_CONFIG = {
    "host": os.getenv("DB_HOST", "db"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}

app = FastAPI()
model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")

class SearchQuery(BaseModel):
    query: str
    top_k: int = 5

@app.post("/search")
def semantic_search(q: SearchQuery):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    query_emb = model.encode(q.query).tolist()
    cur.execute("""
        SELECT article_id, title, source_url,
               (embedding <=> %s) AS distance
        FROM dw.dim_articles
        WHERE embedding IS NOT NULL
        ORDER BY distance ASC
        LIMIT %s;
    """, (str(query_emb), q.top_k))
    results = [{"id": row[0], "title": row[1], "url": row[2], "distance": row[3]} for row in cur.fetchall()]

    cur.close()
    conn.close()
    return {"results": results}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)