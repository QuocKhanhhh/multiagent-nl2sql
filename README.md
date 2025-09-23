```markdown
# NL2SQL Analytics API

A FastAPI service that allows non-technical users to query a **data warehouse** in natural Vietnamese.  
Users can ask questions in plain language, the system generates and validates SQL with a **multi-agent LLM pipeline**, executes it on PostgreSQL, and returns both raw results and a concise human-readable summary.

---

## ✨ Features
- **Natural Language → SQL**: Automatically converts Vietnamese questions into valid SQL.
- **Multi-Agent Pipeline**:  
  - *Deconstructor* → logical plan  
  - *Planner* → SQL generation  
  - *Validator/Corrector* → schema check & auto-repair.
- **Result Summarization**: Uses LLM to produce short, friendly answers in Vietnamese.
- **Robust Validation**: Detects unsupported functions, invalid columns, and GROUP BY issues.
- **Containerized**: Ready for deployment with Docker & `docker-compose`.


## 🚀 Quick Start

### Prerequisites
- **Python 3.11+**
- **PostgreSQL** with your data warehouse schema.
- [Docker & Docker Compose](https://docs.docker.com/) (for containerized deployment).
- [Ollama](https://ollama.ai/) running a model such as `mistral:7b`.

### 1️⃣ Clone & Configure
```bash
git clone https://github.com/QuocKhanhhh/multiagent-nl2sql.git 
````

Set environment variables as needed (database credentials, etc.).

### 2️⃣ Run with Docker Compose

```bash
docker-compose up --build
```

The API will be available at: `http://localhost:8002`

### 3️⃣ Test the API

POST a question:

```bash
curl -X POST http://localhost:8002/ask \
     -H "Content-Type: application/json" \
     -d '{"question": "Có bao nhiêu bài viết có cảm xúc tích cực?"}'
```

Response example:

```json
{
  "sql": "SELECT COUNT(*) FROM dw.fact_articles ... ;",
  "analysis": "Có 123 bài viết có cảm xúc tích cực.",
  "raw_result": { "columns": ["count"], "rows": [[123]] },
  "sql_success": true,
  "corrections": []
}
```

---

## ⚙️ Configuration

Environment variables (with defaults):

| Variable          | Default    | Description             |
| ----------------- | ---------- | ----------------------- |
| DB\_NAME          | postgres   | Database name           |
| DB\_USER          | postgres   | Database user           |
| DB\_PASS          | postgres   | Database password       |
| DB\_HOST          | localhost  | Database host           |
| DB\_PORT          | 5432       | Database port           |
| SUMMARIZER\_MODEL | mistral:7b | LLM model for summaries |

Update `semantic_model.yaml` with your warehouse tables and column descriptions to guide SQL generation.

---

## 🛠 Tech Stack

* **FastAPI** for the REST API
* **PostgreSQL** as the data warehouse
* **Ollama + Mistral** for LLM-based SQL generation and summarization
* **Python** (requests, sqlparse, pydantic, etc.)
* **Docker / docker-compose** for deployment

---

## 📄 License

MIT License – feel free to use and modify.

---

## 🤝 Contributing

Pull requests and issues are welcome!
Please open an issue first to discuss major changes.

---

## 🙌 Acknowledgements

Inspired by the need for **self-service analytics** enabling non-technical users to query complex data warehouses with natural language.

```

This README:

- Explains **purpose and features**.
- Shows **file roles** (analytics_api, nl2sql_generator, sql_validate, semantic_model.yaml, Dockerfile, docker-compose).
- Provides **setup instructions** for both Docker and local development.
- Includes environment variables and example API call.

```
