# Running the Reddit Book Scraper Pipeline

All commands are run from the project root (`Book-Scraper/`) unless stated otherwise.

---

## 1. Infrastructure (once, run from inside `reddit_pipeline/`)

```bash
cd reddit_pipeline && docker compose up -d && cd ..
```

---

## 2. Airflow Scheduler — Terminal 1

```bash
export AIRFLOW_HOME=$(pwd)/reddit_pipeline
airflow scheduler
```

---

## 3. Airflow API Server — Terminal 2

```bash
export AIRFLOW_HOME=$(pwd)/reddit_pipeline
airflow api-server
```

UI available at: `http://localhost:8080`

---

## 4. MongoDB Consumer — Terminal 3

```bash
source .venv/bin/activate
python reddit_pipeline/consumers/mongo_consumer.py
```

---

## 5. Elasticsearch Consumer — Terminal 4

```bash
source .venv/bin/activate
python reddit_pipeline/consumers/elasticsearch_consumer.py
```

---

## 6. Trigger the DAG — any terminal with AIRFLOW_HOME set

```bash

export AIRFLOW_HOME=$(pwd)/reddit_pipeline
airflow dags unpause scrape_books_pipeline
airflow dags trigger scrape_books_pipeline
```

---

## Notes

- The first DAG run seeds posts into Kafka. Trigger a **second run** after the MongoDB consumer logs `inserted post ...` to scrape comments and users.
- If the DAG stays queued, check the scheduler terminal is running.
- If MongoDB Compass on Windows won't connect via `localhost`, use the WSL2 IP instead: run `ip addr show eth0` in WSL2 to find it, then connect to `mongodb://<ip>:27017`.
- `AIRFLOW_HOME` must be exported in every terminal that runs an Airflow command. To set it permanently: `echo 'export AIRFLOW_HOME=/home/vai/VScode-Projs/Python/Book-Scraper/reddit_pipeline' >> ~/.bashrc`
