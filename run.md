# Running the Reddit Book Scraper Pipeline

All commands are run from the project root (`Book-Scraper/`) unless stated otherwise.

---

## Quick Start

```bash
./start.sh
```

Clears Redis, starts Docker, and opens a tmux session with all 4 processes in a 2x2 grid. Then trigger the DAGs manually (step 6).

---

## Manual Start

### 1. Infrastructure (run from inside `reddit_pipeline/`)

```bash
cd reddit_pipeline && docker compose up -d && cd ..
```

---

### 2. Airflow Scheduler — Terminal 1

```bash
export AIRFLOW_HOME=$(pwd)/reddit_pipeline
airflow scheduler
```

---

### 3. Airflow API Server — Terminal 2

```bash
export AIRFLOW_HOME=$(pwd)/reddit_pipeline
airflow api-server
```

UI available at: `http://localhost:8080`

---

### 4. MongoDB Consumer — Terminal 3

```bash
source .venv/bin/activate
python reddit_pipeline/consumers/mongo_consumer.py
```

---

### 5. Elasticsearch Consumer — Terminal 4

```bash
source .venv/bin/activate
python reddit_pipeline/consumers/elasticsearch_consumer.py
```

---

### 6. Trigger the DAGs — any terminal with AIRFLOW_HOME set

```bash
export AIRFLOW_HOME=$(pwd)/reddit_pipeline
airflow dags unpause scrape_posts_comments
airflow dags unpause scrape_users
airflow dags trigger scrape_posts_comments
airflow dags trigger scrape_users
```

---

## DAG Overview

| DAG | Schedule | What it does |
|---|---|---|
| `scrape_posts_comments` | every 30 min | Scrapes r/books posts (paginated) and their comments. Inserts comment authors into `users` with `scraped: false`. |
| `scrape_users` | every 30 min | Queries `users` where `scraped: false`, fetches their Reddit post/comment history, marks `scraped: true`. |

**Expected run order:**
1. `scrape_posts_comments` runs → posts land in Kafka → MongoDB consumer writes them
2. `scrape_posts_comments` runs again → comments scraped → author stubs created in `users`
3. `scrape_users` runs → picks up stubs, fetches full user data

---

## Notes

- The first `scrape_posts_comments` run seeds posts. Trigger a **second run** after the MongoDB consumer logs `inserted post ...` so comments get scraped and author stubs are created.
- `scrape_users` only has work to do after `scrape_posts_comments` has scraped at least one round of comments.
- If a DAG stays queued, check the scheduler terminal is running.
- If MongoDB Compass on Windows won't connect via `localhost`, use the WSL2 IP instead: run `ip addr show eth0` in WSL2 to find it, then connect to `mongodb://<ip>:27017`.
- `AIRFLOW_HOME` must be exported in every terminal that runs an Airflow command. To set it permanently: `echo 'export AIRFLOW_HOME=/home/vai/VScode-Projs/Python/Book-Scraper/reddit_pipeline' >> ~/.bashrc`
