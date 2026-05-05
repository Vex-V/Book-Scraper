# Reddit Book Review Pipeline

A data ingestion pipeline that scrapes r/books, deduplicates incoming data, and stores it in MongoDB and Elasticsearch for downstream recommendation logic.

## Architecture

```
Airflow DAG (every 30 min)
    Task 1: scrape posts    → Kafka topic: raw-posts
    Task 2: scrape comments → Kafka topic: raw-comments
    Task 3: scrape users    → Kafka topic: raw-users

Kafka Consumers (persistent background processes)
    Consumer A: dedupe via Redis → write to MongoDB
    Consumer B: sync to Elasticsearch
```

Tasks run sequentially. MongoDB acts as the coordination layer between tasks via flags — no Airflow XComs.

## Project Structure

```
reddit_pipeline/
├── dags/
│   └── scrape_books_pipeline.py   # Airflow DAG
├── consumers/
│   ├── mongo_consumer.py          # Redis dedup → MongoDB writer
│   └── elasticsearch_consumer.py  # Elasticsearch syncer
├── shared/
│   ├── kafka_client.py            # Producer/consumer helpers
│   ├── mongo_client.py            # MongoDB connection
│   └── redis_client.py            # Redis dedup helpers
├── docker-compose.yml             # Kafka, MongoDB, Redis, Elasticsearch
├── airflow.cfg                    # Airflow config (AIRFLOW_HOME points here)
└── requirements.txt
```

## Prerequisites

- Python 3.12
- Docker + Docker Compose
- tmux (`sudo apt install tmux`)
- The `.venv` virtualenv with dependencies installed

### Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r reddit_pipeline/requirements.txt
```

## Running

### Quick start (recommended)

```bash
./start.sh
```

Clears Redis, starts Docker services, and opens a tmux session with all 4 processes running in a 2x2 grid.

### Manual start

See [run.md](run.md) for step-by-step instructions.

### Trigger the DAG

In any terminal with `AIRFLOW_HOME` set:

```bash
export AIRFLOW_HOME=$(pwd)/reddit_pipeline
airflow dags unpause scrape_books_pipeline
airflow dags trigger scrape_books_pipeline
```

Or use the Airflow UI at `http://localhost:8080`.

## How it works

**Task 1 — scrape_posts**: Fetches posts from r/books using a paginated cursor stored in MongoDB (`scrape_state` collection). New posts are published to the `raw-posts` Kafka topic.

**Task 2 — scrape_comments**: Queries MongoDB for posts where `comments_scraped: false`, fetches their comments, publishes to `raw-comments`, then flips the flag to `true`.

**Task 3 — scrape_users**: Finds distinct comment authors not yet in the `users` collection, fetches their post and comment history, and publishes merged user objects to `raw-users`.

**MongoDB consumer**: Subscribes to all three topics, deduplicates via Redis sets (`seen:posts`, `seen:comments`, `seen:users` with 30-day TTL), and writes to MongoDB.

**Elasticsearch consumer**: Subscribes to the same topics and indexes documents into `posts` and `users` indices with full-text mappings on title, body, and self_text fields.

## Connecting to MongoDB

From WSL2, MongoDB Compass on Windows should connect via the WSL2 IP rather than `localhost`:

```bash
ip addr show eth0   # find the WSL2 IP
```

Then connect Compass to `mongodb://<wsl2-ip>:27017`.

## Notes

- The Reddit API is rate limited to 100 calls per 10 minutes. The scraper library handles retries internally — the DAG has no execution timeout by design.
- The first DAG run seeds posts. Trigger a second run after the MongoDB consumer logs `inserted post ...` to proceed with comment and user scraping.
- Restarting a consumer mid-run causes no data loss — Kafka holds messages until the consumer catches up.
