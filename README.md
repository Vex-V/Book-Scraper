# Reddit Book Review Pipeline

A data ingestion pipeline that scrapes r/books, deduplicates incoming data, and stores it in MongoDB and Elasticsearch for downstream recommendation logic.

## Architecture

```
DAG 1: scrape_posts_comments (every 30 min)
    Task 1: scrape posts (paginated, sort=new) → Kafka: raw-posts
    Task 2: scrape comments + queue authors   → Kafka: raw-comments

DAG 2: scrape_users (every 30 min)
    Task 1: scrape users with scraped=false   → Kafka: raw-users

Kafka Consumers (persistent background processes)
    Consumer A: dedupe via Redis → write to MongoDB
    Consumer B: sync to Elasticsearch
```

Tasks within each DAG run sequentially. MongoDB acts as the coordination layer between tasks via flags — no Airflow XComs.

## Project Structure

```
reddit_pipeline/
├── dags/
│   ├── scrape_posts_comments.py   # DAG 1: posts + comments
│   └── scrape_users.py            # DAG 2: user scraping
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

Clears Redis, starts Docker services, and opens a tmux session with all 4 processes in a 2x2 grid.

### Manual start

See [run.md](run.md) for step-by-step instructions.

### Trigger the DAGs

In any terminal with `AIRFLOW_HOME` set:

```bash
export AIRFLOW_HOME=$(pwd)/reddit_pipeline
airflow dags unpause scrape_posts_comments
airflow dags unpause scrape_users
airflow dags trigger scrape_posts_comments
airflow dags trigger scrape_users
```

Or use the Airflow UI at `http://localhost:8080`.

## How it works

**DAG 1 — `scrape_posts_comments`**

- **Task 1 (scrape_posts)**: Fetches the newest posts from r/books using a paginated cursor stored in MongoDB (`scrape_state`). Publishes new posts to `raw-posts`. When all posts on a page are already known, the cursor resets so the next run picks up the latest submissions.
- **Task 2 (scrape_comments)**: Queries MongoDB for posts where `comments_scraped: false`, fetches their comments, publishes to `raw-comments`, upserts comment authors into the `users` collection with `scraped: false`, then flips `comments_scraped: true`.

**DAG 2 — `scrape_users`**

- Queries `users` where `scraped: false` (populated by DAG 1 Task 2), fetches each user's full post and comment history, publishes merged user objects to `raw-users`, then marks `scraped: true`.

**MongoDB consumer**: Subscribes to all three topics, deduplicates via Redis sets (`seen:posts`, `seen:comments`, `seen:users` with 30-day TTL), and writes to MongoDB.

**Elasticsearch consumer**: Subscribes to the same topics and indexes documents into `posts` and `users` indices with full-text mappings on title, body, and self_text fields.

## MongoDB Collections

| Collection | Description |
|---|---|
| `posts` | Scraped r/books posts with embedded comments |
| `users` | Reddit users with full post/comment history |
| `scrape_state` | Pagination cursor for the posts feed |

## Connecting to MongoDB

From WSL2, MongoDB Compass on Windows should connect via the WSL2 IP rather than `localhost`:

```bash
ip addr show eth0   # find the WSL2 IP
```

Then connect Compass to `mongodb://<wsl2-ip>:27017`, database `reddit_books`.

## Notes

- The Reddit API is rate limited to 100 calls per 10 minutes. The scraper library handles retries internally — the DAGs have no execution timeout by design.
- The first `scrape_posts_comments` run seeds posts into Kafka. Trigger a second run after the MongoDB consumer logs `inserted post ...` so comments get scraped and author stubs are created in `users`.
- `scrape_users` only has work to do after at least one round of comments has been scraped.
- Restarting a consumer mid-run causes no data loss — Kafka holds messages until the consumer catches up.
