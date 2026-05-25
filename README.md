# Reddit Scraper Pipeline

A data ingestion pipeline that scrapes configured subreddits, deduplicates incoming data, and stores it in MongoDB and Elasticsearch.

## Architecture

```
Airflow DAG: scrape_pipeline (every 30 min)
    Task 1: scrape_posts    — fetch top/year posts, write to MongoDB + ES
    Task 2: scrape_comments — scrape comments, queue author stubs
    Task 3: scrape_users    — scrape user comment history, write to MongoDB + ES

Infrastructure
    MongoDB      — primary data store
    Elasticsearch — full-text search index
    Redis         — deduplication (seen:posts, seen:comments, seen:users)
```

Tasks run sequentially within one DAG. MongoDB flags (`comments_scraped`, `scraped`) coordinate progress between tasks.

## Project Structure

```
reddit_pipeline/
├── dags/
│   └── scrape_pipeline.py         # Single 3-task DAG
├── consumers/                     # Empty — writes happen directly from tasks
├── shared/
│   ├── es_client.py               # Elasticsearch connection + index setup
│   ├── mongo_client.py            # MongoDB connection
│   └── redis_client.py            # Redis dedup helpers
├── config.json                    # Subreddit list
├── docker-compose.yml             # MongoDB, Redis, Elasticsearch
├── airflow.cfg                    # Airflow config (AIRFLOW_HOME points here)
└── requirements.txt
```

## Configuration

Edit `reddit_pipeline/config.json` to control which subreddits are scraped:

```json
{
  "subreddits": ["Fantasy", "books"]
}
```

Each subreddit gets its own independent pagination cursor. User comments are filtered to only include activity from subreddits in this list.

## Prerequisites

- Python 3.12
- Docker + Docker Compose
- tmux (`sudo apt install tmux`)

### Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r reddit_pipeline/requirements.txt
```

## Running

### Quick start

```bash
./start.sh
```

Clears Redis, starts Docker services, and opens a tmux session (top: scheduler, bottom: api-server).

### Manual start

See [run.md](run.md) for step-by-step instructions.

### Trigger the DAG

```bash
export AIRFLOW_HOME=$(pwd)/reddit_pipeline
airflow dags unpause scrape_pipeline
airflow dags trigger scrape_pipeline
```

Or use the Airflow UI at `http://localhost:8080`.

## How it works

**Task 1 — scrape_posts**: Fetches top posts from each configured subreddit using a paginated cursor (`top/year` sort). New posts are written directly to MongoDB and indexed in Elasticsearch. The cursor advances each run; when the end of the feed is reached it resets.

**Task 2 — scrape_comments**: Queries MongoDB for posts where `comments_scraped: false`. Fetches comments, embeds them in the post document, updates Elasticsearch, and upserts comment authors into the `users` collection with `scraped: false`.

**Task 3 — scrape_users**: Queries `users` where `scraped: false`. Fetches each user's comment history, filters to allowed subreddits only, writes to MongoDB and Elasticsearch, marks `scraped: true`.

## MongoDB Collections

| Collection | Description |
|---|---|
| `posts` | Scraped posts with embedded comments |
| `users` | Reddit users with filtered comment history |
| `scrape_state` | Per-subreddit pagination cursors |

## Connecting to MongoDB

From WSL2, connect MongoDB Compass on Windows via the WSL2 IP:

```bash
ip addr show eth0   # find the WSL2 IP
```

Connect Compass to `mongodb://<wsl2-ip>:27017`, database `reddit_books`.

## Notes

- The Reddit API is rate limited to 100 calls per 10 minutes. The scraper library handles retries internally — the DAG has no execution timeout by design.
- Redis deduplication prevents reprocessing posts/comments/users across runs (30-day TTL).
- Restarting the pipeline mid-run is safe — MongoDB flags track exactly where each task left off.
