# Running the Reddit Scraper Pipeline

All commands are run from the project root (`Book-Scraper/`) unless stated otherwise.

---

## Quick Start

```bash
./start.sh
```

Clears Redis, starts Docker, and opens a tmux session with the Airflow scheduler and api-server.

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

### 4. Trigger the DAG

```bash
export AIRFLOW_HOME=$(pwd)/reddit_pipeline
airflow dags unpause scrape_pipeline
airflow dags trigger scrape_pipeline
```

---

## Notes

- `AIRFLOW_HOME` must be exported in every terminal that runs an Airflow command. To set it permanently: `echo 'export AIRFLOW_HOME=/home/vai/VScode-Projs/Python/Book-Scraper/reddit_pipeline' >> ~/.bashrc`
- If a DAG stays queued, check the scheduler terminal is running.
- If MongoDB Compass on Windows won't connect via `localhost`, use the WSL2 IP instead: run `ip addr show eth0` in WSL2 to find it, then connect to `mongodb://<ip>:27017`.
- To add more subreddits, edit `reddit_pipeline/config.json`.
