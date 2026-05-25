import json
import logging
import os
import sys
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

_pipeline_root = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if _pipeline_root not in sys.path:
    sys.path.insert(0, _pipeline_root)

log = logging.getLogger(__name__)
_CONFIG_PATH = os.path.join(_pipeline_root, "config.json")


def _load_config() -> dict:
    with open(_CONFIG_PATH) as f:
        return json.load(f)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def scrape_posts() -> None:
    import RedScrapsLib as rs
    from shared.es_client import ensure_indices, get_es
    from shared.mongo_client import get_db
    from shared.redis_client import is_seen, mark_seen

    rs.init(user_agent="BookScraperBot/1.0")
    db = get_db()
    es = get_es()
    ensure_indices(es)

    subreddits = _load_config()["subreddits"]
    existing_ids = {doc["post_id"] for doc in db.posts.find({}, {"post_id": 1})}

    for subreddit in subreddits:
        cursor_key = f"{subreddit}_top_year_after"
        state = db.scrape_state.find_one({"_id": cursor_key})
        raw_after = state["last_id"] if state else None
        after = f"t3_{raw_after}" if raw_after and not raw_after.startswith("t3_") else raw_after

        if after:
            post_id_part = after.replace("t3_", "")
            log.info("r/%s: fetching after=%s (https://www.reddit.com/r/%s/comments/%s)",
                     subreddit, after, subreddit, post_id_part)
        else:
            log.info("r/%s: fetching from top (no cursor)", subreddit)

        home = rs.get_home(subreddit=subreddit, sort="top", time="year", after=after)
        if home is None or not home.Posts:
            log.warning("r/%s: no posts returned, resetting cursor", subreddit)
            db.scrape_state.delete_one({"_id": cursor_key})
            continue

        published = 0
        for post in home.Posts:
            post_id = post.PostID
            if not post_id or post_id in existing_ids or is_seen("posts", post_id):
                continue

            doc = {
                "_id":              post_id,
                "post_id":          post_id,
                "title":            post.Title,
                "author":           post.Author,
                "self_text":        post.SelfText,
                "link":             post.Link,
                "subreddit":        subreddit,
                "scraped_at":       _now_iso(),
                "comments_scraped": False,
            }
            db.posts.update_one({"_id": post_id}, {"$setOnInsert": doc}, upsert=True)
            es.index(index="posts", id=post_id, document=doc)
            mark_seen("posts", post_id)
            existing_ids.add(post_id)
            published += 1

        db.scrape_state.update_one(
            {"_id": cursor_key},
            {"$set": {"last_id": home.LastID}},
            upsert=True,
        )
        log.info("r/%s: %d new posts, cursor -> %s", subreddit, published, home.LastID)


with DAG(
    dag_id="scrape_posts",
    schedule="*/30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 0},
) as dag:
    PythonOperator(task_id="scrape_posts", python_callable=scrape_posts)
