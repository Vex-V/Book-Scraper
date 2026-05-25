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


def scrape_users() -> None:
    import RedScrapsLib as rs
    from shared.es_client import ensure_indices, get_es
    from shared.mongo_client import get_db
    from shared.redis_client import is_seen, mark_seen

    rs.init(user_agent="BookScraperBot/1.0")
    db = get_db()
    es = get_es()
    ensure_indices(es)

    allowed = {s.lower() for s in _load_config()["subreddits"]}
    pending = list(db.users.find({"scraped": False}, {"username": 1}))
    log.info("scrape_users: %d users pending", len(pending))

    for doc in pending:
        username = doc["username"]

        if is_seen("users", username):
            db.users.update_one({"_id": username}, {"$set": {"scraped": True}})
            continue

        commented = rs.get_user_comments(user=username)
        if commented is None:
            log.warning("no data for user %s, marking scraped", username)
            db.users.update_one({"_id": username}, {"$set": {"scraped": True}})
            continue

        comments_list = []
        if commented.Comments:
            for c in commented.Comments:
                if (c.Subreddit or "").lower() not in allowed:
                    continue
                comments_list.append({
                    "comment_id":  c.CommentID,
                    "subreddit":   c.Subreddit,
                    "body":        c.Body,
                    "parent_id":   c.ParentID,
                    "post_id":     c.PostID,
                    "post_title":  c.PostTitle,
                    "link":        c.Link,
                    "upvotes":     c.Upvotes,
                    "created_utc": c.CreatedUtc,
                })

        user_doc = {
            "username":    commented.Username,
            "first_id":    commented.FirstID,
            "last_id":     commented.LastID,
            "total_count": commented.TotalCount,
            "comments":    comments_list,
            "scraped":     True,
            "scraped_at":  _now_iso(),
        }
        db.users.update_one({"_id": username}, {"$set": user_doc})
        es.index(index="users", id=username, document=user_doc)
        mark_seen("users", username)
        log.info("user %s: %d relevant comments stored", username, len(comments_list))


with DAG(
    dag_id="scrape_users",
    schedule="*/30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 0},
) as dag:
    PythonOperator(task_id="scrape_users", python_callable=scrape_users)
