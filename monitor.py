"""
Watch MongoDB for new posts, comments, and users and print them to the terminal.
Run from the project root: python monitor.py
"""
import os
import sys
import threading

_pipeline_root = os.path.join(os.path.dirname(__file__), "reddit_pipeline")
if _pipeline_root not in sys.path:
    sys.path.insert(0, _pipeline_root)

from shared.mongo_client import get_db

db = get_db()


def watch_posts():
    print("[posts] watching...")
    pipeline = [{"$match": {"operationType": {"$in": ["insert", "update", "replace"]}}}]
    with db.posts.watch(pipeline, full_document="updateLookup") as stream:
        for change in stream:
            doc = change.get("full_document") or {}
            op = change["operationType"]
            post_id  = doc.get("post_id", "?")
            title    = doc.get("title", "")[:80]
            comments = doc.get("comments")
            if op == "insert":
                print(f"[posts] NEW  {post_id}  r/{doc.get('subreddit', '?')}  {title}")
            elif comments is not None:
                print(f"[posts] UPD  {post_id}  comments={len(comments)}")


def watch_users():
    print("[users] watching...")
    pipeline = [{"$match": {"operationType": {"$in": ["insert", "update", "replace"]}}}]
    with db.users.watch(pipeline, full_document="updateLookup") as stream:
        for change in stream:
            doc = change.get("full_document") or {}
            op       = change["operationType"]
            username = doc.get("username", "?")
            scraped  = doc.get("scraped")
            comments = doc.get("comments")
            if op == "insert":
                print(f"[users] NEW  {username}  (stub queued)")
            elif scraped and comments is not None:
                print(f"[users] UPD  {username}  comments={len(comments)}")


threads = [
    threading.Thread(target=watch_posts, daemon=True),
    threading.Thread(target=watch_users, daemon=True),
]
for t in threads:
    t.start()

print("Monitor running — Ctrl+C to stop\n")
try:
    for t in threads:
        t.join()
except KeyboardInterrupt:
    print("\nStopped.")
