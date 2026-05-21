import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "reddit_pipeline"))

import RedScrapsLib as rs
from shared.mongo_client import get_db

rs.init(user_agent="MyBot/1.0")
db = get_db()

# Subreddit posts after 1sz3cj2
posts = rs.get_home("books", sort="top",time="month")
if posts is None or not posts.Posts:
    print("No posts returned — cursor may be at end of feed")
else:
    post_ids = [p.PostID for p in posts.Posts if p.PostID]
    existing = {doc["post_id"] for doc in db.posts.find({"post_id": {"$in": post_ids}}, {"post_id": 1})}

    print(f"Got {len(posts.Posts)} posts (FirstID={posts.FirstID}, LastID={posts.LastID})")
    print(f"Already in MongoDB: {len(existing)}/{len(post_ids)}")
    for post in posts.Posts:
        in_db = "✓ in db" if post.PostID in existing else "✗ new"
        print(f"  [{in_db}] {post.PostID}  {post.Title}")
print("\n")
print("#####################")

