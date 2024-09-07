import threading
from os import getenv
import praw

# https://github.com/sleeyax/reddit-stremio-bot/blob/main/bot.py
# https://praw.readthedocs.io/en/stable/tutorials/reply_bot.html

import psycopg2
from dotenv import load_dotenv

load_dotenv()

db = psycopg2.connect(getenv("DATABASE_URL"))
cursor = db.cursor()

bots = []


class RedditBot:
    def __init__(self, campaign_id: int, tags: list):
        self.campaign_id = campaign_id
        self.tags = tags
        self.reddit = praw.Reddit(
            client_id=getenv("CLIENT_ID"),
            client_secret=getenv("CLIENT_SECRET"),
            username=getenv("DEFAULT_USERNAME"),
            password=getenv("DEFAULT_PASSWORD"),
            user_agent="Leads Finder App (by u/Sleeyax1)",
        )
        self.subreddits = ["AskReddit"]

    def start(self):
        subreddit = self.reddit.subreddit("all")
        for submission in subreddit.stream.submissions():
            print(f"bot for campaign {self.campaign_id} found submission {submission}")


def init():
    cursor.execute("""
    SELECT t.campaign_id, array_agg(t.tag) as tags FROM public.campaigns c
    INNER JOIN public.campaign_tags t ON t.campaign_id = c.id
    GROUP BY t.campaign_id
    """)
    campaigns = cursor.fetchall()

    print(f"Found {len(campaigns)} campaigns")

    for campaign in campaigns:
        campaign_id = campaign[0]
        tags = campaign[1]
        bots.append(RedditBot(campaign_id, tags))

    # start each bot in a new thread
    for bot in bots:
        print(f"Starting bot for campaign {bot.campaign_id} in separate thread")
        bot_thread = threading.Thread(target=bot.start)
        bot_thread.start()


if __name__ == "__main__":
    init()
