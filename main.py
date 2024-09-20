import threading
import grpc
import praw
import psycopg2
from os import getenv
from dotenv import load_dotenv
from ai_service_pb2 import ClassificationRequest, ClassificationResponse
from ai_service_pb2_grpc import ClassifierStub
from google.protobuf.json_format import MessageToJson

load_dotenv()

db = psycopg2.connect(getenv("DATABASE_URL"))
cursor = db.cursor()


class RedditBot:
    def __init__(self, campaign_id: int, tags: list):
        self.campaign_id = campaign_id
        self.topics = tags
        self.reddit = praw.Reddit(
            client_id=getenv("CLIENT_ID"),
            client_secret=getenv("CLIENT_SECRET"),
            username=getenv("DEFAULT_USERNAME"),
            password=getenv("DEFAULT_PASSWORD"),
            user_agent="Leads Finder App (by u/Sleeyax1)",
        )
        self.ai_service = ClassifierStub(grpc.insecure_channel(getenv("AI_SERVICE_ADDRESS")))

    def start(self):
        subreddit = self.reddit.subreddit("all")
        for submission in subreddit.stream.submissions():
            req = ClassificationRequest(title=submission.title, body=submission.selftext)
            req.topics.extend(self.topics)
            res: ClassificationResponse = self.ai_service.Classify(req)
            if len(res.matched_topics) > 0:
                print(f"Reddit post URL: {submission.url}")
                print("AI request: ")
                print(MessageToJson(req))
                print("AI response: ")
                print(MessageToJson(res))
                print("-" * 50)


def init():
    bots = []

    cursor.execute("""
    SELECT t.campaign_id, array_agg(t.tag) as tags FROM public.campaigns c
    INNER JOIN public.campaign_tags t ON t.campaign_id = c.id
    GROUP BY t.campaign_id
    """)
    campaigns = cursor.fetchall()

    print(f"Loaded {len(campaigns)} campaigns with tags")

    for campaign in campaigns:
        campaign_id = campaign[0]
        tags = campaign[1]
        bots.append(RedditBot(campaign_id, tags))

    # Run each bot in a separate thread
    for bot in bots:
        bot_thread = threading.Thread(target=bot.start)
        bot_thread.start()


if __name__ == "__main__":
     init()
