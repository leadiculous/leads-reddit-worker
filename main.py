import asyncio
import logging
import grpc
import asyncpraw
import psycopg2
from os import getenv
from dotenv import load_dotenv
from ai_service_pb2 import ClassificationRequest, ClassificationResponse
from ai_service_pb2_grpc import ClassifierStub
from google.protobuf.json_format import MessageToJson
from supabase import acreate_client, AClient, AClientOptions

load_dotenv()

if getenv("DEBUG").lower() == "true":
    logging.basicConfig(
        level=logging.DEBUG,
    )

db = psycopg2.connect(getenv("DATABASE_URL"))


def load_campaigns():
    cursor = db.cursor()
    cursor.execute("""
          SELECT t.campaign_id, array_agg(t.tag) as tags FROM public.campaigns c
          INNER JOIN public.campaign_tags t ON t.campaign_id = c.id
          GROUP BY t.campaign_id
          """)
    return cursor.fetchall()


campaigns = load_campaigns()


def on_campaign_change(payload):
    print("-- DB event --")
    print(payload)
    print("-----")


async def monitor_campaigns():
    supabase: AClient = await acreate_client(
        getenv("SUPABASE_URL"),
        getenv("SUPABASE_KEY"),
        options=AClientOptions(realtime={"auto_reconnect": True, "max_retries": 10})
    )

    await supabase.realtime.connect()

    await (supabase.realtime
           .channel("reddit-worker")
           .on_postgres_changes("*", schema="public", table="campaigns", callback=on_campaign_change)
           .subscribe())

    await supabase.realtime.listen()


async def monitor_subreddits():
    # create praw instance
    reddit = asyncpraw.Reddit(
        client_id=getenv("CLIENT_ID"),
        client_secret=getenv("CLIENT_SECRET"),
        username=getenv("DEFAULT_USERNAME"),
        password=getenv("DEFAULT_PASSWORD"),
        user_agent="Leads Finder App (by u/Sleeyax1)",
    )

    # create AI service
    ai_service = ClassifierStub(grpc.aio.insecure_channel(getenv("AI_SERVICE_ADDRESS")))

    subreddit = await reddit.subreddit("all")
    async for submission in subreddit.stream.submissions():
        for campaign in campaigns:
            campaign_id = campaign[0]
            topics = campaign[1]

            req = ClassificationRequest(title=submission.title, body=submission.selftext)
            req.topics.extend(topics)

            res: ClassificationResponse = await ai_service.Classify(req)

            if len(res.matched_topics) > 0:
                print(f"Reddit post URL: {submission.url}")
                print("AI request: ")
                print(MessageToJson(req))
                print("AI response: ")
                print(MessageToJson(res))
                print("-" * 50)


async def main():
    await asyncio.gather(
        monitor_campaigns(),
        monitor_subreddits(),
    )


if __name__ == "__main__":
    asyncio.run(main())
