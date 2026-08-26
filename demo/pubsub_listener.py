"""Listens on the demo's Pub/Sub subscription for an ingest-complete
notification (published by dataflow/beam_ingest.py after loading Bronze),
then runs the same post-ingest pipeline (Dataplex scan -> quarantine
export -> Silver -> Gold -> catalog) as run_demo.py's later steps -
Option 1 of the three ways to run the demo (see README.md).

This is the concrete, runnable version of the "Dataflow -> Pub/Sub ->
downstream processing" pattern the main framework describes for its
Dataflow architecture (there, wiring a Cloud Function to the job's
completion notification) but never wires up as actual code.

Usage (processes exactly one notification, then exits - good for a demo):
  python demo/pubsub_listener.py --project=$PROJECT_ID \
    --subscription=dq-demo-ingest-complete-sub --once \
    --quarantine_bucket=$QUARANTINE_BUCKET

Drop --once to keep listening indefinitely (e.g. run in the background
while you trigger multiple Dataflow ingestion jobs).
"""
import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from google.cloud import pubsub_v1

from pipeline.post_ingest import run_post_ingest

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--subscription", required=True)
    parser.add_argument("--location", default="us-central1")
    parser.add_argument("--quarantine_bucket", required=True)
    parser.add_argument("--entry_group", default="dq-demo-group")
    parser.add_argument("--once", action="store_true", help="Process one message then exit")
    parser.add_argument("--timeout", type=int, default=600, help="Seconds to wait for a message with --once")
    args = parser.parse_args()

    subscriber = pubsub_v1.SubscriberClient()
    sub_path = subscriber.subscription_path(args.project, args.subscription)

    def callback(message):
        logger.info("Received notification: %s", message.data)
        try:
            result = run_post_ingest(args.project, args.location, args.quarantine_bucket, args.entry_group)
            logger.info("Post-ingest complete: %s", result)
            message.ack()
        except Exception:
            logger.exception("post-ingest failed - nacking for redelivery")
            message.nack()
            if args.once:
                raise
        finally:
            if args.once:
                streaming_pull_future.cancel()

    streaming_pull_future = subscriber.subscribe(sub_path, callback=callback)
    logger.info("Listening on %s ...", sub_path)
    try:
        streaming_pull_future.result(timeout=args.timeout if args.once else None)
    except TimeoutError:
        logger.error("No message received within %ss - is beam_ingest.py's --topic wired to this subscription's topic?", args.timeout)
        streaming_pull_future.cancel()
    except Exception:
        # normal path when the callback calls streaming_pull_future.cancel() after --once
        pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
