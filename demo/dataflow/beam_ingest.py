"""Dataflow (Apache Beam) ingestion for the demo - Option 1 of the three
ways to run the demo (see ../README.md). Reads sample_orders.parquet from
GCS and writes every row into orders_bronze, unfiltered - the same
"ingest everything, validate afterward" contract as pipeline/loader.py's
plain BigQuery load job, just expressed as a Beam pipeline so the demo
also exercises the Dataflow service directly.

On completion, publishes a Pub/Sub message so a downstream listener
(../pubsub_listener.py) can trigger validation - this is the same
Dataflow -> Pub/Sub -> post-processing pattern the main framework
describes for its Dataflow architecture (there, wiring a Cloud Function to
the Dataflow job-completion notification), made concrete and runnable here.

Run locally against real GCP resources (DirectRunner - fast, no Dataflow
service, good for a first test):
  python demo/dataflow/beam_ingest.py --runner=DirectRunner --project=$PROJECT_ID \
    --input=gs://$RAW_BUCKET/raw/orders/sample_orders.parquet \
    --output_table=$PROJECT_ID:dq_demo.orders_bronze \
    --topic=projects/$PROJECT_ID/topics/dq-demo-ingest-complete

Run on the actual Dataflow managed service:
  python demo/dataflow/beam_ingest.py --runner=DataflowRunner --project=$PROJECT_ID \
    --region=us-central1 --temp_location=gs://$RAW_BUCKET/tmp \
    --input=gs://$RAW_BUCKET/raw/orders/sample_orders.parquet \
    --output_table=$PROJECT_ID:dq_demo.orders_bronze \
    --topic=projects/$PROJECT_ID/topics/dq-demo-ingest-complete
"""
import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

logger = logging.getLogger(__name__)


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="GCS path to sample_orders.parquet")
    parser.add_argument("--output_table", required=True, help="BQ Bronze table, project:dataset.table")
    parser.add_argument("--topic", default=None, help="Pub/Sub topic to notify on completion, e.g. projects/P/topics/T")
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadParquet" >> beam.io.ReadFromParquet(known_args.input)
            | "WriteToBQ" >> beam.io.WriteToBigQuery(
                known_args.output_table,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )
    # pipeline block exits only once the Beam job has finished (DirectRunner
    # runs synchronously; the "with" context waits for DataflowRunner too)

    if known_args.topic:
        from google.cloud import pubsub_v1

        publisher = pubsub_v1.PublisherClient()
        future = publisher.publish(known_args.topic, b'{"event": "bronze_loaded", "source": "dataflow"}')
        future.result()
        logger.info("Published ingest-complete notification to %s", known_args.topic)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
