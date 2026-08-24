"""Dataflow (Apache Beam) pipeline: ingest every row of a flat-file source
into its BigQuery staging table, as-is. No row is ever filtered or
dropped here - validation is entirely Dataplex's job, run against the
staging table after this job completes (see dataflow/post_scan_gate.py,
which pulls Dataplex's findings into a Parquet quarantine file per
source and registers it in Data Catalog).

Run (example):
  python -m dataflow.beam_dq_pipeline \
    --runner=DataflowRunner --project=$PROJECT --region=us-central1 \
    --temp_location=gs://$BUCKET/tmp \
    --input=gs://$RAW_BUCKET/raw/utility_bills/*.csv \
    --output_table=$PROJECT:utility_bills.staging
"""
import argparse
import csv
import io
import logging

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions

logger = logging.getLogger(__name__)


class ParseCsv(beam.DoFn):
    """Reads one CSV file per element, emits every row as a dict - no
    validation, so no row is ever dropped here.
    """

    def process(self, file_metadata):
        path = file_metadata.path
        with FileSystems.open(path) as f:
            content = f.read().decode("utf-8")
        reader = csv.DictReader(io.StringIO(content))
        for row in reader:
            yield row


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="GCS glob for raw flat files")
    parser.add_argument("--output_table", required=True, help="BQ staging table, project:dataset.table")
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=options) as p:
        (
            p
            | "MatchFiles" >> beam.io.fileio.MatchFiles(known_args.input)
            | "ParseCsv" >> beam.ParDo(ParseCsv())
            | "WriteToBQ" >> beam.io.WriteToBigQuery(
                known_args.output_table,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
