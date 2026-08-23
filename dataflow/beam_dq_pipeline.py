"""Dataflow (Apache Beam) pipeline: validate utility-bill flat files in GCS
column-by-column, load only the valid rows to BigQuery, and route invalid
rows to a DLQ. Reuses pipeline.validators so column rules stay identical to
the Airflow path.

The Dataplex Auto DQ scan + DQ-agent gate + Data Catalog registration run as
a lightweight follow-on step (Cloud Composer task or Cloud Function
triggered on Dataflow job completion) calling pipeline.dataplex_gate and
pipeline.dq_reporting against the staging table this pipeline writes to -
see dataflow/post_scan_gate.py.

Run (example):
  python -m dataflow.beam_dq_pipeline \
    --runner=DataflowRunner --project=$PROJECT --region=us-central1 \
    --temp_location=gs://$BUCKET/tmp \
    --input=gs://$RAW_BUCKET/raw/utility_bills/*.csv \
    --schema=schemas/utility_bill_schema.yaml \
    --output_table=$PROJECT:utility_bills.staging \
    --dlq=gs://$DLQ_BUCKET/dlq/utility_bills
"""
import argparse
import csv
import io
import json
import logging

import apache_beam as beam
import yaml
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions

from pipeline.validators import apply_validations

logger = logging.getLogger(__name__)

VALID = "valid"
INVALID = "invalid"


class ParseAndValidate(beam.DoFn):
    """Reads one CSV file per element, emits (tag, row_or_error) pairs."""

    def __init__(self, schema):
        self._schema = schema

    def process(self, file_metadata):
        path = file_metadata.path
        with FileSystems.open(path) as f:
            content = f.read().decode("utf-8")
        reader = csv.DictReader(io.StringIO(content))
        for rownum, row in enumerate(reader, start=1):
            ok, errors = apply_validations(row, self._schema)
            if ok:
                yield beam.pvalue.TaggedOutput(VALID, row)
            else:
                yield beam.pvalue.TaggedOutput(
                    INVALID,
                    json.dumps({"file": path, "row_number": rownum, "row": row, "errors": errors}),
                )


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="GCS glob for raw utility bill files")
    parser.add_argument("--schema", required=True, help="Path to column validation schema YAML")
    parser.add_argument("--output_table", required=True, help="BQ table for valid rows, project:dataset.table")
    parser.add_argument("--dlq", required=True, help="GCS prefix for invalid rows (DLQ)")
    known_args, pipeline_args = parser.parse_known_args(argv)

    with open(known_args.schema) as f:
        schema = yaml.safe_load(f)

    options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=options) as p:
        files = p | "MatchFiles" >> beam.io.fileio.MatchFiles(known_args.input)
        results = (
            files
            | "ParseAndValidate" >> beam.ParDo(ParseAndValidate(schema)).with_outputs(VALID, INVALID)
        )

        (
            results[VALID]
            | "WriteValidToBQ" >> beam.io.WriteToBigQuery(
                known_args.output_table,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )

        (
            results[INVALID]
            | "WriteInvalidToDLQ" >> beam.io.WriteToText(
                file_path_prefix=known_args.dlq,
                file_name_suffix=".invalid.jsonl",
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
