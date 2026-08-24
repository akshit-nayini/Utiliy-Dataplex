"""Shared helper for registering entries in Dataplex Universal Catalog
(rebranded "Knowledge Catalog" - same service, same API).

Data Catalog's old `datacatalog_v1` write API (EntryGroup/Entry with a
GcsFilesetSpec, etc.) is being deprecated and is already blocked for write
operations on newer projects ("Project X is not allowed to perform write
operations due to Data Catalog deprecation"). The replacement is
`dataplex_v1.CatalogServiceClient`, which uses EntryGroup / EntryType /
AspectType / Entry instead. This module uses the public, system-provided
"generic" entry type and aspect type (no custom EntryType/AspectType setup
required) so registration works out of the box.
"""
import logging

from google.api_core import exceptions as gax_exceptions
from google.cloud import dataplex_v1
from google.protobuf import struct_pb2

logger = logging.getLogger(__name__)

GENERIC_ENTRY_TYPE = "projects/dataplex-types/locations/global/entryTypes/generic"
GENERIC_ASPECT_TYPE = "projects/dataplex-types/locations/global/aspectTypes/generic"
GENERIC_ASPECT_KEY = "dataplex-types.global.generic"


def ensure_entry_group(project: str, location: str, entry_group_id: str):
    client = dataplex_v1.CatalogServiceClient()
    parent = f"projects/{project}/locations/{location}"
    try:
        operation = client.create_entry_group(
            parent=parent,
            entry_group_id=entry_group_id,
            entry_group=dataplex_v1.EntryGroup(),
        )
        operation.result(60)
        logger.info("Created entry group %s", entry_group_id)
    except gax_exceptions.AlreadyExists:
        pass


def upsert_entry(
    project: str,
    location: str,
    entry_group_id: str,
    entry_id: str,
    display_name: str,
    description: str,
    resource: str = "",
    system: str = "dq_agent_demo",
):
    """Creates (or updates, if it already exists) an Entry under the given
    entry group, using the generic system entry type. `description` carries
    the human-readable DQ summary/breakdown; `resource` is the linked GCS or
    BigQuery resource path, if any.
    """
    ensure_entry_group(project, location, entry_group_id)

    client = dataplex_v1.CatalogServiceClient()
    entry_group_name = f"projects/{project}/locations/{location}/entryGroups/{entry_group_id}"
    entry_name = f"{entry_group_name}/entries/{entry_id}"

    entry = dataplex_v1.Entry(
        entry_type=GENERIC_ENTRY_TYPE,
        entry_source=dataplex_v1.EntrySource(
            display_name=display_name,
            description=description,
            resource=resource,
        ),
        aspects={
            GENERIC_ASPECT_KEY: dataplex_v1.Aspect(
                aspect_type=GENERIC_ASPECT_TYPE,
                data=struct_pb2.Struct(fields={
                    "type": struct_pb2.Value(string_value="dataset"),
                    "system": struct_pb2.Value(string_value=system),
                }),
            )
        },
    )

    try:
        client.create_entry(parent=entry_group_name, entry_id=entry_id, entry=entry)
        logger.info("Created catalog entry %s", entry_name)
    except gax_exceptions.AlreadyExists:
        entry.name = entry_name
        client.update_entry(
            entry=entry,
            update_mask={"paths": ["aspects", "entry_source.description", "entry_source.resource"]},
        )
        logger.info("Updated catalog entry %s", entry_name)
