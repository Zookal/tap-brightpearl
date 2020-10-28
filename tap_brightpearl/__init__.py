#!/usr/bin/env python3
import singer
import json
from singer import utils
from singer import metadata
from singer import Transformer
from tap_brightpearl.context import Context
from tap_brightpearl.stream import Stream
from tap_brightpearl.brightpearl import Brightpearl

REQUIRED_CONFIG_KEYS = ["brightpearl-app-ref", "brightpearl-account-token","domain", "account_id"]
LOGGER = singer.get_logger()

def initialize_client():
    account_token = Context.config['brightpearl-account-token']
    app_ref = Context.config['brightpearl-app-ref']
    domain = Context.config['domain']
    account_id = Context.config['account_id']
    Context.session = Brightpearl(domain=domain, account_id=account_id, app_ref=app_ref, account_token=account_token)


def discover():
    initialize_client()

    streams = []
    for schema_name in Stream("").resource:
        schema_fields = Stream(schema_name).get_schema()

        for key in schema_fields:
            break

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': {"properties": schema_fields, "type": "object"},
            'metadata':[{"breadcrumb": [],"metadata": {"selected": False}}],
            'key_properties': [key],
            'replication_key': key,
            'replication_method': "FULL_TABLE"
        }
        streams.append(catalog_entry)

    return {'streams': streams}

def sync():
    initialize_client()

    # Emit all schemas first so we have them for child streams
    for stream in Context.catalog["streams"]:
        if Context.is_selected(stream["tap_stream_id"]):

            singer.write_schema(stream["tap_stream_id"],
                                stream["schema"],
                                stream["key_properties"],
                                bookmark_properties=stream["replication_key"])
            Context.counts[stream["tap_stream_id"]] = 0


    # # Loop over streams in catalog
    for catalog_entry in Context.catalog['streams']:
        stream_id = catalog_entry['tap_stream_id']
        Context.stream_objects[stream_id] = Stream(stream_id)
        stream = Context.stream_objects[stream_id]

        if not Context.is_selected(stream_id):
            LOGGER.info('Skipping stream: %s', stream_id)
            continue

        LOGGER.info('Syncing stream: %s', stream_id)

        if not Context.state.get('bookmarks'):
            Context.state['bookmarks'] = {}
        Context.state['bookmarks']['currently_sync_stream'] = stream_id

        with Transformer() as transformer:
            for rec in stream.sync():
                extraction_time = singer.utils.now()

                record_schema = catalog_entry['schema']
                record_metadata = metadata.to_map(catalog_entry['metadata'])
                rec = transformer.transform(rec, record_schema, record_metadata)
                singer.write_record(stream_id,
                                    rec,
                                    time_extracted=extraction_time)
                Context.counts[stream_id] += 1

        Context.state['bookmarks'].pop('currently_sync_stream')
        singer.write_state(Context.state)

    LOGGER.info('----------------------')
    for stream_id, stream_count in Context.counts.items():
        LOGGER.info('%s: %d', stream_id, stream_count)
    LOGGER.info('----------------------')

@utils.handle_top_exception(LOGGER)
def main():

    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    Context.config = args.config

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog, indent=2))
    # Otherwise run in sync mode
    else:
        Context.tap_start = utils.now()
        if args.catalog:
            Context.catalog = args.catalog.to_dict()


        Context.state = args.state
        sync()

if __name__ == "__main__":
    main()
