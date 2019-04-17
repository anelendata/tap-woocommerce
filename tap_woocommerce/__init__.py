#!/usr/bin/env python3
import itertools
import os
import sys
import time
import re
import json
import attr
import urllib
import requests
import backoff
from requests.auth import HTTPBasicAuth
import singer
import singer.metrics as metrics
from singer import utils
import datetime
import dateutil
from dateutil import parser


REQUIRED_CONFIG_KEYS = ["url", "consumer_key", "consumer_secret", "start_date", "schema"]
LOGGER = singer.get_logger()

CONFIG = {
    "url": None,
    "consumer_key": None,
    "consumer_secret": None,
    "start_date":None,
    "schema": None,
}

ENDPOINTS = {
    "orders":"wp-json/wc/v2/orders?after={0}&orderby=date&order=asc&per_page=100&page={1}",
    "subscriptions": "wp-json/wc/v1/subscriptions?after={0}&orderby=date&order=asc&per_page=100&page={1}",
    "customers":"wp-json/wc/v2/customers?orderby=id&order=asc&per_page=100&page={1}",
}

USER_AGENT = 'Mozilla/5.0 (Macintosh; scitylana.singer.io) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36 '


def get_endpoint(endpoint, kwargs):
    '''Get the full url for the endpoint'''
    if endpoint not in ENDPOINTS:
        raise ValueError("Invalid endpoint {}".format(endpoint))

    after = urllib.parse.quote(kwargs[0])
    page = kwargs[1]
    return CONFIG["url"] + ENDPOINTS[endpoint].format(after,page)


def get_start(STATE, tap_stream_id, bookmark_key):
    current_bookmark = singer.get_bookmark(STATE, tap_stream_id, bookmark_key)
    if current_bookmark is None:
        return CONFIG["start_date"]
    return current_bookmark


def load_schema(entity):
    '''Returns the schema for the specified source'''
    schema = utils.load_json(get_abs_path("schemas/{}.json".format(entity)))

    return schema


def nested_get(input_dict, nested_key):
    internal_dict_value = input_dict
    for k in nested_key:
        internal_dict_value = internal_dict_value.get(k, None)
        if internal_dict_value is None:
            return None
    return internal_dict_value


def _do_filter(obj, dict_path, schema):
    if not obj:
        return None
    obj_format = nested_get(schema, dict_path + ["type"])
    if type(obj_format) is list:
        obj_type = obj_format[1]
    elif type(obj_format is str):
        obj_type = obj_format

    if obj_type == "object":
        assert(type(obj) is dict and obj.keys())
        filtered = dict()
        for key in obj.keys():
            ret = _do_filter(obj[key], dict_path + ["properties", key], schema)
            if ret:
                filtered[key] = ret
    elif obj_type == "array":
        assert(type(obj) is list)
        filtered = list()
        for o in obj:
            ret = _do_filter(o, dict_path + ["items"], schema)
            if ret:
                filtered.append(ret)
    else:
        if obj_type == "string":
            filtered = str(obj)
        elif obj_type == "number":
            try:
                filtered = float(obj)
            except ValueError as e:
                LOGGER.error(str(e) + "dict_path" + str(dict_path) + " object type: " + obj_type)
                raise
        else:
            filtered = obj
    return filtered


def filter_result(row, schema):
    filtered = _do_filter(row, [], schema)
    tzinfo = parser.parse(CONFIG["start_date"]).tzinfo
    filtered["date_created"] = parser.parse(row["date_created"]).replace(tzinfo=tzinfo).isoformat()
    filtered["date_modified"] = parser.parse(row["date_modified"]).replace(tzinfo=tzinfo).isoformat()
    filtered.pop("meta_data")
    filtered.pop("_links")
    return filtered


def giveup(exc):
    return exc.response is not None \
        and 400 <= exc.response.status_code < 500 \
        and exc.response.status_code != 429


@utils.backoff((backoff.expo,requests.exceptions.RequestException), giveup)
@utils.ratelimit(20, 1)
def gen_request(stream_id, url):
    with metrics.http_request_timer(stream_id) as timer:
        headers = { 'User-Agent': USER_AGENT }
        resp = requests.get(url,
                headers=headers,
                auth=HTTPBasicAuth(CONFIG["consumer_key"], CONFIG["consumer_secret"]))
        timer.tags[metrics.Tag.http_status_code] = resp.status_code
        resp.raise_for_status()
        return resp.json()


def sync_rows(STATE, catalog, schema_name="orders", key_properties=["order_id"]):
    schema = load_schema(schema_name)
    singer.write_schema(schema_name, schema, key_properties)

    start = get_start(STATE, schema_name, "last_update")
    LOGGER.info("Only syncing %s updated since %s" % (schema_name, start))
    last_update = start
    page_number = 1
    with metrics.record_counter(schema_name) as counter:
        while True:
            endpoint = get_endpoint(schema_name, [start, page_number])
            LOGGER.info("GET %s", endpoint)
            rows = gen_request(schema_name,endpoint)
            for row in rows:
                counter.increment()
                row = filter_result(row, schema)
                if "_etl_tstamp" in schema["properties"].keys():
                    row["_etl_tstamp"] = time.time()
                if("date_created" in row) and (parser.parse(row["date_created"]) > parser.parse(last_update)):
                    last_update = row["date_created"]
                singer.write_record(schema_name, row)
            if len(rows) < 100:
                break
            else:
                page_number +=1
    STATE = singer.write_bookmark(STATE, schema_name, 'last_update', last_update)
    singer.write_state(STATE)
    LOGGER.info("Completed %s Sync" % schema_name)
    return STATE


def sync_orders(STATE, catalog):
    sync_rows(STATE, catalog, schema_name="orders", key_properties=["id"])


def sync_subscriptions(STATE, catalog):
    sync_rows(STATE, catalog, schema_name="subscriptions", key_properties=["id"])


def sync_customers(STATE, catalog):
    sync_rows(STATE, catalog, schema_name="customers", key_properties=["id"])


@attr.s
class Stream(object):
    tap_stream_id = attr.ib()
    sync = attr.ib()

STREAMS = {"orders": [Stream("orders", sync_orders)],
           "subscriptions": [Stream("subscriptions", sync_subscriptions)],
           "customers": [Stream("customers", sync_customers)]}


def get_streams_to_sync(streams, state):
    '''Get the streams to sync'''
    current_stream = singer.get_currently_syncing(state)
    result = streams
    if current_stream:
        result = list(itertools.dropwhile(
            lambda x: x.tap_stream_id != current_stream, streams))
    if not result:
        raise Exception("Unknown stream {} in state".format(current_stream))
    return result


def get_selected_streams(remaining_streams, annotated_schema):
    selected_streams = []

    for stream in remaining_streams:
        tap_stream_id = stream.tap_stream_id
        for stream_idx, annotated_stream in enumerate(annotated_schema.streams):
            if tap_stream_id == annotated_stream.tap_stream_id:
                schema = annotated_stream.schema
                if (hasattr(schema, "selected")) and (schema.selected is True):
                    selected_streams.append(stream)

    return selected_streams


def do_sync(STATE, catalogs, schema="orders"):
    '''Sync the streams that were selected'''
    remaining_streams = get_streams_to_sync(STREAMS[schema], STATE)
    selected_streams = get_selected_streams(remaining_streams, catalogs)
    if len(selected_streams) < 1:
        LOGGER.info("No Streams selected, please check that you have a schema selected in your catalog")
        return

    LOGGER.info("Starting sync. Will sync these streams: %s", [stream.tap_stream_id for stream in selected_streams])

    for stream in selected_streams:
        LOGGER.info("Syncing %s", stream.tap_stream_id)
        singer.set_currently_syncing(STATE, stream.tap_stream_id)
        singer.write_state(STATE)

        try:
            catalog = [cat for cat in catalogs.streams if cat.stream == stream.tap_stream_id][0]
            STATE = stream.sync(STATE, catalog)
        except Exception as e:
            LOGGER.critical(e)
            raise e


def get_abs_path(path):
    '''Returns the absolute path'''
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_discovered_schema(stream):
    '''Attach inclusion automatic to each schema'''
    schema = load_schema(stream.tap_stream_id)
    for k in schema['properties']:
        schema['properties'][k]['inclusion'] = 'automatic'
    return schema


def discover_schemas(schema="orders"):
    '''Iterate through streams, push to an array and return'''
    result = {'streams': []}
    for stream in STREAMS[schema]:
        LOGGER.info('Loading schema for %s', stream.tap_stream_id)
        result['streams'].append({'stream': stream.tap_stream_id,
                                  'tap_stream_id': stream.tap_stream_id,
                                  'schema': load_discovered_schema(stream)})
    return result

def do_discover():
    '''JSON dump the schemas to stdout'''
    LOGGER.info("Loading Schemas")
    json.dump(discover_schemas(CONFIG["schema"]), sys.stdout, indent=4)

@utils.handle_top_exception(LOGGER)
def main():
    '''Entry point'''
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    CONFIG.update(args.config)
    STATE = {}

    if args.state:
        STATE.update(args.state)
    if args.discover:
        do_discover()
    elif args.catalog:
        do_sync(STATE, args.catalog, CONFIG["schema"])
    else:
        LOGGER.info("No Streams were selected")

if __name__ == "__main__":
    main()
