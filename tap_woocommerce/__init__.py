#!/usr/bin/env python3

from requests.auth import HTTPBasicAuth
from dateutil import parser
import argparse, attr, backoff, datetime, itertools, json, os, pytz, requests, sys, time, urllib

import singer
from singer import utils
from singer.catalog import Catalog
import singer.metrics as metrics

REQUIRED_CONFIG_KEYS = ["url", "consumer_key", "consumer_secret", "start_date"]
LOGGER = singer.get_logger()

INCREMENTAL_ITEMS_PER_PAGE = 10

CONFIG = {
    "url": None,
    "consumer_key": None,
    "consumer_secret": None,
    "start_date":None,
    "schema": None,
    "items_per_page": 100,
    "schemas_path": "schemas"
}

ENDPOINTS = {
    "orders":"wp-json/wc/v3/orders?after={start_date}&before={end_date}&orderby=id&order=asc&per_page={items_per_page}&page={current_page}",
    "subscriptions": "wp-json/wc/v1/subscriptions?after={start_date}&before={end_date}&orderby=id&order=asc&per_page={items_per_page}&page={current_page}",
    "customers":"wp-json/wc/v3/customers?role=all&orderby=id&order=asc&per_page={items_per_page}&page={current_page}",
    "modified_items": "wp-json/wc/v1/{resource}/updated?days={days}&hours={hours}&limit={items_per_page}&offset={offset}",
    "orders_by_id":"wp-json/wc/v3/orders?include={ids}",
    "subscriptions_by_id":"wp-json/wc/v1/subscriptions?include={ids}",
    "customers_by_id":"wp-json/wc/v3/customers?role=all&include={ids}",
}

USER_AGENT = 'Mozilla/5.0 (Macintosh; scitylana.singer.io) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36 '


def get_endpoint(endpoint, kwargs):
    '''Get the full url for the endpoint'''
    if endpoint not in ENDPOINTS:
        raise ValueError("Invalid endpoint {}".format(endpoint))
    return CONFIG["url"] + ENDPOINTS[endpoint].format(**kwargs)


def get_start(STATE, tap_stream_id, bookmark_key):
    current_bookmark = singer.get_bookmark(STATE, tap_stream_id, bookmark_key)
    if current_bookmark is None:
        return CONFIG["start_date"]
    return current_bookmark


def load_schema(entity, schemas_path):
    '''Returns the schema for the specified source'''
    schema = utils.load_json(get_abs_path(os.path.join(schemas_path, "{}.json".format(entity))))

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
    if obj_format is None:
        return None
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
    if filtered.get("date_created"):
        filtered["date_created"] = parser.parse(row["date_created"]).replace(tzinfo=tzinfo).isoformat()
    if filtered.get("date_modified"):
        filtered["date_modified"] = parser.parse(row["date_modified"]).replace(tzinfo=tzinfo).isoformat()
    if filtered.get("meta_data"):
        filtered.pop("meta_data")
    if filtered.get("_links"):
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
    schema = load_schema(schema_name, CONFIG["schemas_path"])
    singer.write_schema(schema_name, schema, key_properties)

    start = get_start(STATE, schema_name, "last_update")
    LOGGER.info("Only syncing %s updated since %s" % (schema_name, start))
    last_update = start
    page_number = 1
    with metrics.record_counter(schema_name) as counter:
        while True:
            params = {"start_date": urllib.parse.quote(start),
                      "end_date": urllib.parse.quote(CONFIG["end_date"]),
                      "items_per_page": CONFIG["items_per_page"],
                      "current_page": page_number
                      }
            endpoint = get_endpoint(schema_name, params)
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
            if len(rows) < CONFIG["items_per_page"]:
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


@utils.backoff((backoff.expo,requests.exceptions.RequestException), giveup)
@utils.ratelimit(20, 1)
def gen_modified_items_request(stream_id, url):
    with metrics.http_request_timer(stream_id) as timer:
        headers = { 'User-Agent': USER_AGENT }
        resp = requests.get(url,
                headers=headers,
                auth=HTTPBasicAuth(CONFIG["username"], CONFIG["password"]))
        timer.tags[metrics.Tag.http_status_code] = resp.status_code
        resp.raise_for_status()
        return resp.json()


def sync_modified_rows(STATE, catalog, schema_name="orders", key_properties=["order_id"]):
    schema = load_schema(schema_name, CONFIG["schemas_path"])
    singer.write_schema(schema_name, schema, key_properties)

    start = get_start(STATE, schema_name, "last_update")
    last_update = start
    offset = 0

    utc_now = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
    start_datetime = parser.parse(start)
    if start_datetime.tzinfo is None:
        start_datetime = start_datetime.replace(tzinfo=pytz.utc)
    datediff = utc_now - start_datetime
    datediff.seconds
    id_set = set()

    start_process_at = datetime.datetime.now()
    LOGGER.info("Starting %s Sync at %s" % (schema_name, str(start_process_at)))
    LOGGER.info("Only syncing %s updated since %s" % (schema_name, start))

    while True:
        LOGGER.info("Offset: %d" % offset)
        # First get the list of IDs
        params = {"resource": schema_name,
                  "days": datediff.days,
                  "hours": datediff.seconds / 3600,
                  "offset": offset,
                  "items_per_page": CONFIG["items_per_page"]}
        endpoint = get_endpoint("modified_items", params)
        LOGGER.info("GET %s", endpoint)
        rows = gen_modified_items_request(schema_name,endpoint)
        for row in rows[schema_name]:
            # last_updated is an unix timestamp
            current_timestamp = None
            if row["last_updated"]:
                current_timestamp = datetime.datetime.utcfromtimestamp(int(row["last_updated"])).replace(tzinfo=pytz.utc)
            end_date = parser.parse(CONFIG["end_date"])
            if end_date.tzinfo is None:
                end_date = end_date.replace(tzinfo=pytz.utc)
            if CONFIG.get("end_date") is None or row["last_updated"] is None or (current_timestamp and current_timestamp < end_date):
                    id_set.add(row["id"])

        if len(rows[schema_name]) < CONFIG["items_per_page"]:
            LOGGER.info("End of records %d" % len(rows[schema_name]))
            break
        else:
            offset = offset + CONFIG["items_per_page"]

    LOGGER.info("Found %d records" % len(id_set))
    ids = list(id_set)
    with metrics.record_counter(schema_name) as counter:
        current_idx = 0
        while current_idx < len(ids):
            params = {"resource": schema_name,
                      "ids": ",".join(ids[current_idx:min(len(ids), current_idx + INCREMENTAL_ITEMS_PER_PAGE)])}
            endpoint = get_endpoint(schema_name + "_by_id", params)
            LOGGER.info("GET %s", endpoint)
            rows = gen_request(schema_name,endpoint)
            if len(rows) < len(ids[current_idx:min(len(ids), current_idx + INCREMENTAL_ITEMS_PER_PAGE)]):
                LOGGER.warning("Number of items returned from WC API is lower than the ID list size")
            for row in rows:
                counter.increment()
                row = filter_result(row, schema)
                if "_etl_tstamp" in schema["properties"].keys():
                    row["_etl_tstamp"] = time.time()
                singer.write_record(schema_name, row)
            current_idx = current_idx + INCREMENTAL_ITEMS_PER_PAGE

    STATE = singer.write_bookmark(STATE, schema_name, 'last_update', last_update)
    singer.write_state(STATE)
    end_process_at = datetime.datetime.now()
    LOGGER.info("Completed %s Sync at %s" % (schema_name, str(end_process_at)))
    LOGGER.info("Process duration: " + str(end_process_at - start_process_at))

    return STATE

def sync_modified_orders(STATE, catalog):
    sync_modified_rows(STATE, catalog, schema_name="orders", key_properties=["id"])

def sync_modified_subscriptions(STATE, catalog):
    sync_modified_rows(STATE, catalog, schema_name="subscriptions", key_properties=["id"])

def sync_modified_customers(STATE, catalog):
    sync_modified_rows(STATE, catalog, schema_name="customers", key_properties=["id"])


@attr.s
class Stream(object):
    tap_stream_id = attr.ib()
    sync = attr.ib()

STREAMS = {"orders": [Stream("orders", sync_orders)],
           "subscriptions": [Stream("subscriptions", sync_subscriptions)],
           "customers": [Stream("customers", sync_customers)],
           "modified_orders": [Stream("orders", sync_modified_orders)],
           "modified_subscriptions": [Stream("subscriptions", sync_modified_subscriptions)],
           "modified_customers": [Stream("customers", sync_modified_customers)],
           }


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
    schema = load_schema(stream.tap_stream_id, CONFIG["schemas_path"])
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


def parse_args(required_config_keys):
    ''' This is to replace singer's default utils.parse_args()
    https://github.com/singer-io/singer-python/blob/master/singer/utils.py

    Parse standard command-line args.
    Parses the command-line arguments mentioned in the SPEC and the
    BEST_PRACTICES documents:
    -c,--config     Config file
    -s,--state      State file
    -d,--discover   Run in discover mode
    -p,--properties Properties file: DEPRECATED, please use --catalog instead
    --catalog       Catalog file
    Returns the parsed args object from argparse. For each argument that
    point to JSON files (config, state, properties), we will automatically
    load and parse the JSON file.
    '''
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config',
        help='Config file',
        required=True)

    parser.add_argument(
        '-s', '--state',
        help='State file')

    parser.add_argument(
        '-p', '--properties',
        help='Property selections: DEPRECATED, Please use --catalog instead')

    parser.add_argument(
        '--catalog',
        help='Catalog file')

    parser.add_argument(
        '-d', '--discover',
        action='store_true',
        help='Do schema discovery')

    # Capture additional args
    parser.add_argument(
        "--start_date", type=str, default=None,
        help="Inclusive start date time in ISO8601-Date-String format: 2019-04-11T00:00:00Z")
    parser.add_argument(
        "--end_date", type=str, default=None,
        help="Exclusive end date time in ISO8601-Date-String format: 2019-04-12T00:00:00Z")

    parser.add_argument(
        "--modified_items_only", type=str, default=None,
        help="When True, use v1 API to fetch the modified items between start_date and end_date")

    parser.add_argument(
        "--schemas_path", type=str, default=None,
        help="Path to schemas dir. Default schemas are used when not specified.")

    args = parser.parse_args()
    if args.config:
        args.config = utils.load_json(args.config)
    if args.state:
        args.state = utils.load_json(args.state)
    else:
        args.state = {}
    if args.properties:
        args.properties = utils.load_json(args.properties)
    if args.catalog:
        args.catalog = Catalog.load(args.catalog)

    utils.check_config(args.config, required_config_keys)

    return args


@utils.handle_top_exception(LOGGER)
def main():
    '''Entry point'''
    args = parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)

    # Overwrite config specs with commandline args if present
    if args.start_date:
        CONFIG["start_date"] = args.start_date
    if args.end_date:
        CONFIG["end_date"] = args.end_date

    if args.modified_items_only:
        if args.modified_items_only.lower() in ["true", "yes", "t", "y"]:
            CONFIG["modified_items_only"] = True
        elif args.modified_items_only.lower() in ["false", "no", "f", "n"]:
            CONFIG["modified_items_only"] = False
        else:
            raise ValueError("Boolean indicator expected for modified_items_only")

    if args.schemas_path:
        CONFIG["schemas_path"] = args.schemas_path

    if not CONFIG.get("end_date"):
        CONFIG["end_date"]  = datetime.datetime.utcnow().isoformat()
    STATE = {}

    schema = CONFIG["schema"]
    if CONFIG.get("modified_items_only") is True:
        schema = "modified_" + schema

    if args.state:
        STATE.update(args.state)
    if args.discover:
        do_discover()
    elif args.catalog:
        do_sync(STATE, args.catalog, schema)
    else:
        LOGGER.info("No Streams were selected")

if __name__ == "__main__":
    main()
