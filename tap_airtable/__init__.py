import singer
import singer.bookmarks as bookmarks
import singer.metrics as metrics
from singer.catalog import Catalog, CatalogEntry
from singer import utils
import requests
import json
import re
import random
import string

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'metadata_url',
    'records_url',
    'token',
    'base_id',
    'selected_by_default',
    'remove_emojis'
]

CONFIG = {
    # in config.json
    "metadata_url": None,
    "records_url": None,
    "token": None,
    "base_id": None,
    "selected_by_default": None,
    "remove_emojis": False
}

class JsonUtils(object):

    @classmethod
    def remove_emojis(cls, record):
        emoji_pattern = re.compile("["
                                   u"\U0001F600-\U0001F64F"  # emoticons
                                   u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                   u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                   u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                   u"\U00002702-\U000027B0"
                                   u"\U000024C2-\U0001F251"
                                   "]+", flags=re.UNICODE)

        return emoji_pattern.sub(r'', record)

    @classmethod
    def match_record_with_keys(cls, schema, records, remove_emojis):

        records_to_dump = []

        if records is None:
            return records_to_dump

        for record in records:
            record_to_dump = {}
            id = record.get('id')

            for key in schema.get('properties').keys():
                if not record.get('fields').get(key):
                    record_to_dump[key] = None
                else:
                    if schema['properties'][key]['type'] == ["null", "string"] \
                            or schema['properties'][key]['type'] == ["null", "number"]:
                        if remove_emojis:
                            record_to_dump[key] = JsonUtils.remove_emojis(str(record.get('fields').get(key)))
                        else:
                            record_to_dump[key] = str(record.get('fields').get(key))
                    else:
                        if remove_emojis:
                            record_to_dump[key] = JsonUtils.remove_emojis(record.get('fields').get(key))
                        else:
                            record_to_dump[key] = record.get('fields').get(key)

                Relations.save_if_list_of_ids(record.get('fields').get(key), id)

            record_to_dump['id'] = record.get('id')

            records_to_dump.append(record_to_dump)

        return records_to_dump


class Relations(object):
    records = []

    @classmethod
    def save_if_list_of_ids(cls, record, id):
        if isinstance(record, list):
            cls.serialize_list_of_ids(record, id)

    @classmethod
    def serialize_list_of_ids(cls, record, id):
        for rec in record:
            record_to_save = {}
            if cls.is_rec_id(rec):
                record_to_save['id'] = cls.random_word(12)
                record_to_save['relation1'] = id
                record_to_save['relation2'] = rec
                cls.records.append(record_to_save)
            else:
                return

    @classmethod
    def is_rec_id(cls, rec):
        if isinstance(rec, str):
            return rec.startswith("rec")
        else:
            return False

    @classmethod
    def get_records(cls):
        return cls.records

    @classmethod
    def random_word(cls, length):
        letters = string.ascii_lowercase
        return ''.join(random.choice(letters) for i in range(length))


class Airtable(object):
    @classmethod
    def run_discovery(cls, config, args):
        headers = {'Authorization': 'Bearer {}'.format(config['token'])}
        response = requests.get(url=config['metadata_url'] + config['base_id'], headers=headers)
        entries = []

        for table in response.json()["tables"]:

            columns = {}
            table_name = table["name"]
            base = {"selected": config['selected_by_default'],
                    "name": table_name,
                    "properties": columns}

            columns["id"] = {"type": ["null", "string"], 'key': True}

            for field in table["fields"]:
                if not field["name"] == "Id":
                    columns[field["name"]] = {"type": ["null", "string"]}

            entry = CatalogEntry(
                table=table_name,
                stream=table_name,
                metadata=base)
            entries.append(entry)

        return Catalog(entries).dump()

    @classmethod
    def run_sync(cls, config, properties, arguments):
        streams = properties['streams']

        for stream in streams:
            table = re.sub('[^0-9a-zA-Z_]+', '_', stream['table_name']).lower()
            schema = stream['metadata']
            properties = {}

            # Clean field names
            for f_name in schema['properties'].keys():
                clean_f_name = re.sub('[^0-9a-zA-Z_]+', '_', f_name).lower()
                properties[clean_f_name] = schema['properties'][f_name]

            schema['properties'] = properties

            if table != 'relations' and schema['selected']:
                response = Airtable.get_response(config['base_id'], schema["name"])
                if response.json().get('records'):
                    response_records = response.json().get('records')
                    clean_response_records = []

                    for response_record in response_records:
                        fields = {}

                        # Clean field names
                        for f_name in response_record['fields'].keys():
                            clean_f_name = re.sub('[^0-9a-zA-Z_]+', '_', f_name).lower()
                            fields[clean_f_name] = response_record['fields'][f_name]

                        response_record['fields'] = fields
                        clean_response_records.append(response_record)

                    records = JsonUtils.match_record_with_keys(schema,
                                                               clean_response_records,
                                                               config['remove_emojis'])

                    singer.write_schema(table, schema, 'id')
                    singer.write_records(table, records)

                    offset = response.json().get("offset")

                    while offset:
                        response = Airtable.get_response(config['base_id'], schema["name"], offset)
                        if response.json().get('records'):
                            records = JsonUtils.match_record_with_keys(schema,
                                                                       response.json().get('records'),
                                                                       config['remove_emojis'])

                        singer.write_records(table, records)
                        offset = response.json().get("offset")

        relations_table = {"name": "relations",
                           "properties": {"id": {"type": ["null", "string"]},
                                          "relation1": {"type": ["null", "string"]},
                                          "relation2": {"type": ["null", "string"]}}}

        singer.write_schema('relations', relations_table, 'id')
        singer.write_records('relations', Relations.get_records())

    @classmethod
    def get_response(cls, base_id, table, offset=None):

        headers = {'Authorization': 'Bearer {}'.format(CONFIG['token'])}
        table = table.replace('/', '%2F')

        if offset:
            request = CONFIG['records_url'] + base_id + '/' + table + '?offset={}'.format(offset)
        else:
            request = CONFIG['records_url'] + base_id + '/' + table

        return requests.get(url=request, headers=headers)

def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    CONFIG.update(args.config)
    STATE = {}

    if args.state:
        STATE.update(args.state)
    elif args.discover:
        Airtable.run_discovery(args.config, args)
    elif args.properties:
        Airtable.run_sync(args.config, args.properties, args)
    else:
        LOGGER.info("No properties were selected")


if __name__ == "__main__":
    main()
