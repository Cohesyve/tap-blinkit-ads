
import singer
import argparse
from dateutil.parser import parse
import json

LOGGER = singer.get_logger()  # noqa

def get_config_start_date(config):
    return parse(config.get('start_date')).date()

def read_json_file(filename):
    # read file
    with open(f"{filename}", 'r') as filetoread:
        data = filetoread.read()

    # parse file
    content = json.loads(data)

    return content

def write_json_file(filename, content):
    with open(filename, 'w') as f:
        json.dump(content, f, indent=4)

def update_config(updated_config):
    """Update the config with the new state"""
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file', required=True)
    _args, unknown = parser.parse_known_args()
    config_file = _args.config

    config_content = read_json_file(config_file)
    write_json_file(config_file, updated_config)

    LOGGER.info("Config.json updated!")

    return updated_config