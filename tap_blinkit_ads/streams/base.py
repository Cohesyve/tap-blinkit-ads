import math
import os
from typing import Optional
import pytz
import singer
import singer.utils
import singer.metrics
import time
import datetime
import json
import argparse
from tap_blinkit_ads.client import BlinkitAdsClient
from tap_blinkit_ads.config import get_config_start_date
from tap_blinkit_ads.state import incorporate, save_state, \
    get_last_record_value_for_table

from tap_framework.streams import BaseStream as base
from tap_blinkit_ads.cache import stream_cache

LOGGER = singer.get_logger()

DEFAULT_BASE_URL = 'https://brands.blinkit.com'

class BaseStream(base):
    KEY_PROPERTIES = ['id']
    ACCEPT = None
    CONTENT_TYPE = None
    CACHE = False

    def get_params(self):
        return {}

    def get_body(self):
        return {}

    def get_url(self, path):
        api_base_url = DEFAULT_BASE_URL
        return '{}{}'.format(api_base_url, path)

    def transform_record(self, record, inject_profile=False):
        transformed = base.transform_record(self, record)

        return transformed

    def sync_data(self):
        table = self.TABLE
        LOGGER.info('Syncing data for entity {}'.format(table))

        url = self.get_url(self.api_path)
        params = self.get_params()
        body = self.get_body()

        headers = ({key: value for key, value in {
            'Accept': self.ACCEPT,
            'Content-Type': self.CONTENT_TYPE,
        }.items() if value}) or None

        client: BlinkitAdsClient = self.client

        result = client.make_request_json(
            url, self.API_METHOD, params=params, body=body, headers=headers)
        data = self.get_stream_data(result)

        with singer.metrics.record_counter(endpoint=table) as counter:
            for obj in data:
                singer.write_records(
                    table,
                    [obj])

                counter.increment()

        if self.CACHE:
            stream_cache[table].extend(data)
                    
        return self.state
    
class ChildStream(BaseStream):

    def sync_child_data(self, url=None, params=None, body=None):
        table = self.TABLE
        LOGGER.info('Syncing data for entity {}'.format(table))

        url = url if url else self.get_url(self.api_path)
        params = params if params else self.get_params()
        body = body if body else self.get_body()

        headers = ({key: value for key, value in {
            'Accept': self.ACCEPT,
            'Content-Type': self.CONTENT_TYPE,
        }.items() if value}) or None

        client: BlinkitAdsClient = self.client

        result = client.make_request_json(
            url, self.API_METHOD, params=params, body=body, headers=headers)
        data = self.get_stream_data(result)

        with singer.metrics.record_counter(endpoint=table) as counter:
            for obj in data:
                singer.write_records(
                    table,
                    [obj])

                counter.increment()

        if self.CACHE:
            stream_cache[table].extend(data)
                    
        return self.state

class PaginatedStream(BaseStream):

    def sync_data(self):
        table = self.TABLE
        LOGGER.info('Syncing data for entity {}'.format(table))

        body = self.get_body()
                
        client: BlinkitAdsClient = self.client

        headers = ({key: value for key, value in {
            'Accept': self.ACCEPT,
            'Content-Type': self.CONTENT_TYPE,
        }.items() if value}) or None

        page_count = 0
        while True:
            url = self.get_paginated_url(skip=page_count)

            LOGGER.info('Syncing from page {}'.format(page_count))
            try:
                result = client.make_request(
                    url, self.API_METHOD, body=body, headers=headers)
                data = self.get_stream_data(result.json())
                with singer.metrics.record_counter(endpoint=table) as counter:
                    for obj in data:
                        singer.write_records(
                            table,
                            [obj])
                        counter.increment()

                if len(data) < 25:
                    break

                page_count += 25
            except Exception as e:
                LOGGER.error('Error syncing data for entity {}: {}'.format(table, e))
                break
        return self.state


class ReportStream(BaseStream):
    
    def fetch_report_data(self, report_info, report_data):
        """Sync data from a completed report"""
        table = report_info["table"]
        LOGGER.info('Syncing data for entity {}'.format(table))

        # while sync_date <= yesterday:
        LOGGER.info("Syncing {}".format(table))

        data = report_data

        current_stream = [stream for stream in self.catalog.streams if stream.tap_stream_id == table][0]

        # Write schema message
        singer.write_schema(
            current_stream.stream,
            current_stream.schema.to_dict(),
            key_properties=current_stream.key_properties)

        with singer.metrics.record_counter(endpoint=table) as counter:
            for obj in data:
                singer.write_records(
                    table,
                    [obj])

                counter.increment()

        self.state = incorporate(self.state, table,
                      'last_record', str(datetime.datetime.fromisoformat(report_info["endDate"])))
        save_state(self.state)