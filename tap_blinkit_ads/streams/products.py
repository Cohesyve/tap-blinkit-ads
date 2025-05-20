from tap_blinkit_ads.streams.base import PaginatedStream

import singer
import json

LOGGER = singer.get_logger()  # noqa


class ProductsStream(PaginatedStream):
    API_METHOD = 'GET'
    TABLE = 'products'
    KEY_PROPERTIES = ['id']

    @property
    def api_path(self):
        return '/cms/products?format=json&ordering=-id&p_current_state=qc_approved'
    
    def get_paginated_url(self, skip=0, count=25):
        base_url = self.get_url(self.api_path)
        url = f"{base_url}&offset={int(skip)}&limit={int(count)}"
        return url

    def get_stream_data(self, result):
        return [
            self.transform_record(record)
            for record in result['results']
        ]
