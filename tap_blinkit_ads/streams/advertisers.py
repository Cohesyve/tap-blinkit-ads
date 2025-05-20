from tap_blinkit_ads.streams.base import BaseStream

import singer
import json

LOGGER = singer.get_logger()  # noqa


class AdvertisersStream(BaseStream):
    API_METHOD = 'GET'
    TABLE = 'advertisers'
    KEY_PROPERTIES = ['id']

    @property
    def api_path(self):
        return '/adservice/v1/advertisers'

    def get_stream_data(self, result):
        return [
            self.transform_record(record)
            for record in result['items']
        ]
