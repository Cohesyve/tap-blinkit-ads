from tap_blinkit_ads.streams.base import ChildStream
from tap_blinkit_ads.cache import stream_cache

import singer
import json

LOGGER = singer.get_logger()  # noqa


class CampaignDetailsStream(ChildStream):
    API_METHOD = 'GET'
    TABLE = 'campaign_details'
    KEY_PROPERTIES = ['campaign_id']
    REPLICATION_METHOD = 'FULL_TABLE'
    CACHE = True

    def sync_data(self):
        for campaign in stream_cache['campaigns']:
            url = self.get_url(f"/adservice/v1/campaigns/{campaign['id']}")
            self.sync_child_data(url=url)

    def get_stream_data(self, result):
        return [
            self.transform_record(result['data']['campaign'])
        ]
