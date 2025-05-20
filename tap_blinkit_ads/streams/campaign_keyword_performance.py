from tap_blinkit_ads.streams.base import ChildStream
from tap_blinkit_ads.cache import stream_cache

import singer
import json

LOGGER = singer.get_logger()  # noqa


class CampaignKeywordPerformanceStream(ChildStream):
    API_METHOD = 'GET'
    TABLE = 'campaign_keyword_performance'
    KEY_PROPERTIES = ['keyword']
    REPLICATION_METHOD = 'FULL_TABLE'

    def sync_data(self):
        for campaign in stream_cache['campaign_details']:
            url = self.get_url(f"/adservice/v1/campaigns/keywords/attributes")
            if len(campaign['keywords']) == 0:
                LOGGER.info(f"Skipping campaign {campaign['id']} as it has no keywords.")
                continue
            params = {
                "keywords": ','.join(keyword['keyword'] for keyword in campaign['keywords']),
                "campaign_type": campaign['campaign_type'],
                "campaign_id": campaign['id'],
            }
            self.sync_child_data(url=url, params=params)

    def get_stream_data(self, result):
        results = []

        for record in result["data"].get("keyword_attributes", []):
            transformed_record = self.transform_record(record)
            LOGGER.info(f"Transformed record: {record}")
            results.append(transformed_record)

        return results
