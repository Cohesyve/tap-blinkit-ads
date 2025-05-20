from typing import Optional
from tap_blinkit_ads.streams.base import BaseStream

import singer
import json

LOGGER = singer.get_logger()  # noqa


class CampaignsStream(BaseStream):
    API_METHOD = 'POST'
    TABLE = 'campaigns'
    KEY_PROPERTIES = ['id']
    REPLICATION_METHOD = 'FULL_TABLE'
    CACHE = True

    @property
    def api_path(self):
        return '/adservice/v1/advertisers/campaigns'
    
    def get_body(self):
        return {
            "from_date": "1/1/2025",
            "to_date":"5/15/2025",
            "campaign_types":[
                "PRODUCT_LISTING",
                "BANNER_LISTING",
                "PRODUCT_RECOMMENDATION",
                "SEARCH_SUGGESTION",
                "BRAND_BOOSTER"
            ]
        }

    def get_stream_data(self, result):
        results = []

        for record in result["data"].get("campaigns", []):
            transformed_record = self.transform_record(record)
            LOGGER.info(f"Transformed record: {record}")
            results.append(transformed_record)

        return results
