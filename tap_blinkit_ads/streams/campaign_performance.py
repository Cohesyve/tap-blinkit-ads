from tap_blinkit_ads.streams.base import ReportStream
from tap_blinkit_ads.cache import stream_cache

import singer
import json
import pandas as pd
import io
import requests

LOGGER = singer.get_logger()  # noqa


class CampaignPerformanceStream(ReportStream):
    API_METHOD = 'POST'
    TABLE = 'campaign_performance'
    KEY_PROPERTIES = ['campaign_id']
    REPLICATION_METHOD = 'FULL_TABLE'

    @property
    def api_path(self):
        return '/adservice/v2/advertisers/campaigns/reports/download'
    
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
        report_url = result['data']['url']
        report_data = requests.get(report_url)

        excel_file = pd.ExcelFile(io.BytesIO(report_data.content))
        dfs = {sheet_name: excel_file.parse(sheet_name) 
               for sheet_name in excel_file.sheet_names}
        
        json_output = {
            "PRODUCT_LISTING": [],
            "BANNER_LISTING": [],
            "PRODUCT_RECOMMENDATION": [],
            "SEARCH_SUGGESTION": [],
            "BRAND_BOOSTER": []
        }
        
        for sheet_name, df in dfs.items():
            # Convert each sheet's DataFrame to JSON records
            sheet_json = df.to_dict(orient='records')
            json_output[sheet_name].extend(sheet_json)
        
        LOGGER.info("Data: %s", json_output)
        # return json_output
        # self.fetch_report_data(report_info, report_data)
