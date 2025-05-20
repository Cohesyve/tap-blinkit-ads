
from tap_blinkit_ads.streams.campaigns import CampaignsStream
from tap_blinkit_ads.streams.campaign_details import CampaignDetailsStream
from tap_blinkit_ads.streams.campaign_performance import CampaignPerformanceStream
from tap_blinkit_ads.streams.advertisers import AdvertisersStream
from tap_blinkit_ads.streams.products import ProductsStream
from tap_blinkit_ads.streams.sponsored_sov import SponsoredSOVStream
from tap_blinkit_ads.streams.campaign_keyword_performance import CampaignKeywordPerformanceStream

AVAILABLE_STREAMS = [
    CampaignsStream,
    CampaignDetailsStream,
    AdvertisersStream,
    ProductsStream,
    SponsoredSOVStream,
    CampaignKeywordPerformanceStream,
    CampaignPerformanceStream
]

__all__ = [
    'CampaignsStream',
    'CampaignDetailsStream',
    'AdvertisersStream',
    'ProductsStream',
    'SponsoredSOVStream',
    'CampaignKeywordPerformanceStream',
    'CampaignPerformanceStream'
]
