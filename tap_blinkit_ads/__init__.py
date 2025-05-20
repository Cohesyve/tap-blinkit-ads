#!/usr/bin/env python3

import singer

import tap_framework

from tap_blinkit_ads.client import BlinkitAdsClient
from tap_blinkit_ads.streams import AVAILABLE_STREAMS
from tap_blinkit_ads.streams.base import ReportStream
import json

LOGGER = singer.get_logger()  # noqa


class BlinkitAdsRunner(tap_framework.Runner):
    pass

@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.utils.parse_args(
        required_config_keys=['email'])

    client = BlinkitAdsClient(args.config)

    # report_stream = ReportStream(client.config, args.state, args.catalog, client)

    runner = BlinkitAdsRunner(
        args, client, AVAILABLE_STREAMS)

    if args.discover:
        runner.do_discover()
    else:
        # report_stream.sync_pending_reports(client.config)
        runner.do_sync()


if __name__ == '__main__':
    main()
