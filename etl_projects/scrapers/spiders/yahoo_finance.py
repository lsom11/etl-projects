# -*- coding: utf-8 -*-
import json
import logging
import os
from datetime import datetime, timedelta

import scrapy
from scrapy import FormRequest
from scrapy_selenium import SeleniumRequest
from bs4 import BeautifulSoup

SOUP_PARSE_TYPE = 'lxml'


class YahooFinanceSpider(scrapy.Spider):
    name = "yahoo_finance"
    allowed_domains = ["finance.yahoo.com"]
    start_url = "https://finance.yahoo.com"

    def start_requests(self):
        yield SeleniumRequest(
            url=self.start_url,
            callback=self.scrape_stock,
            wait_time=10,
            script="window.scrollTo(0, document.body.scrollHeight);",
        )

    def scrape_stock(self, response):
        return FormRequest(
            url="https://finance.yahoo.com/quote/KO?p=KO",
            # formdata={"startDate": self.start_date, "endDate": self.end_date},
            callback=self.get_data,
        )

    def get_data(self, response):
        if response.status != 200:
            raise RuntimeError(
                "m={}, msg={}".format(
                    "get_data",
                    "Could not get stock "
                    "data. There was a problem "
                    "accessing the "
                    "url {}".format(response.request.url),
                )
            )

        soup = BeautifulSoup(response.text, SOUP_PARSE_TYPE)
        current_price_header = soup.select('D(ib) Mend(20px)')
        # current_price = current_price_header.find("span", class_='D(ib) Mend(20px)')
        # summary_data = soup.select('#quote-summary')
        print(current_price_header)
        # for tag in current_price_header:
            # print(tag, '\n')
            # yield {'title': title.css('a ::text').get()}

        # resp = json.loads(response.text)
        # if resp.get("success"):
        #     try:
        #         yield resp
        #     except Exception as e:
        #         logging.error(
        #             "m={}, msg={}, e={}, data={}".format(
        #                 "get_data",
        #                 "Could not "
        #                 "find "
        #                 "marketing "
        #                 "campaign "
        #                 "data in "
        #                 "the server "
        #                 "response. "
        #                 "The "
        #                 "schema of "
        #                 "the json "
        #                 "returned "
        #                 "was not "
        #                 "expected.",
        #                 e,
        #                 resp,
        #             )
        #         )
