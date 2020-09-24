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

CURRENCY_ENUM = {
    'USD': 'USD',
    'CAD': 'CAD'
}


class YahooFinanceSpider(scrapy.Spider):
    data = {}
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
            url="https://finance.yahoo.com/quote/XIU.TO?p=XIU.TO",
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
        self.get_table_data(soup)
        self.get_table_information(soup)
        return self.data

    def get_table_data(self, data):
        table_data = {}
        summary_data = data.select('#quote-summary')
        ## don't need last bunch of tags
        for tag in summary_data[:2]:
            tables = tag.find_all('tr')
            for table in tables:
                tds = table.find_all('td')
                column_name = tds[0].text
                column_value = tds[1].text
                table_data[column_name] = column_value
        self.data.update(table_data)

    def get_table_information(self, data):
        table_data = {}
        summary_data = data.select('#Lead-3-QuoteHeader-Proxy')
        self.get_stock_name(summary_data[0])
        self.get_current_stock_price(summary_data[0])

    def get_stock_name(self, data):
        ## the stock info is in a h1 in the Lead-3-QuoteHeader-Proxy id
        raw_name = data.find_all('h1')[0].text
        split_name = raw_name.split('(')
        company_name = split_name[0]
        ticker = split_name[1].replace(')', '')
        parsed_data = {
            'company_name': company_name,
            'ticker': ticker
        }
        raw_currency_information = data.find_all('span')[0].text
        if 'Currency in USD' in raw_currency_information:
            parsed_data['currency'] = CURRENCY_ENUM['USD']
        else: parsed_data['currency'] = CURRENCY_ENUM['CAD']
        self.data.update(parsed_data)

    def get_current_stock_price(self, data):
        ## Current price info is in a span in the Lead-3-QuoteHeader-Proxy id, 3rd span
        spans = data.find_all('span')
        parsed_data = {
            'current_price': spans[3].text
        }
        self.data.update(parsed_data)
