import abc
from bs4 import BeautifulSoup
from selenium import webdriver
import pandas as pd

pd.options.display.float_format = '{:.0f}'.format
SOUP_PARSE_TYPE = 'lxml'


class BaseCrawler(object):
    def __init__(self, timeout, base_url):
        super(BaseCrawler, self).__init__()
        self.__base_url = base_url
        self.__url = None
        self.__data = None
        self.__driver = webdriver.Chrome()

    def request_data(self, url):
        self.__build_request_url(url)
        html_data = self.__get_html_data_from_driver()
        self.__soupify_data(html_data)

    def __build_request_url(self, url_append):
        concatted_url = f"{self.__base_url}/{url_append}"
        self.__url = concatted_url

    def __get_html_data_from_driver(self):
        self.__driver.get(self.__url)
        html = self.__driver.execute_script('return document.body.innerHTML;')
        return html

    def __soupify_data(self, data):
        self.__data = BeautifulSoup(data, SOUP_PARSE_TYPE)

    def get_data(self):
        return self.__data

    @abc.abstractmethod
    def parse(self):
        return
