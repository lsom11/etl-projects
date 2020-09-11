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

    def build_request(self, url_append):
        concatted_url = f"{self.__base_url}/{url_append}"
        driver = webdriver.Chrome()
        driver.get(concatted_url)
        html = driver.execute_script('return document.body.innerHTML;')
        return html

    def soupify(self, data):
        soup = BeautifulSoup(data, SOUP_PARSE_TYPE)
        return soup

    @abc.abstractmethod
    def parse(self, url_append):
        return
