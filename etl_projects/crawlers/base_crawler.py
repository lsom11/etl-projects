import requests
import abc
import os
import random
from bs4 import BeautifulSoup


## rotate proxies
## rotate user agents
## send to s3

class BaseCrawler(object):
    def __init__(self, timeout, base_url):
        super(BaseCrawler, self).__init__()

        pwd = os.path.dirname(__file__)
        with open(pwd + '/helpers/user-agents.txt') as ua:
            self.__user_agents = [x.strip() for x in ua.readlines()]

        with open(pwd + '/helpers/proxies.txt') as ua:
            self.__proxies = [x.strip() for x in ua.readlines()]

        self.__timeout = timeout
        self.__base_url = base_url

    def get_headers(self):
        return {"accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "accept-encoding": "gzip, deflate, br",
                "accept-language": "en-GB,en;q=0.9,en-US;q=0.8,ml;q=0.7",
                "cache-control": "max-age=0",
                "dnt": "1",
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "none",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
                "user-agent": random.choice(self.__user_agents)}

    def new_session(self):
        s = requests.Session()
        headers = self.get_headers()

        s.headers = headers

        if len(self.__proxies) > 0:
            p = random.choice(self.__proxies)
            s.proxies = {'http': p, 'https': p}
        return s

    def build_request(self, url_append):
        concatted_url = f"{self.__base_url}/{url_append}"
        with self.new_session() as session:
            r = session.get(
                    concatted_url, verify=False, headers=session.headers, proxies=session.proxies, timeout=self.__timeout)
            return r

    def soupify(r):
        data = r.text
        soup = BeautifulSoup(data)
        return soup

    @abc.abstractmethod
    def parse(self, url_append):
        return
