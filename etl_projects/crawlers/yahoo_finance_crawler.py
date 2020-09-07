from etl_projects.crawlers import BaseCrawler

BASE_URL = 'http://finance.yahoo.com/quote'


def build_ticker_url(ticker):
    return f"{ticker}?p={ticker}"


class YahooFinanceCrawler(BaseCrawler):
    def __init__(self, timeout):
        super(YahooFinanceCrawler, self).__init__(timeout, BASE_URL)

    def parse(self, ticker):
        url_append = build_ticker_url(ticker)
        r = self.build_request(url_append)
        soup = self.soupify(r)
        return soup
