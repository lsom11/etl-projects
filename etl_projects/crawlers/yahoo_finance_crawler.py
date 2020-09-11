from etl_projects.crawlers import BaseCrawler

# BASE_URL = 'https://finance.yahoo.com/quote/'
BASE_URL = 'https://finance.yahoo.com/quote/AAPL/financials?p=AAPL'
SOUP_PARSE_TYPE = 'lxml'


def build_ticker_url(ticker):
    formatted_url = f"{ticker}/financials?p={ticker}"
    return formatted_url


class YahooFinanceCrawler(BaseCrawler):
    def __init__(self, timeout):
        super(YahooFinanceCrawler, self).__init__(timeout, BASE_URL)

    def parse(self, ticker):
        url_append = build_ticker_url(ticker)
        r = self.build_request(url_append)
        soup = self.soupify(r)
        return soup
