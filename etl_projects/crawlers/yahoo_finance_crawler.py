from etl_projects.crawlers import BaseCrawler


BASE_URL = 'https://finance.yahoo.com/quote'
SOUP_PARSE_TYPE = 'lxml'


class YahooFinanceCrawler(BaseCrawler):
    def __init__(self, timeout):
        super(YahooFinanceCrawler, self).__init__(timeout, BASE_URL)

    def build_stats(self):
        self.set_stock_price()

    def set_stock_price(self):
        data = self.get_data()
        stock_price = [entry.text for entry in
                       data.find_all('span', {'class': 'Trsdu(0.3s) Fw(b) Fz(36px) Mb(-4px) D(ib)'})]
        self.__stock_price = stock_price[0]

    def get_stock_price(self):
        return self.__stock_price
