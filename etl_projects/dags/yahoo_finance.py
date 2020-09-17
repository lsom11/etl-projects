from etl_projects.crawlers import YahooFinanceCrawler

TIMEOUT = 30

if __name__ == '__main__':
    ticker = 'AAPL'
    yahoo_crawler = YahooFinanceCrawler(TIMEOUT)
    yahoo_crawler.request_data(ticker)
    yahoo_crawler.build_stats()
    stock_price = yahoo_crawler.get_stock_price()
    print(stock_price)

