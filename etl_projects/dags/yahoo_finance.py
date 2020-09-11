from etl_projects.crawlers import YahooFinanceCrawler

TIMEOUT = 30

if __name__ == '__main__':
    ticker = ''
    yahoo_crawler = YahooFinanceCrawler(TIMEOUT)
    data = yahoo_crawler.parse(ticker)
    print(data)
