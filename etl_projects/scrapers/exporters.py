import gzip

from scrapy.exporters import JsonLinesItemExporter


class JsonLinesGzipItemExporter(JsonLinesItemExporter):
    """
    Exporter for .gz format.
    To use it, add
    ::

        FEED_EXPORTERS = {
            'jl.gz': 'trovit_spider.exporters.JsonLinesGzipItemExporter',
        }
        FEED_FORMAT = 'gz'

    to settings.py and then run scrapy crawl like this::

        scrapy crawl foo -o s3://path/to/items.gz

    (if `FEED_FORMAT` is not explicitly specified, you'll need to add
    `-t .gz` to the command above)
    """

    def __init__(self, file, **kwargs):
        gzfile = gzip.GzipFile(fileobj=file, mode="wb")
        super(JsonLinesGzipItemExporter, self).__init__(gzfile, **kwargs)

    def finish_exporting(self):
        self.file.close()
