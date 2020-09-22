from spidermon import Monitor, MonitorSuite, monitors
from spidermon.contrib.monitors.mixins import StatsMonitorMixin


@monitors.name("Campaign count")
class CampaignCountMonitor(Monitor):
    @monitors.name("Minimum number of campaigns")
    def test_minimum_number_of_campaigns(self):
        # assert
        item_extracted = getattr(self.data.stats, "item_scraped_count", 0)
        minimum_threshold = 0

        self.assertTrue(
            item_extracted >= minimum_threshold,
            msg="The number of extracted campaigns {} is less than the defined "
            "threshold {}".format(item_extracted, minimum_threshold),
        )


@monitors.name("Campaign validation")
class CampaignValidationMonitor(Monitor, StatsMonitorMixin):
    @monitors.name("No campaign validation errors")
    # assert
    def test_no_campaign_validation_errors(self):
        validation_errors = getattr(self.stats, "spidermon/validation/fields/errors", 0)
        self.assertEqual(
            validation_errors,
            0,
            msg="Found validation errors in {} fields".format(validation_errors),
        )


class SpiderCloseMonitorSuite(MonitorSuite):
    monitors = [CampaignCountMonitor, CampaignValidationMonitor]
