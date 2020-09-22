# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html


class CastToStrPipeline(object):
    def process_item(self, campaign, spider):
        campaign["id"] = str(campaign["id"])
        campaign["clicks"] = str(campaign["clicks"])
        campaign["total_cost"] = str(campaign["total_cost"])
        campaign["desktop_cost"] = str(campaign["desktop_cost"])
        campaign["mobile_cost"] = str(campaign["mobile_cost"])
        campaign["group_name"] = str(campaign["group_name"])
        return campaign
