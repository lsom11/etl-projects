from schematics.models import Model
from schematics.types import FloatType, IntType, StringType


class TrovitCampaign(Model):
    id = IntType(required=True)
    name = StringType(required=True)
    clicks = IntType(required=True)
    total_cost = FloatType(required=True)
    desktop_cost = FloatType(required=True)
    mobile_cost = FloatType(required=True)
    curr_date = StringType(required=True, regex="\d{4}-\d{2}-\d{2}")
    group_name = StringType(required=True)
