import os
from dotenv import load_dotenv
from src.paths import PARENT_DIR

load_dotenv(PARENT_DIR / ".env")

try:
    HOPSWORK_API_KEY = os.environ["HOPSWORKS_API_KEY"]

except:
    raise Exception(
        "Please set HOPSWORK_API_KEY in your .env file."
    )
HOPSWORK_PROJECT_NAME = "taxidemand"

FEATURE_GROUP_NAME = "time_series_hourly_feature_group"
FEATURE_GROUP_VERSION = 1
FEATURE_VIEW_NAME = "time_series_hourly_feature_view"
FEATURE_VIEW_VERSION = 1