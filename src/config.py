import os
from dotenv import load_dotenv
from src.paths import PARENT_DIR

load_dotenv(PARENT_DIR / ".env")

try:
    HOPSWORKS_API_KEY = os.environ["HOPSWORKS_API_KEY"]

except:
    raise Exception(
        "Please set HOPSWORK_API_KEY in your .env file."
    )
HOPSWORKS_PROJECT_NAME = "taxidemand"

FEATURE_GROUP_NAME = "time_series_hourly_feature_group_v2"
FEATURE_GROUP_VERSION = 1
FEATURE_VIEW_NAME = "time_series_hourly_feature_view_v2"
FEATURE_VIEW_VERSION = 1
N_FEATURES = 24 * 28
MODEL_NAME = "taxi_demand_model"
MODEL_VERSION = 3
FEATURE_VIEW_MODEL_PREDICTIONS = 'model_predictions_feature_view'
FEATURE_GROUP_MODEL_PREDICTIONS = 'model_predictions_feature_group'