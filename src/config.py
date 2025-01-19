import os
from dotenv import load_dotenv
from src.paths import PARENT_DIR
from src.feature_store_api import FeatureGroupConfig, FeatureViewConfig

load_dotenv(PARENT_DIR / ".env")

try:
    HOPSWORKS_API_KEY = os.environ["HOPSWORKS_API_KEY"]

except:
    raise Exception(
        "Please set HOPSWORK_API_KEY in your .env file."
    )
HOPSWORKS_PROJECT_NAME = "taxidemand"

FEATURE_GROUP_NAME = "time_series_hourly_feature_group"
FEATURE_GROUP_VERSION = 1
FEATURE_VIEW_NAME = "time_series_hourly_feature_view"
FEATURE_VIEW_VERSION = 1
N_FEATURES = 24 * 28
MODEL_NAME = "taxi_demand_model"
MODEL_VERSION = 1
FEATURE_VIEW_MODEL_PREDICTIONS = 'model_predictions_feature_view'
FEATURE_GROUP_MODEL_PREDICTIONS = 'model_predictions_feature_group'

FEATURE_GROUP_PREDICTIONS_METADATA = FeatureGroupConfig(
    name=FEATURE_GROUP_MODEL_PREDICTIONS,
    version=1,
    description='Predictions generate by our production model',
    primary_key=['pickup_location_id', 'pickup_hour'],
    event_time='pickup_hour',
)

FEATURE_VIEW_PREDICTIONS_METADATA = FeatureViewConfig(
    name=FEATURE_VIEW_MODEL_PREDICTIONS,
    version=1,
    feature_group=FEATURE_GROUP_PREDICTIONS_METADATA,
)


FEATURE_GROUP_METADATA = FeatureGroupConfig(
    name=FEATURE_GROUP_NAME,
    version=1,
    description='Feature group with hourly time-series data of historical taxi rides',
    primary_key=['pickup_location_id', 'pickup_hour'],
    event_time='pickup_hour',
    online_enabled=True,
)

MONITORING_FV_NAME = 'monitoring_feature_view'
MONITORING_FV_VERSION = 1