HOPSWORK_PROJECT_NAME = "taxidemand"

import os
from dotenv import load_dotenv
from src.paths import PARENT_DIR
from src.data import transform_raw_data_into_ts_data
import hopsworks
from confluent_kafka import Producer
import src.config as config


load_dotenv(PARENT_DIR / ".env")

HOPSWORK_API_KEY = os.environ["HOPSWORKS_API_KEY"]
project = hopsworks.login(
    project = HOPSWORK_PROJECT_NAME,
    api_key_value = HOPSWORK_API_KEY
)

FEATURE_GROUP_NAME = config.FEATURE_GROUP_NAME
FEATURE_GROUP_VERSION = config.FEATURE_GROUP_VERSION


from datetime import datetime
import pandas as pd
from src.data import load_raw_data

feature_store = project.get_feature_store()

feature_group = feature_store.get_or_create_feature_group(
    name = FEATURE_GROUP_NAME,
    version = FEATURE_GROUP_VERSION,
    description = "Time-series data at hourly frequency",
    primary_key = ["pickup_location_id","pickup_hour"],
    event_time = "pickup_hour"
)

from_year = 2022
to_year = datetime.now().year
print(f'Downloading raw data from {from_year} to {to_year}')

rides = pd.DataFrame()
for year in range(from_year, to_year + 1):
    rides_one_year = load_raw_data(year)
    rides = rides_one_year
    ts_data = transform_raw_data_into_ts_data(rides)
    feature_group.insert(ts_data, write_options={"wait_for_job": False})