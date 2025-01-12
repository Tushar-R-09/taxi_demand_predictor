import src.config as config
from datetime import datetime, timedelta

import pandas as pd

current_date = pd.to_datetime(datetime.utcnow()).floor('H')
print(f'{current_date = }')

fetch_date_to = current_date

fetch_date_from = current_date - timedelta(days = 28)
from src.data import load_raw_data

def fetch_batch_raw_data(from_date: datetime, to_date: datetime) -> pd.DataFrame:
    from_date_ = from_date - timedelta(days = 7*52)
    to_date_ = to_date - timedelta(days = 7*52)

    rides = load_raw_data(year = from_date_.year, months = from_date_.month)
    rides = rides[rides.pickup_datetime >= from_date_]

    rides_2 = load_raw_data(year = to_date_.year, months=to_date_.month)
    rides_2 = rides_2[rides_2.pickup_datetime < to_date_]
    rides = pd.concat([rides, rides_2])

    rides["pickup_datetime"] += timedelta(days=7*52)

    rides.sort_values(by = ["pickup_location_id", "pickup_datetime"], inplace = True)
    return rides

rides = fetch_batch_raw_data(from_date=fetch_date_from, to_date=fetch_date_to)

from src.data import transform_raw_data_into_ts_data
ts_data = transform_raw_data_into_ts_data(rides)