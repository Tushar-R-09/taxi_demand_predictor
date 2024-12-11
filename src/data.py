from pathlib import Path
import requests
import pandas as pd
import numpy as np
from tqdm import tqdm
from src.paths import RAW_DATA_DIR, TRANSFORMED_DATA_DIR
from typing import Optional, List
import os
from tqdm import tqdm


def download_one_file_of_raw_data(year: int, month: int) -> Path:
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
    response = requests.get(url)

    if response.status_code == 200:
        path = RAW_DATA_DIR / f"rides_{year}-{month:02d}.parquet"
        open(path, "wb").write(response.content)
        return path
    else:
        raise Exception(f"Failed to download {url}")
    

def validate_raw_data(rides: pd.DataFrame, 
                    year: int,
                    month: int) -> pd.DataFrame:
    
    this_month_start = f'{year}-{month:02d}-01'
    next_month_start = f'{year}-{month+1:02d}-01' if month < 12 else f'{year+1}-01-01'
    rides = rides[rides.pickup_datetime >= this_month_start]
    rides = rides[rides.pickup_datetime < next_month_start]

    return rides


def load_raw_data(
        year: int,
        months: Optional[List[int]] = None
) -> pd.DataFrame:
    
    rides = pd.DataFrame()

    if months is None:
        months = list(range(1, 13))

    elif isinstance(months, int):
        months = [months]

    for month in months:

        local_file = RAW_DATA_DIR / f'rides_{year}-{month:02d}.parquet'
        if not local_file.exists():
            try:

                print(f'Downloading file {year}-{month:02d}')
                download_one_file_of_raw_data(year, month)
            except:
                print(f'Failed to download {year}-{month:02d}')
                continue

        else:
            print(f" File {year}-{month:02d} already exists")

        rides_one_month = pd.read_parquet(local_file)

        rides_one_month = rides_one_month[['tpep_pickup_datetime', 'PULocationID']]

        rides_one_month.rename(columns = {
            'tpep_pickup_datetime': 'pickup_datetime',
            'PULocationID': 'pickup_location_id',}, inplace = True)
        
        rides_one_month = validate_raw_data(rides_one_month, year, month)

        rides = pd.concat([rides, rides_one_month])

    rides = rides[['pickup_datetime', 'pickup_location_id']]

    return rides

def add_missing_slots(agg_rides: pd.DataFrame) -> pd.DataFrame:

    location_ids = agg_rides["pickup_location_id"].unique()
    full_range = pd.date_range(
        agg_rides["pickup_hour"].min(),
        agg_rides["pickup_hour"].max(),
        freq="H"
    )
    output = pd.DataFrame()

    for location_id in tqdm(location_ids):
        agg_rides_i = agg_rides.loc[agg_rides.pickup_location_id == location_id, ['pickup_hour', 'rides']]

        agg_rides_i.set_index("pickup_hour", inplace=True)
        agg_rides_i.index = pd.to_datetime(agg_rides_i.index)

        agg_rides_i = agg_rides_i.reindex(full_range, method="ffill")

        agg_rides_i["pickup_location_id"] = location_id
        output = pd.concat([output, agg_rides_i])

    output = output.reset_index().rename(columns={"index": "pickup_hour"})

    return output

def transform_raw_data_into_ts_data(
        rides: pd.DataFrame
) -> pd.DataFrame:
    rides["pickup_hour"] = rides['pickup_datetime'].dt.floor('H')
    app_rides = rides.groupby(["pickup_hour", "pickup_location_id"]).size().reset_index()
    app_rides.rename(columns = {0: 'rides'}, inplace = True)
    agg_rides_all_slots = add_missing_slots(app_rides)
    return agg_rides_all_slots

def get_cutoff_indices(
        data: pd.DataFrame,
        n_features: int,
        step_size: int
) -> list:
    stop_position = len(data) -1
    subseq_first_idx = 0
    subseq_mid_index = n_features
    subseq_last_index = n_features + 1
    indices = []
    while subseq_last_index <= stop_position:
        indices.append((subseq_first_idx, subseq_mid_index, subseq_last_index))
        subseq_first_idx += step_size
        subseq_mid_index += step_size
        subseq_last_index += step_size

    return indices

def transform_ts_data_into_features_and_target(
        ts_data: pd.DataFrame,
        n_sez_len: int,
        step_size: int
) -> pd.DataFrame:
    assert set(ts_data.columns) == {'pickup_hour', 'rides', 'pickup_location_id'}

    location_ids = ts_data['pickup_location_id'].unique()
    features = pd.DataFrame()
    targets = pd.DataFrame()

    for location_id in tqdm(location_ids):
        ts_data_one_location = ts_data.loc[ts_data.pickup_location_id == location_id,
                                            ['pickup_hour', 'rides']
                                            ]
        indices = get_cutoff_indices(ts_data_one_location, n_sez_len, step_size)
        n_examples = len(indices)
        x = np.ndarray(shape = (n_examples, n_sez_len), dtype=np.float32)
        y = np.ndarray(shape = (n_examples), dtype=np.float32)
        pickup_hours = []
        for i, idx in enumerate(indices):
            x[i, :] = ts_data_one_location.iloc[idx[0]:idx[1]]['rides'].values
            y[i] = ts_data_one_location.iloc[idx[1]:idx[2]]['rides'].values
            pickup_hours.append(ts_data_one_location.iloc[idx[1]]['pickup_hour'])

        features_one_location = pd.DataFrame(
            x,
            columns = [f'rides_previous_{i+1}_hour' for i in reversed(range(n_sez_len))]
        )

        features_one_location['pickup_hour'] = pickup_hours
        features_one_location['pickup_location_id'] = location_id

        targets_one_location = pd.DataFrame(
            y,
            columns = [f'target_rides_next_hour']
        )

        features = pd.concat([features, features_one_location])
        targets = pd.concat([targets, targets_one_location])
    
    features.reset_index(drop=True, inplace=True)
    targets.reset_index(drop=True, inplace=True)

    return features, targets['target_rides_next_hour']

    


