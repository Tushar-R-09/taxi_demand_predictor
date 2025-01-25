import src.config as config
from datetime import datetime, timedelta, timezone
from argparse import ArgumentParser
import pandas as pd
from src.data import transform_raw_data_into_ts_data, fetch_ride_events_from_data_warehouse
from src.logger import get_logger
from src.feature_store_api import get_or_create_feature_group

logger = get_logger()


def run(date: datetime):


    fetch_date_to = date

    fetch_date_from = date - timedelta(days = 28)

    

    rides = fetch_ride_events_from_data_warehouse(from_date=fetch_date_from, to_date=fetch_date_to)
    ts_data = transform_raw_data_into_ts_data(rides)

    location_id_per_pickup_hour = set(list(ts_data.groupby('pickup_hour')['pickup_location_id'].nunique()))
    pickup_hour_per_location_id = set(list(ts_data.groupby('pickup_location_id')['pickup_hour'].nunique()))

    assert len(location_id_per_pickup_hour) == 1 and len(pickup_hour_per_location_id) == 1

    ts_data['pickup_location_id'] = ts_data['pickup_location_id'].astype('int64')

    logger.info('Adding column `pickup_ts` with Unix seconds...')
    ts_data['pickup_hour'] = pd.to_datetime(ts_data['pickup_hour'], utc=True)
    ts_data['pickup_ts'] = ts_data['pickup_hour'].view('int64') // 10**6

    logger.info('Getting pointer to the feature group we wanna save data to')
    logger.info(f"project name and api key is {config.HOPSWORKS_PROJECT_NAME, config.HOPSWORKS_API_KEY}")
    feature_group = get_or_create_feature_group(config.FEATURE_GROUP_METADATA)


    logger.info('Starting job to insert data into feature group...')
    feature_group.insert(ts_data, write_options={"wait_for_job": False})

    logger.info('Finished job to insert data into feature group')



if __name__ == '__main__':
    # parse command line arguments
    parser = ArgumentParser()
    parser.add_argument(
        '--datetime',
        type=lambda s: datetime.strptime(s, '%Y-%m-%d %H:%M:%S'),
        help='Datetime argument in the format of YYYY-MM-DD HH:MM:SS',
    )
    args = parser.parse_args()

    # if args.datetime was provided, use it as the current_date, otherwise
    # use the current datetime in UTC
    if args.datetime:
        current_date = pd.to_datetime(args.datetime)
    else:
        current_date = pd.to_datetime(datetime.now(timezone.utc)).floor('H')

    logger.info(f'Running feature pipeline for {current_date=}')
    run(current_date)