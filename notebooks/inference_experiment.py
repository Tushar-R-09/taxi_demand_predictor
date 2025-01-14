from datetime import datetime, timezone
import pandas as pd

current_date = pd.to_datetime(datetime.now(timezone.utc)).floor('H')
print(f'{current_date = }')

from src.inference import load_batch_of_features_from_store

features = load_batch_of_features_from_store(current_date)