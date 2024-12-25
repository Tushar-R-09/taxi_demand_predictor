import pandas as pd
from src.paths import TRANSFORMED_DATA_DIR
from sklearn.preprocessing import FunctionTransformer
from sklearn.base import BaseEstimator, TransformerMixin
import lightgbm as lgb
from sklearn.pipeline import make_pipeline, Pipeline

def average_rides_past_four_weeks(X: pd.DataFrame) -> pd.DataFrame:
    required_columns = [
        f'rides_previous_{7*24}_hour',
        f'rides_previous_{2*7*24}_hour',
        f'rides_previous_{3*7*24}_hour',
        f'rides_previous_{4*7*24}_hour'
    ]

    # Check if all required columns exist
    missing_columns = [col for col in required_columns if col not in X.columns]
    if missing_columns:
        raise KeyError(f"Missing columns in input DataFrame: {missing_columns}")

    # Calculate the average
    X['average_rides_last_4_weeks'] = (
        X[f'rides_previous_{7*24}_hour'] +
        X[f'rides_previous_{2*7*24}_hour'] +
        X[f'rides_previous_{3*7*24}_hour'] +
        X[f'rides_previous_{4*7*24}_hour']
    ) / 4

    return X

class TemporalFeatureTransformer(BaseEstimator, TransformerMixin):

    def fit(self, X, y = None):
        return self

    def transform(self, X, y = None):

        X_ = X.copy()

        X_["hour"] = X_["pickup_hour"].dt.hour
        X_["day_of_week"] = X_["pickup_hour"].dt.dayofweek

        return X_.drop(columns = ["pickup_hour"])
    
def get_pipeline(**hyperparams) -> Pipeline:

    #sklearn transform
    add_feature_average_rides_last_4_weeks = FunctionTransformer(
        average_rides_past_four_weeks, validate = False
    )

    #sklearn transform
    add_temporal_features = TemporalFeatureTransformer()

    return make_pipeline(
        add_feature_average_rides_last_4_weeks,
        add_temporal_features,
        lgb.LGBMRegressor(**hyperparams)
    )