"""
This is an example for the feature store (end to end)
To show how you could use a custom offlinestore
"""
import pandas as pd
from pandas import Timestamp
from pathlib import Path
from feast import FeatureStore
from feast.repo_config import load_repo_config

# custom offline store
from feast_spark import FileOfflineStore
import databricks.koalas as ks


entity_df = pd.DataFrame(
    [
        {"datetime": Timestamp("2021-06-11 00:00:00"), "driver_id": 1005},
        {"datetime": Timestamp("2021-06-12 00:00:00"), "driver_id": 1005},
        {"datetime": Timestamp("2021-06-13 00:00:00"), "driver_id": 1001},
        {"datetime": Timestamp("2021-06-14 00:00:00"), "driver_id": 1001},
        {"datetime": Timestamp("2021-06-15 00:00:00"), "driver_id": 1005},
        {"datetime": Timestamp("2021-06-16 00:00:00"), "driver_id": 1005},
        {"datetime": Timestamp("2021-06-17 00:00:00"), "driver_id": 1003},
        {"datetime": Timestamp("2021-06-18 00:00:00"), "driver_id": 1003},
        {"datetime": Timestamp("2021-06-19 00:00:00"), "driver_id": 1003},
        {"datetime": Timestamp("2021-06-20 00:00:00"), "driver_id": 1005},
    ]
)


# this bit does't quite work using cli
config = load_repo_config(Path("feature_store"))
config.offline_store = FileOfflineStore()

store = FeatureStore(config=config)

training_df = store.get_historical_features(
    entity_df=entity_df,
    feature_refs=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
    ],
)

print(ks.DataFrame(training_df.to_df()).sort_values("datetime"))
