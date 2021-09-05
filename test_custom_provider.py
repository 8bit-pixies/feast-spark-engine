import os
from datetime import datetime

import pandas as pd
from feast import FeatureStore

from basic_feature_repo.repo import driver, driver_hourly_stats_view


def test_end_to_end():
    fs = FeatureStore("basic_feature_repo/")

    # apply repository
    fs.apply([driver, driver_hourly_stats_view])

    # load data into online store
    fs.materialize_incremental(end_date=datetime.now())

    # Read features from online store
    feature_vector = fs.get_online_features(
        features=["driver_hourly_stats:conv_rate"], entity_rows=[{"driver_id": 1001}]
    ).to_dict()
    conv_rate = feature_vector["conv_rate"][0]
    assert conv_rate > 0

    # tear down feature store

    # test get_historical
    feature_df = fs.get_historical_features(
        features=["driver_hourly_stats:conv_rate"],
        entity_df=pd.DataFrame(
            [{"driver_id": 1001, "event_timestamp": datetime.now()}]
        ),
    )

    df = feature_df.to_df()
    if "to_koalas" in dir(df):
        df = df.to_koalas()

    print(df)
    fs.teardown()


def test_cli():
    os.system(
        "PYTHONPATH=$PYTHONPATH:/$(pwd) feast -c basic_feature_repo apply > output"
    )
    with open("output", "r") as f:
        output = f.read()

    if "Launching custom streaming jobs is pretty easy" not in output:
        raise Exception(
            'Failed to successfully use provider from CLI. See "output" for more details.'
        )


test_end_to_end()
