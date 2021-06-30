"""
Minimal example which does "nothing"
"""
from typing import List, Optional, Union
import pandas as pd
import pyarrow
from datetime import datetime
from feast.data_source import DataSource

from feast.repo_config import RepoConfig
from feast.feature_view import FeatureView
from feast.registry import Registry
from feast.infra.offline_stores.offline_store import RetrievalJob, OfflineStore
from feast.infra.provider import DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL


class DummyRetrievalJob(RetrievalJob):
    def __init__(self, df):
        self.df = df

    def to_df(self):
        print("- dummy retrieval job.")
        return self.df


class DummyOfflineStore(OfflineStore):
    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: Registry,
        project: str,
    ) -> DummyRetrievalJob:
        print("- get historical features.")

        if not isinstance(entity_df, pd.DataFrame):
            raise ValueError(
                f"Please provide an entity_df of type {type(pd.DataFrame)} instead of type {type(entity_df)}"
            )
        entity_df_event_timestamp_col = DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL  # local modifiable copy of global variable
        if entity_df_event_timestamp_col not in entity_df.columns:
            datetime_columns = entity_df.select_dtypes(
                include=["datetime", "datetimetz"]
            ).columns
            if len(datetime_columns) == 1:
                print(
                    f"Using {datetime_columns[0]} as the event timestamp. To specify a column explicitly, please name it {DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL}."
                )
                entity_df_event_timestamp_col = datetime_columns[0]
            else:
                raise ValueError(
                    f"Please provide an entity_df with a column named {DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL} representing the time of events."
                )

        print("- feature refs")
        for fr in feature_refs:
            print("\t", fr)

        job = DummyRetrievalJob(entity_df)
        return job

    @staticmethod
    def pull_latest_from_table_or_query(
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> pyarrow.Table:
        pass
