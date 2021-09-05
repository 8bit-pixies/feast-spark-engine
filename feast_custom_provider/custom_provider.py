from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import databricks.koalas as ks
import pandas as pd
import pyarrow
import pyspark
from feast.entity import Entity
from feast.feature_table import FeatureTable
from feast.feature_view import FeatureView
from feast.infra.local import LocalProvider
from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.infra.offline_stores.offline_utils import (
    DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
)

# deprecate this
from feast.infra.provider import _get_requested_feature_views_to_features_dict
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.registry import Registry
from feast.repo_config import RepoConfig
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql.functions import col, expr, monotonically_increasing_id, row_number
from pyspark.sql.types import LongType
from tqdm import tqdm

EVENT_TIMESTAMP_ALIAS = "event_timestamp"
CREATED_TIMESTAMP_ALIAS = "created_timestamp"


def as_of_join(
    entity_df: DataFrame,
    entity_event_timestamp_column: str,
    feature_table_df: DataFrame,
    feature_table: FeatureView,
    feature_columns: List[str],
) -> DataFrame:
    """Perform an as of join between entity and feature table, given a maximum age tolerance.
    Join conditions:
    1. Entity primary key(s) value matches.
    2. Feature event timestamp is the closest match possible to the entity event timestamp,
       but must not be more recent than the entity event timestamp, and the difference must
       not be greater than max_age, unless max_age is not specified.
    3. If more than one feature table rows satisfy condition 1 and 2, feature row with the
       most recent created timestamp will be chosen.
    4. If none of the above conditions are satisfied, the feature rows will have null values.
    Args:
        entity_df (DataFrame): Spark dataframe representing the entities, to be joined with
            the feature tables.
        entity_event_timestamp_column (str): Column name in entity_df which represents
            event timestamp.
        feature_table_df (Dataframe): Spark dataframe representing the feature table.
        feature_table (FeatureTable): Feature table specification, which provide information on
            how the join should be performed, such as the entity primary keys and max age.
    Returns:
        DataFrame: Join result, which contains all the original columns from entity_df, as well
            as all the features specified in feature_table, where the feature columns will
            be prefixed with feature table name.
    Example:
        >>> entity_df.show()
            +------+-------------------+
            |entity|    event_timestamp|
            +------+-------------------+
            |  1001|2020-09-02 00:00:00|
            +------+-------------------+
        >>> feature_table_1_df.show()
            +------+-------+-------------------+-------------------+
            |entity|feature|    event_timestamp|  created_timestamp|
            +------+-------+-------------------+-------------------+
            |    10|    200|2020-09-01 00:00:00|2020-09-02 00:00:00|
            +------+-------+-------------------+-------------------+
            |    10|    400|2020-09-01 00:00:00|2020-09-01 00:00:00|
            +------+-------+-------------------+-------------------+
        >>> feature_table_1.max_age
            None
        >>> feature_table_1.name
            'table1'
        >>> df = as_of_join(entity_df, "event_timestamp", feature_table_1_df, feature_table_1)
        >>> df.show()
            +------+-------------------+---------------+
            |entity|    event_timestamp|table1__feature|
            +------+-------------------+---------------+
            |  1001|2020-09-02 00:00:00|            200|
            +------+-------------------+---------------+
        >>> feature_table_2.df.show()
            +------+-------+-------------------+-------------------+
            |entity|feature|    event_timestamp|  created_timestamp|
            +------+-------+-------------------+-------------------+
            |    10|    200|2020-09-01 00:00:00|2020-09-02 00:00:00|
            +------+-------+-------------------+-------------------+
            |    10|    400|2020-09-01 00:00:00|2020-09-01 00:00:00|
            +------+-------+-------------------+-------------------+
        >>> feature_table_2.max_age
            43200
        >>> feature_table_2.name
            'table2'
        >>> df = as_of_join(entity_df, "event_timestamp", feature_table_2_df, feature_table_2)
        >>> df.show()
            +------+-------------------+---------------+
            |entity|    event_timestamp|table2__feature|
            +------+-------------------+---------------+
            |  1001|2020-09-02 00:00:00|           null|
            +------+-------------------+---------------+
    This is a patched version of: https://github.com/feast-dev/feast-spark/blob/master/python/feast_spark/pyspark/historical_feature_retrieval_job.py
    """
    entity_with_id = entity_df.withColumn("_row_nr", monotonically_increasing_id())

    event_timestamp_alias = feature_table.input.event_timestamp_column
    created_timestamp_alias = feature_table.input.created_timestamp_column

    feature_event_timestamp_column_with_prefix = (
        f"{feature_table.name}__{event_timestamp_alias}"
    )
    feature_created_timestamp_column_with_prefix = (
        f"{feature_table.name}__{created_timestamp_alias}"
    )

    projection = [
        col(col_name).alias(f"{feature_table.name}__{col_name}")
        for col_name in feature_table_df.columns
    ]

    aliased_feature_table_df = feature_table_df.select(projection)

    join_cond = (
        entity_with_id[entity_event_timestamp_column]
        >= aliased_feature_table_df[feature_event_timestamp_column_with_prefix]
    )
    if feature_table.ttl:
        join_cond = join_cond & (
            aliased_feature_table_df[feature_event_timestamp_column_with_prefix]
            >= entity_with_id[entity_event_timestamp_column]
            - expr(f"INTERVAL {feature_table.ttl.total_seconds()} seconds")
        )

    for key in feature_table.entities:
        join_cond = join_cond & (
            entity_with_id[key]
            == aliased_feature_table_df[f"{feature_table.name}__{key}"]
        )

    conditional_join = entity_with_id.join(
        aliased_feature_table_df, join_cond, "leftOuter"
    )
    for key in feature_table.entities:
        conditional_join = conditional_join.drop(
            aliased_feature_table_df[f"{feature_table.name}__{key}"]
        )

    window = Window.partitionBy("_row_nr", *feature_table.entities).orderBy(
        col(feature_event_timestamp_column_with_prefix).desc(),
        col(feature_created_timestamp_column_with_prefix).desc(),
    )
    filter_most_recent_feature_timestamp = conditional_join.withColumn(
        "_rank", row_number().over(window)
    ).filter(col("_rank") == 1)

    return filter_most_recent_feature_timestamp.select(
        entity_df.columns + feature_columns
    )


def _map_column(df: SparkDataFrame, col_mapping: Dict[str, str]):
    source_to_alias_map = {v: k for k, v in col_mapping.items()}
    # source_to_alias_map = col_mapping
    projection = [
        col(col_name).alias(source_to_alias_map.get(col_name, col_name))
        for col_name in df.columns
    ]
    return df.select(projection)


def _filter_feature_table_by_time_range(
    feature_table_df: SparkDataFrame,
    feature_view: FeatureView,
    entity_df: SparkDataFrame,
    entity_event_timestamp_column: str,
):
    feature_event_timestamp_column = feature_view.input.event_timestamp_column
    # feature_created_timestamp_column = feature_view.input.created_timestamp_column
    entity_max_timestamp = entity_df.agg(
        {entity_event_timestamp_column: "max"}
    ).collect()[0][0]
    entity_min_timestamp = entity_df.agg(
        {entity_event_timestamp_column: "min"}
    ).collect()[0][0]

    feature_table_timestamp_filter = (
        col(feature_event_timestamp_column).between(
            entity_min_timestamp - timedelta(seconds=feature_view.ttl.total_seconds()),
            entity_max_timestamp,
        )
        if feature_view.ttl
        else col(feature_event_timestamp_column) <= entity_max_timestamp
    )
    time_range_filtered_df = feature_table_df.filter(feature_table_timestamp_filter)

    return time_range_filtered_df


def join_entity_to_feature_tables(
    entity_df: SparkDataFrame,
    entity_event_timestamp_column: str,
    feature_table_dfs: List[SparkDataFrame],
    feature_tables: List[FeatureView],
) -> DataFrame:
    """Perform as of join between entity and multiple feature table.
    Args:
        entity_df (DataFrame): Spark dataframe representing the entities, to be joined with
            the feature tables.
        entity_event_timestamp_column (str): Column name in entity_df which represents
            event timestamp.
        feature_table_dfs (List[Dataframe]): List of Spark dataframes representing the feature tables.
        feature_tables (List[FeatureTable]): List of feature table specification. The length and ordering
            of this argument must follow that of feature_table_dfs.
    Returns:
        DataFrame: Join result, which contains all the original columns from entity_df, as well
            as all the features specified in feature_tables, where the feature columns will
            be prefixed with feature table name.
    Example:
        >>> entity_df.show()
            +------+-------------------+
            |entity|    event_timestamp|
            +------+-------------------+
            |  1001|2020-09-02 00:00:00|
            +------+-------------------+
        >>> table1_df.show()
            +------+--------+-------------------+-------------------+
            |entity|feature1|    event_timestamp|  created_timestamp|
            +------+--------+-------------------+-------------------+
            |    10|     200|2020-09-01 00:00:00|2020-09-01 00:00:00|
            +------+--------+-------------------+-------------------
        >>> table1 = FeatureTable(
                name="table1",
                features=[Field("feature1", "int32")],
                entities=[Field("entity", "int32")],
            )
        >>> table2_df.show()
            +------+--------+-------------------+-------------------+
            |entity|feature2|    event_timestamp|  created_timestamp|
            +------+--------+-------------------+-------------------+
            |    10|     400|2020-09-01 00:00:00|2020-09-01 00:00:00|
            +------+--------+-------------------+-------------------
        >>> table2 = FeatureTable(
                name="table2",
                features=[Field("feature2", "int32")],
                entities=[Field("entity", "int32")],
            )
        >>> tables = [table1, table2]
        >>> joined_df = join_entity_to_feature_tables(
                entity,
                tables,
            )
        >>> joined_df = join_entity_to_feature_tables(entity_df, "event_timestamp",
                [table1_df, table2_df], [table1, table2])
        >>> joined_df.show()
            +------+-------------------+----------------+----------------+
            |entity|    event_timestamp|table1__feature1|table2__feature2|
            +------+-------------------+----------------+----------------+
            |  1001|2020-09-02 00:00:00|             200|             400|
            +------+-------------------+----------------+----------------+
    """
    joined_df = entity_df

    for (feature_table_df, feature_table,) in zip(feature_table_dfs, feature_tables):
        joined_df = as_of_join(
            joined_df, entity_event_timestamp_column, feature_table_df, feature_table,
        )
    return joined_df


class FileRetrievalJob(RetrievalJob):
    def __init__(self, evaluation_function: Callable):
        """Initialize a lazy historical retrieval job"""

        # The evaluation function executes a stored procedure to compute a historical retrieval.
        self.evaluation_function = evaluation_function

    def to_df(self):
        # Only execute the evaluation function to build the final historical retrieval dataframe at the last moment.
        df = self.evaluation_function()
        return df

    def to_arrow(self):
        # Only execute the evaluation function to build the final historical retrieval dataframe at the last moment.
        df = self.evaluation_function().toPandas()
        return pyarrow.Table.from_pandas(df)


class MyCustomProvider(LocalProvider):
    def __init__(self, config: RepoConfig, repo_path):
        super().__init__(config)
        # Add your custom init code here. This code runs on every feast operation.

    def update_infra(
        self,
        project: str,
        tables_to_delete: Sequence[Union[FeatureTable, FeatureView]],
        tables_to_keep: Sequence[Union[FeatureTable, FeatureView]],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        # The update_infra method will be run during "feast apply" and is used to set up databases or launch
        # long-running jobs on a per table/view basis. This method should also clean up infrastructure that is unused
        # when feature views or tables are deleted. Examples of operations that update_infra typically fulfills
        # * Creating, updating, or removing database schemas for tables in an online store
        # * Launching a streaming ingestion job that writes features into an online store

        # Replace the code below in order to define your own custom infrastructure update operations
        super().update_infra(
            project,
            tables_to_delete,
            tables_to_keep,
            entities_to_delete,
            entities_to_keep,
            partial,
        )
        print("Launching custom streaming jobs is pretty easy...")

    def teardown_infra(
        self,
        project: str,
        tables: Sequence[Union[FeatureTable, FeatureView]],
        entities: Sequence[Entity],
    ):
        # teardown_infra should remove all deployed infrastructure

        # Replace the code below in order to define your own custom teardown operations
        super().teardown_infra(project, tables, entities)

    def online_write_batch(
        self,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        # online_write_batch writes feature values to the online store
        super().online_write_batch(config, table, data, progress)

    def materialize_single_feature_view(
        self,
        config: RepoConfig,
        feature_view: FeatureView,
        start_date: datetime,
        end_date: datetime,
        registry: Registry,
        project: str,
        tqdm_builder: Callable[[int], tqdm],
    ) -> None:
        # materialize_single_feature_view loads the latest feature values for a specific feature value from the offline
        # store into the online store.
        # This method can be overridden to also launch custom batch ingestion jobs that loads the latest batch feature
        # values into the online store.

        # Replace the line below with your custom logic in order to launch your own batch ingestion job
        super().materialize_single_feature_view(
            config, feature_view, start_date, end_date, registry, project, tqdm_builder
        )
        print("Launching custom batch jobs is pretty easy...")

    def get_historical_features(
        self,
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool,
    ) -> RetrievalJob:
        # get_historical_features returns a training dataframe from the offline store
        # see reference here for spark setup
        # https://github.com/feast-dev/feast-spark/blob/master/python/feast_spark/pyspark/historical_feature_retrieval_job.py

        spark = SparkSession.builder.getOrCreate()
        if not (
            isinstance(entity_df, pd.DataFrame)
            or isinstance(entity_df, ks.DataFrame)
            or isinstance(entity_df, SparkDataFrame)
        ):
            raise ValueError(
                f"Please provide an entity_df of type {type(pd.DataFrame)} instead of type {type(entity_df)}"
            )

        # force it to be a SparkDataFrame
        if isinstance(entity_df, pd.DataFrame):
            entity_df = ks.from_pandas(entity_df)
        if isinstance(entity_df, ks.DataFrame):
            entity_df = entity_df.to_spark()

        entity_df_event_timestamp_col = DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL  # local modifiable copy of global variable
        if entity_df_event_timestamp_col not in entity_df.columns:
            datetime_columns = (
                ks.DataFrame(entity_df)
                .select_dtypes(include=["datetime", "datetimetz", "timestamp"])
                .columns
            )
            if len(datetime_columns) == 1:
                print(
                    f"Using {datetime_columns[0]} as the event timestamp. To specify a column explicitly, please name it {DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL}."
                )
                entity_df_event_timestamp_col = datetime_columns[0]
            else:
                raise ValueError(
                    f"Please provide an entity_df with a column named {DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL} representing the time of events."
                )

        feature_views_to_features = _get_requested_feature_views_to_features_dict(
            feature_refs, feature_views
        )

        # Create lazy function that is only called from the RetrievalJob object
        def evaluate_historical_retrieval():
            # Create a copy of entity_df to prevent modifying the original
            entity_df_with_features = entity_df

            # Load feature view data from sources and join them incrementally
            for feature_view, features in feature_views_to_features.items():
                event_timestamp_column = feature_view.input.event_timestamp_column
                created_timestamp_column = feature_view.input.created_timestamp_column

                # Read offline parquet data in pyarrow format
                df_to_join = _filter_feature_table_by_time_range(
                    spark.read.parquet(feature_view.input.path),
                    feature_view,
                    entity_df,
                    entity_df_event_timestamp_col,
                )

                # Rename columns by the field mapping dictionary if it exists
                if feature_view.input.field_mapping is not None:
                    not NotImplementedError
                    # table = _run_field_mapping(table, feature_view.input.field_mapping)

                # Build a list of all the features we should select from this source
                feature_names = []
                feature_mapping = {}
                for feature in features:
                    # Modify the separator for feature refs in column names to double underscore. We are using
                    # double underscore as separator for consistency with other databases like BigQuery,
                    # where there are very few characters available for use as separators
                    prefixed_feature_name = f"{feature_view.name}__{feature}"

                    # Add the feature name to the list of columns
                    feature_names.append(prefixed_feature_name)
                    feature_mapping[prefixed_feature_name] = feature

                # Build a list of entity columns to join on (from the right table)
                join_keys = []
                for entity_name in feature_view.entities:
                    entity = registry.get_entity(entity_name, project)
                    join_keys.append(entity.join_key)
                right_entity_columns = join_keys
                right_entity_key_columns = [
                    event_timestamp_column
                ] + right_entity_columns

                # Remove all duplicate entity keys (using created timestamp)
                if created_timestamp_column:
                    # join_as_of dedups so do nothing...
                    right_entity_key_columns.append(created_timestamp_column)

                # Select only the columns we need to join from the feature dataframe
                df_to_join = df_to_join.select(right_entity_key_columns + features)

                entity_df_with_features = as_of_join(
                    entity_df_with_features,
                    entity_df_event_timestamp_col,
                    df_to_join,
                    feature_view,
                    feature_names,
                )

                # Remove right (feature table/view) event_timestamp column.
                if event_timestamp_column != entity_df_event_timestamp_col:
                    entity_df_with_features = entity_df_with_features.drop(
                        event_timestamp_column
                    )

                # Ensure that we delete dataframes to free up memory
                # del df_to_join

            # Move "datetime" column to front
            current_cols = entity_df_with_features.columns
            current_cols.remove(entity_df_event_timestamp_col)
            entity_df_with_features = entity_df_with_features.select(
                [entity_df_event_timestamp_col] + current_cols
            )

            return entity_df_with_features

        job = FileRetrievalJob(evaluation_function=evaluate_historical_retrieval)
        return job

        # return super().get_historical_features(
        #     config,
        #     feature_views,
        #     feature_refs,
        #     entity_df,
        #     registry,
        #     project,
        #     full_feature_names,
        # )

    def online_read(
        self,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        entity_keys: List[EntityKeyProto],
        requested_features: List[str] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        # get_historical_features returns a training dataframe from the offline store
        return super().online_read(config, table, entity_keys, requested_features)
