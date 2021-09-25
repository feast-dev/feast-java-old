# This is an example feature definition file

from google.protobuf.duration_pb2 import Duration
import pandas as pd

from feast import Entity, Feature, FeatureView, FileSource, ValueType, FeatureService, OnDemandFeatureView

# Read data from parquet files. Parquet is convenient for local development mode. For
# production, you can use your favorite DWH, such as BigQuery. See Feast documentation
# for more info.
driver_hourly_stats = FileSource(
    path="/Users/achal/tecton/feast/prompt_dory/data/driver_stats.parquet",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created",
)

# Define an entity for the driver. You can think of entity as a primary key used to
# fetch features.
driver = Entity(name="driver_id", value_type=ValueType.INT64, description="driver id",)

# Our parquet files contain sample data that includes a driver_id column, timestamps and
# three feature column. Here we define a Feature View that will allow us to serve this
# data to our model online.
driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",
    entities=["driver_id"],
    ttl=Duration(seconds=86400 * 365),
    features=[
        Feature(name="conv_rate", dtype=ValueType.DOUBLE),
        Feature(name="acc_rate", dtype=ValueType.FLOAT),
        Feature(name="avg_daily_trips", dtype=ValueType.INT64),
    ],
    online=True,
    batch_source=driver_hourly_stats,
    tags={},
)

fs = FeatureService(
    name="driver_hourly_stats_feature_service",
    features=[driver_hourly_stats_view]
)


def conv_rate_plus_100(driver_hourly_stats: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["conv_rate_plus_100"] = driver_hourly_stats["conv_rate"] + 100
    return df


odfv = OnDemandFeatureView(
    name=conv_rate_plus_100.__name__,
    inputs={"driver": driver_hourly_stats_view},
    # features=[Feature("conv_rate_plus_100", ValueType.FLOAT)],
    features=[],
    udf=conv_rate_plus_100,
)