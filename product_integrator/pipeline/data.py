"""Data loading and preparation."""

from pyspark.sql import SparkSession
from product_integrator.constants import SUPPLIER_DATA_FILEPATH
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, StringType


def load_supplier_data(spark: SparkSession, schema: StructType) -> DataFrame:
    """Load suppliers data into a dataframe.

    Args:
        spark: Spark session.
        schema: schema.

    Returns:
        New Dataframe.
    """
    supplier_car_df = spark.read.json(
        SUPPLIER_DATA_FILEPATH,
        schema,
    )
    return supplier_car_df


def get_supplier_data_schema() -> StructType:
    """Get supplier data schema.

    Returns:
        New schema of type StructType.
    """
    schema = StructType(
        [
            StructField("ID", StringType(), True),
            StructField("MakeText", StringType(), True),
            StructField("TypeName", StringType(), True),
            StructField("TypeNameFull", StringType(), True),
            StructField("ModelText", StringType(), True),
            StructField("ModelTypeText", StringType(), True),
            StructField("Attribute Names", StringType(), True),
            StructField("Attribute Values", StringType(), True),
            StructField("entity_id", StringType(), True),
        ]
    )
    return schema
