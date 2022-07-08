"""Script for suppliers data preprocessing."""

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession

from product_integrator.pipeline.data import (
    get_supplier_data_schema,
    load_supplier_data,
)
from product_integrator.pipeline.transformations import (
    create_dataframe_with_attributes_as_columns,
    preprocess_dataframe_after_concatenation,
)


def preprocess_supplier_data(
    spark: SparkSession, output_path: str, save: bool
) -> DataFrame:
    """Preprocess step of the input suppliers data.

    Args:
        spark: Spark session.
        output_path: CSV output path.
        save: bool to save or not save the output csv.

    Returns:
        Preprocessed Dataframe.
    """
    # Get supplier data schema
    schema = get_supplier_data_schema()

    # Load data from supplier json file
    suppliers_df = load_supplier_data(spark, schema)

    # Convert dataframe to add each AttributeName as a new column
    suppliers_df = create_dataframe_with_attributes_as_columns(spark, suppliers_df)

    # Remove duplicates and output a dataframe with a row for each product
    suppliers_df = preprocess_dataframe_after_concatenation(suppliers_df)

    # Save dataframe as CSV
    if save:
        suppliers_df.toPandas().to_csv(output_path + "/preprocessing.csv")

    return suppliers_df
