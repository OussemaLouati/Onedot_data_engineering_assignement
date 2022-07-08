"""Script for suppliers data normalisation."""

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession

from product_integrator.pipeline.transformations import (
    transform_maketext_make,
    add_color_column,
    normalise_fuel_consumption_unit_column,
    normalise_price_on_request_column,
)


def normalise_preprocessed_supplier_data(
    preprocessed_suppliers_df: DataFrame, output_path: str, save: bool
) -> DataFrame:
    """Normalisation step of the input suppliers data.

    Args:
        preprocessed_suppliers_df: Preprocessed dataframe
        output_path: CSV output path.
        save: bool to save or not save the output csv.

    Returns:
        Normalised Dataframe.
    """

    # Transform "MakeText" column : rename and convert its values to Title case
    preprocessed_suppliers_df = transform_maketext_make(preprocessed_suppliers_df)

    # Use Google translate api to translate colors from german to english
    preprocessed_suppliers_df = add_color_column(preprocessed_suppliers_df)

    # Normalise "fuel_consumption_unit" to match target dataset
    #  'l_km_consumption'  if 'l/100km' in 'ConsumptionTotalText' else 'null'
    preprocessed_suppliers_df = normalise_fuel_consumption_unit_column(
        preprocessed_suppliers_df
    )

    # Normalise "price_on_request" to match target dataset
    #  'true'  if 'Hp' > 0 else false
    preprocessed_suppliers_df = normalise_price_on_request_column(
        preprocessed_suppliers_df
    )

    # Save as CSV
    if save:
        preprocessed_suppliers_df.toPandas().to_csv(output_path + "/normalisation.csv")

    return preprocessed_suppliers_df
