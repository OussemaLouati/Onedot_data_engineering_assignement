"""Script for generating final integration data."""

from pyspark.sql.dataframe import DataFrame

from product_integrator.pipeline.transformations import (
    rename_column,
    add_country_column,
    add_conditions_column,
)
from product_integrator.constants import SUPPLIER_TARGET_MAPPING


def integrate_supplier_data(
    extracted_suppliers_df: DataFrame, output_path: str, save: bool
) -> DataFrame:
    """Extraction of new values from the suppliers data.

    Args:
        normalised_suppliers_df: Normalised dataframe
        output_path: CSV output path.
        save: bool to save or not save the output csv.

    Returns:
        Extracted Dataframe.
    """

    # Rename and map columns:
    # "MakeText" <-> "make",
    #  "City"<-> "city",
    # "BodyColorText" <-> "color",
    # "FirstRegYear" <-> "manufacture_year",
    #  "FirstRegMonth"<-> "manufacture_month",
    #  "ConsumptionTotalText" <-> "fuel_consumption_unit",
    #  "Km"<-> "mileage",
    #  "Hp"<-> "price_on_request",
    # "ModelText" <-> "model",
    #  "TypeName"  <-> "model_variant",
    for column in SUPPLIER_TARGET_MAPPING.keys():
        extracted_suppliers_df = rename_column(extracted_suppliers_df, column)

    # Use and external dataset to map each city name to its corresponding country code
    extracted_suppliers_df = add_country_column(extracted_suppliers_df)

    # Drop attributes not mapped to the target schema
    extracted_suppliers_df = extracted_suppliers_df.drop(
        "ID",
        "TypeNameFull",
        "ModelTypeText",
        "InteriorColorText",
        "Co2EmissionText",
        "Ccm",
        "Properties",
        "TransmissionTypeText",
        "ConsumptionRatingText",
        "FuelTypeText",
    )

    # Integrate "condition" column
    extracted_suppliers_df = add_conditions_column(extracted_suppliers_df)

    # Save CSV
    if save:
        extracted_suppliers_df.toPandas().to_csv(output_path + "/integration.csv")

    return extracted_suppliers_df
