"""Script for extracting new features from suppliers data."""

from pyspark.sql.dataframe import DataFrame

from product_integrator.pipeline.transformations import extract_new_attributes


def extraction_from_normalised_supplier_data(
    normalised_suppliers_df: DataFrame, output_path: str, save: bool
) -> DataFrame:
    """Extraction of new values from the suppliers data.

    Args:
        normalised_suppliers_df: Normalised dataframe
        output_path: CSV output path.
        save: bool to save or not save the output csv.

    Returns:
        Extracted Dataframe.
    """

    # Extract two new attributes:
    # The value from the supplier attribute “ConsumptionTotalText” into an attribute called: 
    # “ex-tracted-value-ConsumptionTotalText”
    # The unit  the supplier attribute “ConsumptionTotalText” into an attribute called:
    #  “ex-tracted-unit-ConsumptionTotalText”
    normalised_suppliers_df = extract_new_attributes(normalised_suppliers_df)

    # save csv
    if save:
        normalised_suppliers_df.toPandas().to_csv(output_path + "/extraction.csv")

    return normalised_suppliers_df
