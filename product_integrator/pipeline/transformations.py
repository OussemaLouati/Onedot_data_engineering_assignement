"""Transformation functions."""

from pyspark.sql import SparkSession, Row
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, initcap, udf, expr, split
from product_integrator.constants import SUPPLIER_TARGET_MAPPING
from product_integrator.pipeline.user_defined_fns import (
    get_country_code,
    translate_german_color_to_english,
    map_conditions,
)
from product_integrator.pipeline.utils import (
    diff_between_two_lists,
    create_list_from_collected_object,
)


def transform_maketext_make(supplier_car_df: DataFrame) -> DataFrame:
    """Normalise "MakeText" column to match target "make" column.

    For each "value" of "MakeText", we need to make it Title case for each of its words even if the
    are separated by a certain delimeter.
    Source: 
    https://stackoverflow.com/questions/66263194/how-can-i-capitalize-each-word-delimited-by-some-characters-in-pyspark

    Args:
        supplier_car_df: suppliers data dataframe.

    Returns:
        New normalised Dataframe.
    """
    supplier_car_df = supplier_car_df.withColumn(
        "MakeText",
        expr(
            r"""
        array_join(
            transform(
                split(regexp_replace(MakeText, '(\\s|\\(|\\+|-|\\/)(.)', '$1#$2'), '#'),
                x -> initcap(x)
            ),
            ""
        )
    """
        ),
    )
    return supplier_car_df


def rename_column(supplier_car_df: DataFrame, column: str) -> DataFrame:
    """Rename column of a datframe.


    Args:
        supplier_car_df:  Dataframe .
        column: column to rename.

    Returns:
        New Dataframe with renamed column.
    """
    new_name = SUPPLIER_TARGET_MAPPING[column]
    supplier_car_df = supplier_car_df.withColumnRenamed(column, new_name)
    return supplier_car_df


def add_country_column(city_df: DataFrame) -> DataFrame:
    """Add Country Column.


    Args:
        city_df: A Dataframe that contains a column with cities names.

    Returns:
        New Dataframe with added country column.
    """
    get_country_code_UDF = udf(get_country_code)
    city_df = city_df.select("*", get_country_code_UDF(col("city")).alias("country"))
    return city_df


def add_conditions_column(df: DataFrame) -> DataFrame:
    """Add conditions Column.


    Args:
        df: A Dataframe that contains a column "ConditionTypeText".

    Returns:
        New Dataframe with added conditions column.
    """
    map_conditions_UDF = udf(map_conditions)
    df = df.select(
        "*", map_conditions_UDF(col("ConditionTypeText")).alias("condition")
    ).drop("ConditionTypeText")
    return df


def add_color_column(colors_df: DataFrame) -> DataFrame:
    """Add Country Column.


    Args:
        colors_df: A Dataframe that contains a column with colors written in german.

    Returns:
        New Dataframe with added color column.
    """
    translate_german_color_to_english_UDF = udf(translate_german_color_to_english)
    colors_df = colors_df.select(
        "*",
        initcap(translate_german_color_to_english_UDF(col("BodyColorText"))).alias(
            "Color"
        ),
    ).drop("BodyColorText")
    return colors_df


def extract_new_attributes(fuel_consumption_unith_df: DataFrame) -> DataFrame:
    """Extract new attributes from supplier data.

    Value of supplier attribute “ConsumptionTotalText” into an attribute 
      called: “ex-tracted-value-ConsumptionTotalText”
    Unit supplier attribute “ConsumptionTotalText” into an attribute 
      called: “ex-tracted-unit-ConsumptionTotalText”

    Args:
        fuel_consumption_unith_df: A Dataframe that contains a column with "ConsumptionTotalText".

    Returns:
        New Dataframe with two added columns “ex-tracted-value-ConsumptionTotalText” 
                                         and “ex-tracted-unit-ConsumptionTotalText”.
    """
    fuel_consumption_unith_df = fuel_consumption_unith_df.select(
        "*",
        split(col("ConsumptionTotalText"), " ")
        .getItem(0)
        .alias("ex-tracted-value-ConsumptionTotalText"),
        split(col("ConsumptionTotalText"), " ")
        .getItem(1)
        .alias("ex-tracted-unit-ConsumptionTotalText"),
    ).drop("ConsumptionTotalText")
    return fuel_consumption_unith_df


def normalise_fuel_consumption_unit_column(
    fuel_consumption_unith_df: DataFrame,
) -> DataFrame:
    """Add "fuel_consumption_unit" Column.


    Args:
        fuel_consumption_unith_df: A Dataframe that contains 
                                  a column with "ConsumptionTotalText".

    Returns:
        New Dataframe with normalised fuel_consumption_unith column.
    """

    df = fuel_consumption_unith_df.rdd.map(
        lambda x: (
            x.ID,
            "l_km_consumption" if "l/100km" in x["ConsumptionTotalText"] else "null",
        )
    ).toDF(["ID", "fuel_consumption_unit"])
    fuel_consumption_unith_df = fuel_consumption_unith_df.join(df, ["ID"])
    fuel_consumption_unith_df.drop("ConsumptionTotalText")
    return fuel_consumption_unith_df


def normalise_price_on_request_column(supplier_car_df: DataFrame) -> DataFrame:
    """Add "price_on_request" Column.


    Args:
        supplier_car_df: A Dataframe that contains a column with "Hp".

    Returns:
        New Dataframe with normalised price_on_request column.
    """

    df = supplier_car_df.rdd.map(
        lambda x: (x.ID, "true" if int(x["Hp"]) > 0 else "false")
    ).toDF(["ID", "price_on_request"])

    supplier_car_df = supplier_car_df.drop("price_on_request")
    supplier_car_df = supplier_car_df.join(df, ["ID"])
    supplier_car_df = supplier_car_df.drop("Hp")
    return supplier_car_df


def get_dataframe_with_one_attribute(
    spark: SparkSession, supplier_car_df: DataFrame, attribute: str
) -> DataFrame:
    """Get new dataframe with atwo colums: attribute and ID.

    for example if supplier_car_df =
    +------+-------------+--------------------+------+---------------+----------------+
    |ID    |MakeText     |model_variant       |model |Attribute Names|Attribute Values|
    +------+-------------+--------------------+------+---------------+----------------+
    |976.0 |MERCEDES-BENZ|McLaren             |SLR   |Seats          |2               |
    |1059.0|MERCEDES-BENZ|ML 350 Inspiration  |ML 350|Hp             |235             |
    |524.0 |AUDI         |S6 Avant quattro 4.2|S6    |FuelTypeText   |Benzin          |
    |608.0 |SAAB         |9-3 2.0i-16 TS Aero |9-3   |Ccm            |1985            |
    |726.0 |PORSCHE      |911 Turbo Cabrio    |911   |BodyColorText  |schwarz mét.    |

    if attribute is "FuelTypeText" then new_df =
    +------+------------+
    |    ID|FuelTypeText|
    +------+------------+
    | 524.0|      Benzin|
    | 197.0|      Benzin|
    |  34.0|      Benzin|
    | 892.0|      Benzin|
    | 800.0|      Benzin|
    | 171.0|      Benzin|

    Args:
        spark: spark session
        supplier_car_df: suppliers data dataframe.
        attribute: attribute name

    Returns:
        New  Dataframe.
    """
    new_df = (
        supplier_car_df.where(col("Attribute Names") == attribute)
        .withColumnRenamed("Attribute Values", attribute)
        .drop("Attribute Names")
    )
    new_df = new_df.select(["ID", attribute])

    # Test if we have missing row in the data, json has 21906 lines, each product has 19 attributes
    # Dataset should have 1 row for each product resulting in 1153 rows in total
    if new_df.count() < 1153:
        less = new_df.select("ID").collect()
        complete = supplier_car_df.select("ID").distinct().collect()

        list1 = create_list_from_collected_object(less)
        list2 = create_list_from_collected_object(complete)

        # We check the missing attribute belongs to what product, and add it as null 
        # so later we don't lose records when concatenating dataframes
        diff = diff_between_two_lists(list1, list2)
        rows = []
        for e in range(len(diff)):
            newRow = Row(diff[e], "Null")
            rows.append(newRow)

        dff = spark.createDataFrame(rows, ["ID", attribute])
        new_df = new_df.union(dff)
    return new_df


def create_dataframe_with_attributes_as_columns(
    spark: SparkSession,
    supplier_car_df: DataFrame,
) -> DataFrame:
    """Transform input dataframe to add new column for each attribute name.


    Args:
        spark: spark session
        supplier_car_df: Dataframe .

    Returns:
        New Dataframe with 19 new columns.
    """
    attributes = supplier_car_df.select("Attribute Names").distinct().collect()
    list_dfs = []

    for i in range(len(attributes)):
        new_df = get_dataframe_with_one_attribute(
            spark, supplier_car_df, attributes[i][0]
        )
        list_dfs.append(new_df)

    for i in range(0, len(list_dfs)):
        supplier_car_df = supplier_car_df.join(list_dfs[i], ["ID"])

    return supplier_car_df


def preprocess_dataframe_after_concatenation(
    supplier_car_df: DataFrame,
) -> DataFrame:
    """Clean dataframe by removing duplicates.


    Args:
        supplier_car_df: Dataframe .

    Returns:
        New Dataframe.
    """
    supplier_car_df = supplier_car_df.drop(
        "Attribute Names", "Attribute Values", "entity_id"
    )
    supplier_car_df = supplier_car_df.dropDuplicates()
    return supplier_car_df
