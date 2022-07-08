"""Utility and helper functions package."""

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from product_integrator.constants import CITIES_JSON_FILEPATH
from typing import List

import json


def init_spark_session(app_name: str = "DataEngineerAssignement") -> SparkSession:
    """Get new spark Session.

    Args:
        app_name: Spark app name.

    Returns:
        New Spark Session.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()


def get_city_country_mappings() -> dict():
    """Get a dictionary of cities and their corresponding countries.

    Returns:
        Dictionary of City Name -> Country Code.
    """
    with open(CITIES_JSON_FILEPATH, "r") as data_file:
        json_data = data_file.read()

    data = json.loads(json_data)
    cities_details = {}

    for d in data:
        cities_details[d["fields"]["name"]] = d["fields"]["country_code"]

    return cities_details


def create_list_from_collected_object(li: List[str]) -> List[str]:
    """Get list from dataframe.collect() obj.

    Returns:
        List of values.
    """
    lst = []
    for i in range(len(li)):
        lst.append(li[i][0])
    return lst


def diff_between_two_lists(li1: List[str], li2: List[str]) -> List[str]:
    """Return the difference between two lists.

    Returns:
        List of values.
    """
    return list(set(li1) - set(li2)) + list(set(li2) - set(li1))


def save_df_as_csv(df: DataFrame, file_path: str) -> None:
    """Save a dataframe as a csv.

    Args:
        df: dataframe
        file_path: path to save csv
    Returns:
        List of values.
    """

    ## This won't work with Java SDK 17, it only works with java 8, 11
    ## That's why we are using df.toPandas().to_csv()
    (df.write.option("header", True).option("encoding", "UTF-8 ").csv(file_path))
