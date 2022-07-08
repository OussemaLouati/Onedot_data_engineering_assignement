"""Spark User defined functions (UDFs)."""

from thefuzz import fuzz
from deep_translator import GoogleTranslator
from product_integrator.pipeline.utils import get_city_country_mappings
from product_integrator.constants import CONDITIONS


colors_cache = {}
cities_details = get_city_country_mappings()


def get_country_code(city_name: str) -> str:
    """Get country code from city name.


    Args:
        cities_details: dictionary of city names mapped to their corresponding countries.
        city_name: city name.

    Returns:
        Country code.
    """

    # If city_name not found, we use fuzz package which uses Levenshtein Distance to calculate the differences between sequence
    # This used in case a city name is there but with different but close spelling
    # This proved useful for: "St. Gallen" which is written "sankt gallen" in Cities dataset

    if city_name not in cities_details.keys():
        # we set a threshold of 80, meaning a city name is considered as the same city name if it matched more than 80%
        # the word with highest score (closest) and > 80 is taken
        threshold = 80
        for city in cities_details.keys():
            current_score = fuzz.token_sort_ratio(city_name, city)
            if current_score >= threshold:
                threshold = current_score
                city_name = city

    corresponding_country = cities_details[city_name]
    return corresponding_country


def translate_german_color_to_english(color: str) -> str:
    """Translate colors from german to english.


    Args:
        color: Color in german.

    Returns:
        Color in english.
    """
    global colors_cache
    color = color.replace(" mét.", "")
    if color in colors_cache:
        return colors_cache[color]
    translated_color = GoogleTranslator(source="de", target="en").translate(color)
    colors_cache[color] = translated_color
    return translated_color


def map_conditions(condition: str) -> str:
    """Translate condition from german to english.


    Args:
        condition: Condition in german.

    Returns:
        Condition in english.
    """
    return CONDITIONS[condition]
