"""Package Constants."""


# This data is dowloaded from: 
# https://public.opendatasoft.com/explore/dataset/geonames-all-cities-with-a-population-1000
CITIES_JSON_FILEPATH = "./data/input/external/geonames-all-cities-with-a-population-1000.json"  
SUPPLIER_DATA_FILEPATH = "./data/input/supplier_car.json"


SUPPLIER_TARGET_MAPPING = {
    "MakeText": "make",
    "City": "city",
    "BodyColorText": "color",
    "FirstRegYear": "manufacture_year",
    "FirstRegMonth": "manufacture_month",
    "ConsumptionTotalText": "fuel_consumption_unit",
    "Km": "mileage",
    "Hp": "price_on_request",
    "ModelText": "model",
    "TypeName": "model_variant",
}


CONDITIONS = {
    "Neu": "New",
    "Occasion": "Used",
    "Oldtimer": "Restored",
    "Vorführmodell": "Original Condition",
}
