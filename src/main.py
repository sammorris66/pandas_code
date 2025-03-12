from assets.builder import DataFrameBuilder

import pandas as pd

import json

def read_dict_from_json(file_path: str) -> dict:
    """Reads a dictionary from a JSON file and returns it as a Python dict."""
    with open(file_path, 'r', encoding='utf-8') as file:
        return json.load(file)
    


def main() -> None:
    my_dict: dict = read_dict_from_json("assets.json")

    builder: DataFrameBuilder = DataFrameBuilder(my_dict)

    df: pd.DataFrame = (
        builder
        .extract_objects_attributes()
        .extract_attributes_types()
        .select_attribute_type_columns()
        .select_attribute_columns()
        .pivot_attributes()
        .rename_headers()
        .map_attribute_types_to_headers()
        .build()
    )

    # Show all columns
    pd.set_option('display.max_columns', None)

    # Show all rows
    pd.set_option('display.max_rows', None)

    # # Set column width to see full text values
    pd.set_option('display.max_colwidth', None)

    # # Prevent line wrapping (optional)
    pd.set_option('display.width', 1000)

    print(df)

main()