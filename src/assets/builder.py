import pandas as pd

from typing import Any, Optional, Self

class DataFrameBuilder:

    def __init__(self: Self, data: dict[Any, Any]):

        self.data: dict = data["values"]
        self.attribute_types: dict = data["objectTypeAttributes"]
        self.df_attributes: Optional[pd.DataFrame] = None
        self.df_attribute_types: Optional[pd.DataFrame] = None

    
    def extract_objects_attributes(self: Self) -> "DataFrameBuilder":

        self.df_attributes = pd.json_normalize(
            self.data,
            record_path=["attributes", "objectAttributeValues"],
            meta=["label", "id", ["attributes", "objectTypeAttributeId"]],
            sep=".",
            errors="ignore",
            )

        return self
    
    def extract_attributes_types(self: Self) -> "DataFrameBuilder":

        self.df_attribute_types = pd.json_normalize(
            self.attribute_types,
            meta=["id", ["objectType", "id"], ["objectType", "name"]],
            sep=".",
            errors="ignore",
        )
    

        return self
    
    def select_attribute_type_columns(self: Self) -> "DataFrameBuilder":

        columns_to_keep: list[str] = [
            "id",
            "objectType.id",
        ]

        self.df_attribute_types = self.df_attribute_types.reindex(columns=columns_to_keep, fill_value=None)

        return self
    
    
    def select_attribute_columns(self: Self) -> "DataFrameBuilder":

        columns_to_keep: list[str] = [
            "label",
            "id",
            "displayValue",
            "referencedType",
            "attributes.objectTypeAttributeId",
            "referencedObject.objectType.name",
            "referencedObject.objectType.id",
        ]

        self.df_attributes = self.df_attributes.reindex(columns=columns_to_keep, fill_value=None)

        return self

    def pivot_attributes(self: Self) -> "DataFrameBuilder":

        if self.df_attributes is not None:
            self.df_attributes = self.df_attributes.pivot(
                 index=["label", "id"],
                 columns=["attributes.objectTypeAttributeId"],
                 values=["displayValue", "referencedObject.objectType.name", "referencedObject.objectType.id"]
            )

            self.df_attributes.columns = [self._format_column_name(col) for col in self.df_attributes.columns]

        return self
    
    def _format_column_name(self: Self, col: tuple) -> str:

        base_name, suffix = col
        return f"{base_name}_{suffix}" if suffix else str(base_name)

    
    def rename_headers(self: Self) -> "DataFrameBuilder":
    
        rename_map: dict[str, str] = {
            "referencedObject.objectType": "RefObjType",
            "displayValue": "attrib_val",
        }

        if self.df_attributes is not None:
            self.df_attributes.columns = [
                self._replace_column_name(col, rename_map) for col in self.df_attributes.columns
            ]
    
        return self

    def _replace_column_name(self: Self, col: str, rename_map: dict) -> str:
   
        for old, new in rename_map.items():
            col = col.replace(old, new)
        return col
    
    def map_attribute_types_to_headers(self: Self) -> "DataFrameBuilder":
    
        if self.df_attribute_types is None or self.df_attributes is None:
            return self
        
        attribute_mapping = self.df_attribute_types.set_index("id")["objectType.id"].to_dict()


        self.df_attributes.columns = [
            self._replace_column_name(col, attribute_mapping) for col in self.df_attributes.columns
        ]

        return self


    def build(self: Self):
        return self.df_attributes
