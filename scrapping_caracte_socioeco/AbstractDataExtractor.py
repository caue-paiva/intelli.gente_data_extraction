from enum import Enum
from typing import Any

class DataPointTypes(Enum):
   INT = int
   FLOAT = float
   STRING = str
   BOOL = bool


class DataPoint():
   stardard_name: str
   column_name:str
   data_type: DataPointTypes
   multiply_amount: int 

   def __init__(self, data_dict:dict) -> None:
      self.stardard_name:str = data_dict["standard_name"] 
      self.column_name:str = data_dict["column_name"]
      self.data_type: DataPointTypes = data_dict["data_type"]
      multiply_amount: int | float | None = data_dict.get("multiply_amount") 
     
      if multiply_amount is None:
         self.multiply_amount = 1
      else:
         if self.data_type not in [DataPointTypes.INT, DataPointTypes.FLOAT]:
            raise IOError("Valor de multiplicação não é valido para tipos diferentes de inteiros ou float")
         self.multiply_amount = multiply_amount

   def multiply_value(self, value:Any)-> Any:
      if self.data_type not in [DataPointTypes.INT, DataPointTypes.FLOAT]:
         return value
      else:
         return value * self.multiply_amount
      
class DataFrameInfo():
   year_column_name: str
   city_column_name:str
   data_point_list: list[DataPoint]

   def __init__(self, year_column_name:str, city_column_name:str, data_point_list: list[DataPoint]) -> None:
      self.year_column_name = year_column_name
      self.city_column_name = city_column_name
      
      if not data_point_list:
         raise IOError("Lista de Dados está vazia")
      else:
         self.data_point_list = data_point_list.copy()