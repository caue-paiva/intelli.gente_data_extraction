from typing import Any
from DataEnums import DataTypes


class DataColumn(): #clase para representar uma coluna de uma tabela que representa um dado específico que queremos extrair
   data_category:str
   data_name: str
   column_name:str
   data_type: DataTypes
   multiply_amount: int  #algumas vezes os dados vão vir em escala de 1 equivale a 1000 ou similares, então é necessário
   #guardar por quanto esse valor deve ser multiplicado

   def __init__(
      self,
      data_category:str,
      data_name:str,
      column_name:str,
      data_type:DataTypes, 
      multiply_amount: int | None = None
   )-> None:
      
      self.data_category = data_category
      self.data_name:str =  data_name
      self.column_name:str = column_name
      self.data_type: DataTypes = data_type
      multiply_amount: int | float | None = multiply_amount
     
      if multiply_amount is None:
         self.multiply_amount = 1
      else:
         if self.data_type not in [DataTypes.INT, DataTypes.FLOAT]:
            raise IOError("Valor de multiplicação não é valido para tipos que não sejam inteiros ou float")
         self.multiply_amount = multiply_amount

   def multiply_value(self, value:Any)-> int | float:
      if self.data_type not in [DataTypes.INT, DataTypes.FLOAT]:
         return value
      else:
         return value * self.multiply_amount
      
class TableDataPoints(): #classe para agregar todos os pontos de dados extraido de uma tabela de dados
   
   year_column_name: str #nome da coluna que tem a info do ano
   city_code_column:str #nome da coluna que tem a info do código do município
   data_column_list: list[DataColumn] #lista de colunas na tabela com cada uma sendo atrelada a um ponto de dado

   def __init__(self, year_column_name:str, city_code_column:str) -> None:
      self.year_column_name = year_column_name
      self.city_code_column = city_code_column
      self.data_column_list = []

   def add_data_point(
      self,
      data_category:str,
      data_name:str,
      column_name:str,
      data_type:DataTypes,
      multiply_amount: int | None = None
   )->None:
      self.data_column_list.append(DataColumn(data_category,data_name,column_name,data_type,multiply_amount))

   def add_data_point_dict(self,data_dict:dict[str, str|int])->None:
      data_category:str = data_dict["data_category"]
      data_name:str = data_dict["data_name"]
      column_name:str = data_dict["column_name"]
      data_type:DataTypes = data_dict["data_type"]
      multiply_amount: int | None = data_dict.get("multiply_amount")

      self.data_column_list.append(DataColumn(data_category,data_name,column_name,data_type,multiply_amount))

   def add_data_points_dicts(self, dict_list: list[dict[str, str|int]])->None:
      for data_point in dict_list:
          self.add_data_point_dict(data_point)