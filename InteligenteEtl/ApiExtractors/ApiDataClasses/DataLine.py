from enum import Enum
from typing import Any

class DataLineTypes(Enum):
   INT = "int"
   FLOAT = "float"
   STRING = "str"
   BOOL = "bool"
   UNKNOWN = "str"
   NULL = "NULL"

class DataLine:
   """
   Essa classe tem uma relação quase 1 <-> 1 com uma linha de uma tabela do BD de categorias com dados brutos (a única diferença seria o campo de multiply amount)
   mas ele é usado para calcular o campo value e não será colocado na tabela
   
   """
   city_id: int
   year: int
   data_name: str
   data_type: DataLineTypes
   value: Any
   multiply_amount:int|float


   def __init__(
      self, 
      city_id: int, 
      year: int, 
      data_name: str, 
      value: Any,
      data_type: DataLineTypes = DataLineTypes.STRING, 
      multiply_amount: int|float = 1,
   ) -> None:
      
      self.city_id = city_id
      self.year = year
      self.data_name = data_name
      self.data_type = data_type
      self.value = value
      self.multiply_amount = multiply_amount

      if self.data_type not in [DataLineTypes.INT, DataLineTypes.FLOAT] and multiply_amount != 1:
            raise IOError("Valor de multiplicação além de 1 (default) não é valido para tipos que não sejam inteiros ou float")
     
      self.transform_value()

   def infer_dtype_and_multiply_amnt(self, unit_description_str:str)->bool:
      """
      APIs como a do ibge tem um campo chamado "unidade", onde é explicado qual unidade o dado se refere, essa unidade pode seguir um padrão como
      "mil reais" e a partir desses padrões é possível inferir o tipo de dado e quanto precisa multiplicar o dado
      
      Transforma o valor do DataLine baseado no tipo e qntd de multiplicação inferida
      """
      sucess_flag:bool = True #vai ser retornado true se foi possível inferir tanto o tipo de dado quanto a qntd pra multiplicar da string

      lowercase_str: str = unit_description_str.lower()
      multiply_amnt_map: dict[str,int] = {
         "mil" : 1000,
         "cem" : 100
      }

      for key in multiply_amnt_map:
         if key in lowercase_str:
            self.multiply_amount = multiply_amnt_map[key]
            break
      else:
         self.multiply_amount = 1
         sucess_flag = False


      dtype_map: dict[str,DataLineTypes] = {
         "reais" : DataLineTypes.FLOAT,
         "real"  : DataLineTypes.FLOAT,
         "pessoas": DataLineTypes.INT,
         "unidades": DataLineTypes.INT
      }

      for key in dtype_map:
         if key in lowercase_str:
            self.data_type = dtype_map[key]
            sucess_flag = True
            break
      else:
         sucess_flag = False
         self.data_type = DataLineTypes.STRING
      
      self.transform_value()
      return sucess_flag
   
   def transform_value(self)->None:
      """
      Transforma o campo value de acordo com o tipo de dado e o valor de multiplicar
      """
      match (self.data_type):
       case DataLineTypes.STRING:
            self.value = str(self.value)
       case DataLineTypes.INT:
            self.value = int(self.value) * self.multiply_amount
       case DataLineTypes.FLOAT:
            self.value = float(self.value) * self.multiply_amount
       case DataLineTypes.BOOL:
            self.value = bool(self.value) 
       case _:
            self.value = str(self.value) #caso o tipo de dado seja desconhecido, ele será um string
       
   def __str__(self)->str:
      return f"""
      nome dado: {self.data_name}
      id_muni: {self.city_id}  
      valor: {self.value}  
      """
   
