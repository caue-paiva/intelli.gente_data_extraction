import pandas as pd
from dataclasses import dataclass
from etl_config import get_config
from citiesinfo import get_city_codes
from datastructures import DataTypes

"""
Classe que representa o dado que vai ser carregado no BD/Data Warehouse, com o DF sendo os dados em si e com os outros
campos da classe sendo metadadas como a categoria, nome do dado e os anos da série histórica.
"""


#dataclass cria automaticamente métodos __init__, __eq__ entre outros, além de que mostra que o propósito principal da classe é guardar dados
@dataclass
class ProcessedDataCollection:
   """
   Essa classe representa um conjunto de um dado específico pronta para ser inserida no banco de dados

   Ela contém a categoria (tabela) dos dados, o nome dos dados, a lista de anos da série histórica e os
   dados na forma de DF do pandas
   
   """
   category:str
   dtype: DataTypes
   data_name:str
   time_series_years:list[int]
   df: pd.DataFrame

   def check_df_validity(self)->bool:
     """
     TODO
     Checa se o DF contido no objeto é valido ou não
     """
   
   def __str__(self) -> str:
      return   f"""
            Nome do dado: {self.data_name},\n
            Tópico do dado: {self.category},\n
            Anos da série histórica: {self.time_series_years},\n
            Tipo de dado: {self.dtype.value} \n
            """
       
