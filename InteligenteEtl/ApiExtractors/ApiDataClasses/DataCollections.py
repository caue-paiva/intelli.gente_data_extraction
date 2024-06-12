from .DataLine import DataLine
from pandas import DataFrame
from dataclasses import dataclass

#dataclass cria automaticamente métodos __init__, __eq__ entre outros, além de que mostra que o propósito principal da classe é guardar dados
@dataclass
class ProcessedDataCollection:
   """
   Essa classe contém todos os dados necessários para inserir os resultados da pipeline ETL no banco de dados

   Ela contém a categoria (tabela) dos dados, o nome dos dados, a lista de anos da série histórica e os
   dados na forma de DF do pandas
   
   """
   category:str
   data_name:str
   time_series_years:list[int]
   df: DataFrame

@dataclass
class RawDataCollection:
   """
   Classe que guarda um  dado específico não processado vindos da API num formato intermediário com os metadados sobre categoria,
   série histórica...

   tem um método para retornar um objeto da classe ProcessedDataCollection caso seja provido um DF
   
   """

   category:str
   data_name:str
   time_series_years:list[int]
   data_lines: list[DataLine]

   def create_processed_collection(self,df:DataFrame)->ProcessedDataCollection:
       return ProcessedDataCollection(
         self.category, self.data_name, self.time_series_years, df
       )

