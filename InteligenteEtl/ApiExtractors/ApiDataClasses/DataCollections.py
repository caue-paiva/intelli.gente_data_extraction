from .DataLine import DataLine
from pandas import DataFrame
from dataclasses import dataclass
from DBInterface import ProcessedDataCollection

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

