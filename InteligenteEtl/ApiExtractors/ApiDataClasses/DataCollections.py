from .DataLine import DataLine
from DataClasses import DataTypes, ProcessedDataCollection
from pandas import DataFrame
from dataclasses import dataclass

@dataclass
class RawDataCollection:
   """
   Classe que guarda um  dado específico não processado vindos da API num formato intermediário com os metadados sobre categoria,
   série histórica...

   tem um método para retornar um objeto da classe ProcessedDataCollection caso seja provido um DF
   
   """

   category:str
   data_name:str
   data_type: DataTypes
   time_series_years:list[int]
   data_lines: list[DataLine]

   def create_processed_collection(self,df:DataFrame)->ProcessedDataCollection:
      return ProcessedDataCollection(
         self.category,self.data_type ,self.data_name, self.time_series_years, df
      )

