from abc import ABC,abstractmethod
from enum import Enum
from DBInterface.DataCollection import ProcessedDataCollection
import pandas as pd


class AbstractDataExtractor(ABC):
   
   CITY_COLUMN = "codigo_municipio" #constantes para o nome das colunas no dataframe final 
   YEAR_COLUMN = "ano"
   DATA_IDENTIFIER_COLUMN = "dado_identificador"
   DATA_VALUE_COLUMN = "valor"
   DTYPE_COLUMN = "tipo_dado"


   def __init__(self) -> None:
      pass

   @abstractmethod
   def extract_processed_collection(self,df:pd.DataFrame)->ProcessedDataCollection:
      pass