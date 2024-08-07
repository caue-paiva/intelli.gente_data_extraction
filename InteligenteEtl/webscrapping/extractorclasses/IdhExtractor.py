from webscrapping.scrapperclasses import IdhScrapper
from datastructures import ProcessedDataCollection, YearDataPoint
from datastructures import DataTypes
from .AbstractDataExtractor import AbstractDataExtractor
import pandas as pd
from citiesinfo import get_city_code_from_string


class IdhExtractor(AbstractDataExtractor):

   EXTRACTED_CITY_NAME_COL = "Territorialidade" # 
   EXTRACTED_DATA_VALUES_COL = "IDHM" #coluna dos valores a serem extraídos

   DATA_NAME = ""
   DTYPE = DataTypes.FLOAT

   def extract_processed_collection(self, scrapper: IdhScrapper)-> list[ProcessedDataCollection]:
      data_points:list[YearDataPoint] = scrapper.extract_database()

   def __add_cols(self,df:pd.DataFrame, year:int)->pd.DataFrame:
      pass
   
   def __replace_city_names_for_codes(self,df:pd.DataFrame)->pd.DataFrame:
      get_city_state_code = lambda x: x[ x.find("(")+1:x.find("(")+3 ] #string: "São Paulo (SP) vira SP"
      extract_city_name_and_state = lambda x: get_city_code_from_string(
         city_name=x[ : x.find(")") ], #toda a string até os parênteses
         city_state=get_city_state_code(x) #pega código da cidade
      )

      df[self.CITY_CODE_COL] = df[self.EXTRACTED_CITY_NAME_COL].apply(extract_city_name_and_state) #cria a coluna de códigos dos municípios
      df = df.drop(self.EXTRACTED_CITY_NAME_COL,axis="columns") #tira coluna de nomes dos municípios

      return df
   
   def __drop_unnecessary_cols(self,df:pd.DataFrame)->pd.DataFrame:
      necessary_cols: list[str] = [self.EXTRACTED_CITY_NAME_COL,self.EXTRACTED_DATA_VALUES_COL]#lista de colunas necessárias
      all_cols:list[str] = df.columns

      cols_to_drop:list[str] = [x not in necessary_cols for x in all_cols]
      df = df.drop(cols_to_drop,axis="columns")

      return df