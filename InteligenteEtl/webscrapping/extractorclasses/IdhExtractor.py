from webscrapping.scrapperclasses import IdhScrapper
from datastructures import ProcessedDataCollection, YearDataPoint
from datastructures import DataTypes
from .AbstractDataExtractor import AbstractDataExtractor
import pandas as pd
from citiesinfo import  match_city_names_with_codes


class IdhExtractor(AbstractDataExtractor):

   EXTRACTED_CITY_NAME_COL = "Territorialidade" # 
   EXTRACTED_DATA_VALUES_COL = "IDHM" #coluna dos valores a serem extraídos

   DATA_NAME = "Índice de desenvolvimento humano do município (IDH-M)"
   DTYPE = DataTypes.FLOAT
   DATA_TOPIC = "IDH-M" #tópico

   __SCRAPPER_CLASS = IdhScrapper() #classe scrapper para fazer webscraping

   def extract_processed_collection(self)-> list[ProcessedDataCollection]:
      data_points:list[YearDataPoint] = self.__SCRAPPER_CLASS.extract_database()
      time_series_years:list[int] = YearDataPoint.get_years_from_list(data_points)

      df = self._concat_data_points(data_points) #junta todos os dfs da lista de datapoint e add coluna do ano
      df = self.__replace_city_names_for_codes(df) #troca o nome da cidade por códigos de municípios
      df = self.__drop_unnecessary_cols(df) #retira as colunas desnecessárias
      final_df = self.__add_and_rename_cols(df) #add colunas e renomeia algumas

      collection = ProcessedDataCollection(
         category=self.DATA_TOPIC,
         dtype=self.DTYPE,
         data_name=self.DATA_NAME,
         time_series_years=time_series_years,
         df=final_df
      )
      return [collection] #retorna uma lista desse objeto para manter padronização

   def __add_and_rename_cols(self,df:pd.DataFrame)->pd.DataFrame:
      df[self.DTYPE_COLUMN] = self.DTYPE.value #coluna de tipo de dado
      df[self.DATA_IDENTIFIER_COLUMN] = self.DATA_NAME #coluna do nome/identificador do dado

      df = df.rename(mapper={
         self.EXTRACTED_DATA_VALUES_COL:self.DATA_VALUE_COLUMN, #coluna dos valores dos dados é renomeada
         self.EXTRACTED_CITY_NAME_COL: self.CITY_CODE_COL #coluna dos códigos dos municípios será renomeada
      },axis="columns")

      return df
   
   def __replace_city_names_for_codes(self,df:pd.DataFrame)->pd.DataFrame:
      get_city_state_code = lambda x: x[ x.find("(")+1:x.find("(")+3 ] #string: "São Paulo (SP) vira SP"
      df["codigo_uf"] = df[self.EXTRACTED_CITY_NAME_COL].apply(get_city_state_code) #cria coluna de siglas do estado
      df[self.EXTRACTED_CITY_NAME_COL] = df[self.EXTRACTED_CITY_NAME_COL].apply(lambda x: x[ : x.find("(") ])
      
      df = match_city_names_with_codes(df,self.EXTRACTED_CITY_NAME_COL,"codigo_uf")
      df = df.drop(self.EXTRACTED_CITY_NAME_COL,axis="columns") #tira coluna de nomes dos municípios

      return df
   
   def __drop_unnecessary_cols(self,df:pd.DataFrame)->pd.DataFrame:
      necessary_cols: list[str] = [self.EXTRACTED_CITY_NAME_COL,self.EXTRACTED_DATA_VALUES_COL,self.YEAR_COLUMN,self.CITY_CODE_COL]#lista de colunas necessárias
      all_cols:list[str] = df.columns

      cols_to_drop:list[str] = [x for x in all_cols if x not in necessary_cols]
      df = df.drop(cols_to_drop,axis="columns")

      return df