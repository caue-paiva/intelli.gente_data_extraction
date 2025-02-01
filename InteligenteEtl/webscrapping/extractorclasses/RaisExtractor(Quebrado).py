from datastructures import ProcessedDataCollection,YearDataPoint
from .AbstractDataExtractor import AbstractDataExtractor
from webscrapping.scrapperclasses import RaisScrapper,RaisDataInfo
from citiesinfo import match_city_names_with_codes
import pandas as pd



class RaisExtractor(AbstractDataExtractor):

   EXTRACTED_CITY_CODE_COL = "Município"
   EXTRACTED_DATA_VALUE_COL = "Total"


   webscrapping_delay_multiplier:int

   def __init__(self, webscrapping_delay_multiplier:int = 1):
      self.webscrapping_delay_multiplier = webscrapping_delay_multiplier

   def __filter_rows(self,df:pd.DataFrame)->pd.DataFrame:
      """
      Função para remover clinhas do df cujo valor na coluna Município seja diferente do padrão:
      {sigla_estado}-{nome_municipio}. Ex de linhas removidas: {ñ class}, Total, Seleções vigentes
      """
      if df[self.EXTRACTED_CITY_CODE_COL].dtype == "int64": #coluna de codigos do municipio so tem número, já n tem linhas para remover
         print("int64")
         return df

      row_is_normal = lambda x: x.isdigit()
      return df[df[self.EXTRACTED_CITY_CODE_COL].apply(row_is_normal)] #filtra as linhas do df
  
   def __rename_and_add_cols(self,df:pd.DataFrame,data_point_info:RaisDataInfo)->pd.DataFrame:
      df = df.rename({
         self.EXTRACTED_CITY_CODE_COL:self.CITY_CODE_COL,
         self.EXTRACTED_DATA_VALUE_COL: self.DATA_VALUE_COLUMN
      },axis="columns")

      #add as 2 colunas faltando
      df[self.DTYPE_COLUMN] = data_point_info.value["dtype"].value #valor da coluna do tipo de dado
      df[self.DATA_IDENTIFIER_COLUMN] = data_point_info.value["data_identifier"] #valor da coluna do identificador do dado

      return df

   def extract_processed_collection(self) -> list[ProcessedDataCollection]:
      #faz um loop por todos os data points presentes em RaisDataInfo e retorna uma lista com esses Dados 
      return [ self.__get_data_point(data_point) for data_point in RaisDataInfo ]

   def __get_data_point(self,data_point:RaisDataInfo) -> ProcessedDataCollection:
      scrapper_instance = RaisScrapper(data_point,self.webscrapping_delay_multiplier)

      data_points:list[YearDataPoint] = scrapper_instance.extract_database()
      time_series_years:list[int] = YearDataPoint.get_years_from_list(data_points)


      joined_df:pd.DataFrame = self._concat_data_points(data_points,add_year_col=True) #concatena data points
      joined_df = self.__filter_rows(joined_df) #filtra linhas do df
      
      joined_df[self.EXTRACTED_DATA_VALUE_COL] = joined_df[self.EXTRACTED_DATA_VALUE_COL].astype(
         data_point.value["dtype"].value #str que represeta o tipo do dado
      )

      joined_df = self.update_city_code(joined_df,self.EXTRACTED_CITY_CODE_COL) #coloca coluna do numero do município no dataframe
      joined_df = self.__rename_and_add_cols(joined_df,data_point) #renomeia e add colunas

      data_collection = ProcessedDataCollection(
         category=data_point.value["topic"],
         dtype=data_point.value["dtype"],
         data_name=data_point.value["data_identifier"],
         time_series_years=time_series_years,
         df=joined_df
      )
      return data_collection