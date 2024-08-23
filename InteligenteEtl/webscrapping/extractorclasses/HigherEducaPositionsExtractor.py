from typing import Type
from datastructures import ProcessedDataCollection,YearDataPoint, DataTypes
from webscrapping.scrapperclasses.AbstractScrapper import AbstractScrapper
from webscrapping.scrapperclasses import HigherEducaPositionsScrapper
from .AbstractDataExtractor import AbstractDataExtractor
import pandas as pd


class HigherEducaPositionsExtractor(AbstractDataExtractor):
   
   DATA_NAME = "Vagas no ensino superior"
   DATA_TOPIC = "Educação"
   DTYPE = DataTypes.INT
   EXTRACTED_CITY_CODE_COL = "CO_MUNICIPIO" #coluna original do

   EXTRACTED_DATA_VALUES_COL = "QT_VG_TOTAL" #coluna original do valor dos dados extraidos anteriormente


   def extract_processed_collection(self, scrapper: HigherEducaPositionsScrapper) -> list[ProcessedDataCollection]:
      data_points:list[YearDataPoint] = scrapper.extract_database()
      years_in_data:list[int] = YearDataPoint.get_years_from_list(data_points)

      joined_df:pd.DataFrame = self._concat_data_points(data_points) #junta data points e add coluna de anos
      joined_df = self.__filter_and_drop_cols(joined_df) #dropa colunas desnecessárias e NaNs
      joined_df = self.__sum_city_values(joined_df)

      return
      #joined_df = self.__add_and_rename_columns(joined_df)

      print(joined_df.info())
      joined_df.to_csv("processado_final.csv",index=False)

      collection = ProcessedDataCollection(
         category=self.DATA_TOPIC,
         data_name=self.DATA_NAME,
         dtype=self.DTYPE,
         time_series_years=years_in_data,
         df= joined_df
      )

      return [collection]

   def __sum_city_values(self,df:pd.DataFrame)->pd.DataFrame:
      """
      O dataframe extraido tem várias entradas repetidas para uma única cidade, pois cada entrada se refere a um curso de graduação em uma certa universidade
      """
      print(df[self.EXTRACTED_CITY_CODE_COL].nunique())
      grouped_object = df.groupby([self.EXTRACTED_CITY_CODE_COL,self.YEAR_COLUMN])
      df = grouped_object.sum().reset_index()
      print(df.info())
      return df

   def __filter_and_drop_cols(self,df:pd.DataFrame)->pd.DataFrame:
      FINAL_COLS = [self.EXTRACTED_CITY_CODE_COL,self.EXTRACTED_DATA_VALUES_COL,self.YEAR_COLUMN]
      existing_cols:list[str] = list(df.columns)

      cols_to_drop:list[str] = [col for col in existing_cols if col not in FINAL_COLS]

      df = df.drop(cols_to_drop,axis="columns")
      df = df.dropna(axis="index")
      return df
   
   def __add_and_rename_columns(self,df:pd.DataFrame)->pd.DataFrame:

      df[self.DATA_IDENTIFIER_COLUMN] = self.DATA_NAME #coluna do nome do dado
      df[self.DTYPE_COLUMN] = self.DTYPE.value #coluna de tipo de dado

      df = df.rename(
         {
            EXTRACTED_CITY_CODE_COL: self.CITY_CODE_COL,
            EXTRACTED_DATA_VALUES_COL: self.DATA_VALUE_COLUMN #renomeia colunas originais para o padrão
         }
      ,axis="columns")

      return df

