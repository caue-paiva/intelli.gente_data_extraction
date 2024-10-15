from datastructures import ProcessedDataCollection,YearDataPoint, DataTypes
from webscrapping.scrapperclasses import IdebFinalYearsScrapper
from .AbstractDataExtractor import AbstractDataExtractor
import pandas as pd


class idebFinalYearsExtractor(AbstractDataExtractor):
   
   DTYPE = DataTypes.FLOAT
   DATA_NAME = "Índice de desenvolvimento da educação básica (IDEB) - anos finais "
   DATA_TOPIC = "Educação"
   EXTRACTED_CITY_COL = "Código do Município"

   __SCRAPPER_CLASS = IdebFinalYearsScrapper()

   def __treat_vals_and_add_cols(self,df:pd.DataFrame)->pd.DataFrame:
      new_df = df.copy()
      
      new_df[self.DATA_VALUE_COLUMN] = new_df[self.DATA_VALUE_COLUMN].replace({"-":None})
      new_df = new_df.dropna(axis="index")
      
      new_df[self.DATA_VALUE_COLUMN] = new_df[self.DATA_VALUE_COLUMN].astype(self.DTYPE.value)
      new_df[self.YEAR_COLUMN] = new_df[self.YEAR_COLUMN].astype("int")

      new_df = new_df.rename({
            self.EXTRACTED_CITY_COL: self.CITY_CODE_COL,    
      },axis="columns")
      new_df[self.CITY_CODE_COL] = new_df[self.CITY_CODE_COL].astype("int")

      new_df[self.DTYPE_COLUMN] = self.DTYPE.value #coloca coluna de tipo de dado
      new_df[self.DATA_IDENTIFIER_COLUMN] = self.DATA_NAME #coloca coluna do 

      return new_df
   
   def extract_processed_collection(self)-> list[ProcessedDataCollection]:
      data_points:list[YearDataPoint] = self.__SCRAPPER_CLASS.extract_database()
      time_series_years:list[int]= YearDataPoint.get_years_from_list(data_points)
      joined_df:pd.DataFrame = self._concat_data_points(data_points)
      joined_df = self.__treat_vals_and_add_cols(joined_df)

      collection = ProcessedDataCollection(
         category=self.DATA_TOPIC,
         dtype=self.DTYPE,
         data_name=self.DATA_NAME,
         time_series_years=time_series_years,
         df=joined_df
      )

      return [collection]