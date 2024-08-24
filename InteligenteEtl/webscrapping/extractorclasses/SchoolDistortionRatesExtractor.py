from .AbstractDataExtractor import AbstractDataExtractor
from webscrapping.scrapperclasses import SchoolDistortionRatesScrapper
from datastructures import ProcessedDataCollection,YearDataPoint,DataTypes
import pandas as pd

class SchoolDistortionRatesExtractor(AbstractDataExtractor):
   
   DATA_CATEGORY = "Educação"
   DTYPE = DataTypes.FLOAT
   DATA_NAME = "Taxas de distorção idade-série"

   def extract_processed_collection(self, scrapper: SchoolDistortionRatesScrapper) -> list[ProcessedDataCollection]:
      data_points:list[YearDataPoint] = scrapper.extract_database()
      time_series_years:list[int] = YearDataPoint.get_years_from_list(data_points)

      joined_df:pd.DataFrame = self._concat_data_points(data_points)
      joined_df = self.__process_df(joined_df)
      print(joined_df.info())

      joined_df = self.__rename_and_add_cols(joined_df)
      joined_df = joined_df.dropna() #remove valores NaN

      collection = ProcessedDataCollection(
         category=self.DATA_CATEGORY,
         dtype=self.DTYPE,
         data_name=self.DATA_NAME,
         time_series_years=time_series_years,
         df= joined_df
      )

      return [collection]

   def __process_df(self,df: pd.DataFrame) -> pd.DataFrame:
        try:
            municipality_col = 'Unnamed: 3'
            location_col = 'Unnamed: 5'
            admin_dependency_col = 'Unnamed: 6'

            df_filtered = df[(df[location_col] == 'Total') & (df[admin_dependency_col] == 'Total')]
            
            filtered_df = df_filtered.drop(
               [location_col,admin_dependency_col],
               axis="columns"
            ) #remove colunas desnecessárias

            filtered_df = filtered_df.rename({
                municipality_col: self.CITY_CODE_COL,
            },axis="columns")

            filtered_df[self.CITY_CODE_COL] = filtered_df[self.CITY_CODE_COL].astype("int")
            filtered_df = filtered_df.reset_index(drop=True)
            return filtered_df
        
        except Exception as e:
            print(f"Erro ao processar o DataFrame: {e}")
            return None

   def __rename_and_add_cols(self,df:pd.DataFrame)->pd.DataFrame:
      EXTRACTED_DATA_VAL_COL = "Total"

      df = df.rename({
         EXTRACTED_DATA_VAL_COL: self.DATA_VALUE_COLUMN

      },axis="columns")

      df[self.DATA_IDENTIFIER_COLUMN] = self.DATA_NAME
      df[self.DTYPE_COLUMN] = self.DTYPE.value


      return df

