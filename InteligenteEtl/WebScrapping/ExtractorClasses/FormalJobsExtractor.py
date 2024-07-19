import pandas as pd
from DataClasses.DataCollection import ProcessedDataCollection
from DataClasses import DataTypes
from WebScrapping.ScrapperClasses.FormalJobsScrapper import FormalJobsScrapper
from .AbstractDataExtractor import AbstractDataExtractor


class FormalJobsExtractor(AbstractDataExtractor):
   
   #constantes sobre o DF bruto extraido pelo webscrapping, como nome das colunas
   EXTRACTED_LOCATION_CODE_COL = "Cod. Loc."
   EXTRACTED_LOCATION_NAME_COL = "Divisões Territoriais"
   EXTRACTED_DATA_NAME = "População ocupada com vínculo formal"
   EXTRACTED_DTYPE: DataTypes = DataTypes.INT
   
   NULL_VAL_IDENTIFIER = "Não Disponível"
   TIME_SERIES_YEARS:list[int] = [2000,2010] #anos da série histórica,hard-coded por enquanto
   CATEGORY: str = "Emprego" #tópico do dado


   def __treat_nulls_and_add_cols(self,df:pd.DataFrame)->pd.DataFrame:
      """
      remove as aspas nas colunas e valores e converte eles para números no caso dos valores
      """
      final_df_val_col:str = self.DATA_VALUE_COLUMN
      final_dtype_col:str = self.DTYPE_COLUMN
      
      df[final_df_val_col] = df[final_df_val_col].replace({self.NULL_VAL_IDENTIFIER : DataTypes.NULL.value})
      infer_dtype = lambda x: self.EXTRACTED_DTYPE.value if x.isdigit() else DataTypes.NULL.value
      dtype_col:pd.Series = df[final_df_val_col].apply(infer_dtype)
      df[final_dtype_col] = dtype_col

      return df

   def __treat_column_dtypes(self,df:pd.DataFrame)->pd.DataFrame:
         final_df_data_name_col:str = self.DATA_IDENTIFIER_COLUMN
         final_df_val_col:str = self.DATA_VALUE_COLUMN
         final_df_city_code_col:str = self.CITY_CODE_COL

         #tira o . das strings que representam os dados de inteiros (401.192 -> 401192)
         df[final_df_val_col] = df[final_df_val_col].apply(lambda x: x.replace(".",""))
         df[final_df_data_name_col] = self.EXTRACTED_DATA_NAME

         df[final_df_city_code_col] = df[final_df_city_code_col].astype("int")

         return df

   def __remove_non_city_lines(self,df:pd.DataFrame)->pd.DataFrame:
      """
      Remove linhas do df que não são municípios (ex dados sobre o país ou estados). Considera que os dados dos municípios estão no padrão
      do código de 6 ou mais dígitos do IBGE
      """
      is_city_code = lambda x : x.isdigit() and len(x) >= 6 #checa se a string de código de localização é um código de 6 ou mais numeros
      df = df[df[self.EXTRACTED_LOCATION_CODE_COL].apply(is_city_code)]

      return df
   
   def __make_df_into_right_shape(self,df:pd.DataFrame)->pd.DataFrame:
      df = df.drop([self.EXTRACTED_LOCATION_NAME_COL],axis="columns")
      
      final_df_year_col:str = self.YEAR_COLUMN
      final_df_val_col:str = self.DATA_VALUE_COLUMN
      df = pd.melt(df, id_vars=[self.EXTRACTED_LOCATION_CODE_COL], var_name=final_df_year_col, value_name=final_df_val_col)
      
      final_df_city_code_col:str = self.CITY_CODE_COL
      df.columns = [final_df_city_code_col,final_df_year_col,final_df_val_col]
      
      return df

   def extract_processed_collection(self,formal_jobs_scrapper:FormalJobsScrapper)-> ProcessedDataCollection:
      df: pd.DataFrame = formal_jobs_scrapper.extract_database()

      df = self.__remove_non_city_lines(df)
      df = self.__make_df_into_right_shape(df)
      df = self.__treat_column_dtypes(df)
      df = self.__treat_nulls_and_add_cols(df)
      df = super().update_city_code(df,self.CITY_CODE_COL) #atualiza código do município de 6 para 7 dígitos

      collection = ProcessedDataCollection(
         category=self.CATEGORY,
         dtype=self.EXTRACTED_DTYPE,
         data_name=self.EXTRACTED_DATA_NAME,
         time_series_years=self.TIME_SERIES_YEARS,
         df= df
      )

      return collection.fill_non_existing_cities()
   
