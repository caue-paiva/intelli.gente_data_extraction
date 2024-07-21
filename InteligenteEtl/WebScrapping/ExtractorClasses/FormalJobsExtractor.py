import pandas as pd
from DataClasses.DataCollection import ProcessedDataCollection
from DataClasses import DataTypes , YearDataPoint
from WebScrapping.ScrapperClasses.FormalJobsScrapper import FormalJobsScrapper
from .AbstractDataExtractor import AbstractDataExtractor


class FormalJobsExtractor(AbstractDataExtractor):
   
   #constantes sobre o DF bruto extraido pelo webscrapping, como nome das colunas
   EXTRACTED_LOCATION_CODE_COL = "Cod. Loc."
   EXTRACTED_DATA_NAME = "População ocupada com vínculo formal"
   EXTRACTED_DTYPE: DataTypes = DataTypes.INT
   NULL_VAL_IDENTIFIER = "Não Disponível"
   CATEGORY: str = "Emprego" #tópico do dado


   def __treat_nulls_and_add_cols(self,df:pd.DataFrame)->pd.DataFrame:
      """
      remove as aspas nas colunas e valores do df  e converte eles para números no caso dos valores
      """
      final_df_val_col:str = self.DATA_VALUE_COLUMN #coluna de valores
      final_dtype_col:str = self.DTYPE_COLUMN #coluna de tipo de dado
      
      df[final_df_val_col] = df[final_df_val_col].replace({self.NULL_VAL_IDENTIFIER : DataTypes.NULL.value})
      infer_dtype = lambda x: self.EXTRACTED_DTYPE.value if x.isdigit() else DataTypes.NULL.value
      dtype_col:pd.Series = df[final_df_val_col].apply(infer_dtype)
      df[final_dtype_col] = dtype_col

      return df

   def __treat_column_dtypes(self,df:pd.DataFrame)->pd.DataFrame:
         """
         trata e muda os tipos de dados das colunas
         """
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
      df = df[df[self.CITY_CODE_COL].apply(is_city_code)]
      df = df.reset_index(drop=True) #reseta o index para ele começar do zero

      return df
   
   def __make_df_into_right_shape(self,df:pd.DataFrame)->pd.DataFrame:
      
      final_df_year_col:str = self.YEAR_COLUMN
      final_df_val_col:str = self.DATA_VALUE_COLUMN
      df = pd.melt(df, id_vars=[self.EXTRACTED_LOCATION_CODE_COL], var_name=final_df_year_col, value_name=final_df_val_col)
      
      final_df_city_code_col:str = self.CITY_CODE_COL
      df.columns = [final_df_city_code_col,final_df_year_col,final_df_val_col]
      
      return df

   def __concat_dfs(self,data_points:list[YearDataPoint])->pd.DataFrame:
      """
      Concatena os dataframes de anos diferentes em um dataframe só
      """

      new_df = pd.DataFrame()
      for point in data_points:
         year:str = point.data_year
         df = point.df
         df[self.YEAR_COLUMN] = year
         new_df = pd.concat([new_df,df],ignore_index=True)

      new_df.columns = [self.CITY_CODE_COL,self.YEAR_COLUMN,self.DATA_VALUE_COLUMN] #novas colunas do df concatenado
      return new_df

   def extract_processed_collection(self,formal_jobs_scrapper:FormalJobsScrapper)-> ProcessedDataCollection:
      data_list = formal_jobs_scrapper.extract_database()
      time_series_years:list[str] = YearDataPoint.get_years_from_list(data_list)
      dfs:list[pd.DataFrame] = YearDataPoint.get_dfs_from_list(data_list)

      df = self.__concat_dfs(data_list)
      print(df.info())
      df = self.__remove_non_city_lines(df)
      print(df.info())

      #df = self.__make_df_into_right_shape(df)
      df = self.__treat_column_dtypes(df)
      print(df.info())

      df = self.__treat_nulls_and_add_cols(df)
      print(df.info())
      if df.empty:
            raise ValueError("DataFrame is empty")

        # Check if the DataFrame contains the expected index
      if 0 not in df.index:
            raise KeyError(f"Index 0 not found in DataFrame. Available indices: {df.index}")
      df = super().update_city_code(df,self.CITY_CODE_COL) #atualiza código do município de 6 para 7 dígitos

      collection = ProcessedDataCollection(
         category=self.CATEGORY,
         dtype=self.EXTRACTED_DTYPE,
         data_name=self.EXTRACTED_DATA_NAME,
         time_series_years=time_series_years,
         df= df
      )
      return collection
   
