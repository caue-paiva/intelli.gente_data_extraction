import pandas as pd
import re, os
from DataCollection import ProcessedDataCollection
from webscrapping.DatasusLinkScrapper import DatasusAbreviations , DatasusLinkScrapper
from AbstractDataExtractor import AbstractDataExtractor

class DatasusDataExtractor(AbstractDataExtractor):

   CITY_IDENTIFIER_COLUMN: str = "Município"

   def __init__(self) -> None:
      pass

   def __remove_city_name_column(self,df:pd.DataFrame)->pd.DataFrame:
      remove_non_ints = lambda x : re.sub(r'[^0-9]', '', x)
      df[self.CITY_IDENTIFIER_COLUMN]= df[self.CITY_IDENTIFIER_COLUMN].apply(remove_non_ints)

      return df

   def __transform_df(self,df:pd.DataFrame, data_identifier:str,data_value_col:str)->pd.DataFrame:
      df[self.DATA_IDENTIFIER_COLUMN] = data_identifier #coloca coluna do nome do dado

      df = self.__remove_city_name_column(df) #deixa so os códigos na coluna do município (tira os nomes)
      df = df.rename({data_value_col: self.DATA_VALUE_COLUMN}, axis="columns") #renomeia a coluna com os valores dos dados
      
      try:
         df[self.DATA_IDENTIFIER_COLUMN] = df[self.DATA_IDENTIFIER_COLUMN].astype("float64")
      except ValueError as e:
         if "could not convert string to float" in str(e):
            try:
               df[self.DATA_IDENTIFIER_COLUMN] = df[self.DATA_IDENTIFIER_COLUMN].replace(",",".",regex=True)
               df[self.DATA_IDENTIFIER_COLUMN] = df[self.DATA_IDENTIFIER_COLUMN].astype("float64")
            except:
               raise ValueError("Não foi possível converter valores do df de str pra float")
      
      df[self.DTYPE_COLUMN] = "float"
      return df

   def __join_df_parts(self,df_list:list[pd.DataFrame], list_of_years:list[int])->pd.DataFrame:
         """
         Junta os CSVs extraidos de um ano específico do Datasus, cada CSV deve conter dados de apenas 1 ano
         """
         if len(df_list) != len(list_of_years):
            raise IOError("Tamanho da lista de DF é difernte do tamanho da lista de anos")
         
         final_df: pd.DataFrame = pd.DataFrame()
         for df,year in zip(df_list,list_of_years):
            new_df =  self.__process_and_clean_df(df,[year])
            final_df = pd.concat(objs=[final_df,new_df],axis="index",ignore_index=True)
         
         return final_df

   def __process_and_clean_df(self,df:pd.DataFrame,list_of_years:list[int],data_value_col:str)->pd.DataFrame:
      df = df.dropna(how = "any", axis= "index") #da drop nos NaN que vem de linhas do CSV com informações sobre os estudos 
      df = df[df["Município"] != "Total"]
      for col in df.columns:
         df[col] = df[col].replace("...",None)

      if len(list_of_years) == 1:
         df["ano"] = list_of_years[0]
      else:
         df = pd.melt(df, id_vars=['Município'], var_name='ano', value_name=data_value_col)

      return df

   def extract_processed_collection(
      self,
      data_scrapper: DatasusLinkScrapper,
      data_category:str,
      data_identifier:str,
   )->ProcessedDataCollection:
      
      dfs = data_scrapper.extract_database()

      if len(dfs) < 1:
         raise IOError("Lista de dataframes deve ter tamanho de pelo menos 1")

      if data_scrapper.data_abrevia == DatasusAbreviations.GINI_COEF: #df único, caso separado
         processed_df:pd.DataFrame = self.__process_and_clean_df(dfs[0],time_series_years)
      else: #lista de dataframes, um para cada ano, caso padrão
         processed_df:pd.DataFrame = self.__join_df_parts(dfs,time_series_years)

      processed_df = self.__transform_df(processed_df,data_identifier,data_abrevia.value)
      return ProcessedDataCollection(data_category,data_identifier,time_series_years,processed_df)

if __name__ == "__main__":


   url1 = "http://tabnet.datasus.gov.br/cgi/deftohtm.exe?ibge/censo/cnv/alfbr"
   url2 = "http://tabnet.datasus.gov.br/cgi/ibge/censo/cnv/ginibr.def"
   abreviation1 = DatasusAbreviations.ILLITERACY_RATE
   abreviation2 = DatasusAbreviations.GINI_COEF

   extractor = DatasusDataExtractor()
   scrapper = DatasusLinkScrapper()

   df = scrapper.extract_database(url1,abreviation1)

   collection = extractor.extract_processed_collection()


   """
   df = pd.read_csv(os.path.join("webscrapping","tempfiles","datasus_alfbr.csv"))
   collection:ProcessedDataCollection = extractor.get_data_collection(df,"educa",[1991,2000,2010],"TAXA ANALFABETISMO","Município","Taxa_de_analfabetismo")

   #df = __transform_df(df,"TAXA ANALFABETISMO","Município","Taxa_de_analfabetismo")

   print(collection.df.head(5))
   print(collection.df.info())
   """