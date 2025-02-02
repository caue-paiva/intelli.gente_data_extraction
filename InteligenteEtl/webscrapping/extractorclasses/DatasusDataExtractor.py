import pandas as pd
import re
from datastructures.DataCollection import ProcessedDataCollection
from datastructures import DataTypes, YearDataPoint
from webscrapping.scrapperclasses.DatasusLinkScrapper import DatasusDataInfo, DatasusLinkScrapper
from .AbstractDataExtractor import AbstractDataExtractor
from typing import Type





class DatasusDataExtractor(AbstractDataExtractor):
   """
   Essa classe extraí os dados do site do datasus, a maioria dos dados são extraídos com um script do selenium que seleciona um
   CSV por anos dos dados, porém o coeficiente de Gini é um exceção onde o script de selenium não é necessário e o CSV
   tem 3 anos de dados.
   """

   EXTRACTED_TABLE_CITY_COL = "Município" #coluna que identifica o município nas tabelas do datasus
   NULL_VAL_IDENTIFIER = "..." #string que o datasus adota para dados com valores nulos

   def __init__(self) -> None:
      pass

   def __remove_city_names(self,df:pd.DataFrame)->pd.DataFrame:
      """
      As colunas de cidade do datasus tem o nome da cidade e o código (de 6 dígitos) do IBGE associadas a elas
      porém para carregar no BD precisamos apenas manter os códigos

      Args:
         df (pd.Dataframe): dataframe sendo processado
      
      Return:
          (pd.Dataframe): dataframe com a coluna de municipios apenas com os códigos do IBGE
      """
      remove_non_ints = lambda x : re.sub(r'[^0-9]', '', x)
      df[self.CITY_CODE_COL]= df[self.CITY_CODE_COL].apply(remove_non_ints)

      return df

   def __convert_column_values(self,df:pd.DataFrame, dtype: DataTypes)->pd.DataFrame:
      """
      Com o df no formato certo para ser carregado no BD, essa função transforma a coluna de municípios (com
      a função __remove_city_names() e transforma a coluna de valores em float 
      
      Args:
         df (pd.Dataframe): dataframe sendo processado
      
      Return:
          (pd.Dataframe): df no formato e com tipos prontos para ser inserido no BD
      """
      
      df = self.__remove_city_names(df) #deixa so os códigos na coluna do município (tira os nomes)
      city_code_is_valid = lambda x : x != "" and x != " "
      df = df[ df[self.CITY_CODE_COL].apply(city_code_is_valid)] #remove linhas onde o código do município não é valido
      
      try:
         df.loc[:,self.DATA_VALUE_COLUMN] = df[self.DATA_VALUE_COLUMN].astype(dtype.value) #acessa todos os rows da coluna de valores de dados e modifica
      except ValueError as e:
         if "could not convert string to float" in str(e): #erro na conversão de str para float/int
            try:
               df.loc[:,self.DATA_VALUE_COLUMN] = df[self.DATA_VALUE_COLUMN].replace(",",".",regex=True) #troca , por . para o pandas converter certo
               df.loc[:,self.DATA_VALUE_COLUMN] = df[self.DATA_VALUE_COLUMN].astype(dtype.value)
            except:
               raise ValueError("Não foi possível converter valores do df de str pra float/int")
      
      return df

   def __join_df_parts(self,data_list:list[YearDataPoint],data_info:DatasusDataInfo)->pd.DataFrame:
         """
         Junta os DFs extraidos de um ano específico do Datasus, cada DF deve dados de apenas 1 ano, com o
         DF e o ano sendo associados nos objeto YearDataPoint

         Args:
            data_list(list[YearDataPoint]): lista de YearDataPoints

            data_info (DatasusDataInfo): enum de infomação sobre o dado extraido do datasus
         Return:
            (pd.DataFrame): dataframe unido com os dados de todos os anos.
         """
         
         final_df: pd.DataFrame = pd.DataFrame()
         for data_point in data_list:
            new_df =  self.__process_df_right_shape(data_point.df,data_point.data_year,data_info)
            final_df = pd.concat(objs=[final_df,new_df],axis="index",ignore_index=True)
         
         return final_df

   def __process_df_right_shape(self,df:pd.DataFrame,data_year:int,data_info:DatasusDataInfo)->pd.DataFrame:
      """
      Processa o df extraido em um formato padrão para ser inserido no BD, e também remove valores NULL/NaN
      
      Caso a lista de anos que o df se refere seja maior que 1, que é o caso se o dado for o Coeficiente de Gini 
      (caso especial) então o DF será transformado por meio do método pd.melt() para transformar um DF wide (mtas colunas) 
      em um tall (menos colunas e mais linhas).

      Args:
         df (pd.Dataframe): df bruto que vai ser processado

         data_year (int) : Ano dos dados que o DF contém

         data_info (DatasusDataInfo): enum de infomação sobre o dado extraido do datasus

      Return:
          (pd.Dataframe): df no formato certo para ser inserido no banco de dados, mas não totalmente processado
      """
      
      df = df.dropna(how = "any", axis= "index") #da drop nos NaN que vem de linhas do CSV com informações sobre os estudos 
      get_only_valid_cities = lambda x : x != "Total" and "IGNORADO" not in x #funcao para filtrar as linhas cujo valor de municipio é ignorado ou valor total do brasil
      df = df[df[self.EXTRACTED_TABLE_CITY_COL].apply(get_only_valid_cities)]

      for col in df.columns:
         df[col] = df[col].replace(self.NULL_VAL_IDENTIFIER,None) #troca os valores nulos que o datasus coloca com o none
      
      df = df.dropna(axis="index") #remove valores nulos/none/NaN

       #df so tem um ano de dados e 2 colunas
      data_value_col:str = df.columns[1] #nome da coluna dos dados
      df = df.rename({data_value_col: self.DATA_VALUE_COLUMN},axis="columns") #troca o nome dela
      df[self.YEAR_COLUMN] = data_year


      df = df.rename({self.EXTRACTED_TABLE_CITY_COL: self.CITY_CODE_COL},axis="columns") #troca o nome da coluna de municípios
      df[self.DATA_IDENTIFIER_COLUMN] = data_info.value["data_name"] #coloca coluna do nome do dado
      df[self.DTYPE_COLUMN] = data_info.value["dtype"].value #coloca coluna do tipo de dado

      return df
   
   def extract_processed_collection(self,data_info:DatasusDataInfo)->list[ProcessedDataCollection]:
      """
      Função da interface que recebe um objeto scrapper, chama a função dele que retorna o df extraido do site do datasus.
      Esse DF retornado é processado, valores nulos são removidos, e o df é colocado no formato certo para ser inserido no BD.

      Args:
         data_info (DatasusDataInfo): Enum que representa o data_point a extrair
      
      Return:
         (list[ProcessedDataCollection]): Lista contendo objetos dessa Classe com os dados já processados e prontos para serem mandados pro BD
         
      """
      scrapper_object = DatasusLinkScrapper(data_info)
      data_list:list[YearDataPoint] = scrapper_object.extract_database()
      time_series_years:list[str] = [x.data_year for x in data_list]

      if len(data_list) < 1: #nenhum dado foi extraído
         return []
      
      data_info: DatasusDataInfo = scrapper_object.data_info
      processed_df:pd.DataFrame = self.__join_df_parts(data_list,data_info)

      processed_df = self.__convert_column_values(processed_df,scrapper_object.data_info.value["dtype"])
      processed_df = self.update_city_code(processed_df, self.CITY_CODE_COL)
     
      collection =  ProcessedDataCollection(
         category= data_info.value["data_topic"],
         dtype= scrapper_object.data_info.value["dtype"],
         data_name= data_info.value["data_name"],
         time_series_years= time_series_years,
         df = processed_df
      )
      return [collection]
