import pandas as pd
import re
from DBInterface.DataCollection import ProcessedDataCollection
from DataEnums import DataTypes
from WebScrapping.ScrapperClasses.DatasusLinkScrapper import DatasusDataInfo, DatasusLinkScrapper
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

   def __join_df_parts(self,df_list:list[pd.DataFrame], list_of_years:list[int],data_info:DatasusDataInfo)->pd.DataFrame:
         """
         Junta os CSVs extraidos de um ano específico do Datasus, cada CSV deve conter dados de apenas 1 ano
         Não é usado no caso especial dos dados do coef de gini

         Args:
            df_list(list[pd.DataFrame]): lista de dataframes

            list_of_years(list[int]): lista de anos que cada dataframe se refere
            OBS: o indice do vetor dos 2 argumentos devem ser relacionados, ou seja o df do index 0 se refere ao ano de index 0

            data_info (DatasusDataInfo): enum de infomação sobre o dado extraido do datasus

         Return:
               (pd.DataFrame): dataframe unido com os dados de todos os anos.
         """
         
         final_df: pd.DataFrame = pd.DataFrame()
         for df,year in zip(df_list,list_of_years):
            new_df =  self.__process_df_right_shape(df,[year],data_info)
            final_df = pd.concat(objs=[final_df,new_df],axis="index",ignore_index=True)
         
         return final_df

   def __process_df_right_shape(self,df:pd.DataFrame,list_of_years:list[int],data_info:DatasusDataInfo)->pd.DataFrame:
      """
      Processa o df extraido em um formato padrão para ser inserido no BD, e também remove valores NULL/NaN
      
      Caso a lista de anos que o df se refere seja maior que 1, que é o caso se o dado for o Coeficiente de Gini 
      (caso especial) então o DF será transformado por meio do método pd.melt() para transformar um DF wide (mtas colunas) 
      em um tall (menos colunas e mais linhas) .

      Args:
         df (pd.Dataframe): df bruto que vai ser processado

         list_of_years(list[int]): lista de anos dos dados que o DF contém

         data_info (DatasusDataInfo): enum de infomação sobre o dado extraido do datasus

      Return:
          (pd.Dataframe): df no formato certo para ser inserido no banco de dados, mas não totalmente processado
      """
      
      df = df.dropna(how = "any", axis= "index") #da drop nos NaN que vem de linhas do CSV com informações sobre os estudos 
      get_only_valid_cities = lambda x : x != "Total" and "IGNORADO" not in x #funcao para filtrar as linhas cujo valor de municipio é ignorado ou valor total do brasil
      df = df[df[self.EXTRACTED_TABLE_CITY_COL].apply(get_only_valid_cities)]

      for col in df.columns:
         df[col] = df[col].replace(self.NULL_VAL_IDENTIFIER,None) #remove os valores null em cada coluna

      if len(list_of_years) == 1: #df so tem um ano de dados e 2 colunas
         data_value_col:str = df.columns[1] #nome da coluna dos dados
         df = df.rename({data_value_col: self.DATA_VALUE_COLUMN},axis="columns") #troca o nome dela
         df["ano"] = list_of_years[0]
      else: #caso especial do coef de gini
         df = pd.melt(df, id_vars=[self.EXTRACTED_TABLE_CITY_COL], var_name=self.YEAR_COLUMN, value_name=self.DATA_VALUE_COLUMN)

      df = df.rename({self.EXTRACTED_TABLE_CITY_COL: self.CITY_CODE_COL},axis="columns") #troca o nome da coluna de municípios
      df[self.DATA_IDENTIFIER_COLUMN] = data_info.value["data_name"] #coloca coluna do nome do dado
      df[self.DTYPE_COLUMN] = data_info.value["dtype"].value #coloca coluna do tipo de dado

      return df
 
   def extract_processed_collection(self,scrapper: Type[DatasusLinkScrapper])->ProcessedDataCollection:
      """
      Função da interface que recebe um objeto scrapper, chama a função dele que retorna o df extraido do site do datasus.
      Esse DF retornado é processado, valores nulos são removidos, e o df é colocado no formato certo para ser inserido no BD.

      Args:
         scrapper( classe ou subclasse (Type) de DatasusLinkScrapper): objeto da classe mencionada que faz scrapping dos dados do site do datasus
      
         data_category (str): categoria (tabela do BD) que o dado pertence à

         data_identifier (str): nome do dado
      
      Return:

         (ProcessedDataCollection): Classe com os dados já processados e prontos para serem mandados pro BD
         
      """
     
      dfs,time_series_years = scrapper.extract_database()
      if len(dfs) < 1:
         raise IOError("Lista de dataframes deve ter tamanho de pelo menos 1")
      
      data_info: DatasusDataInfo = scrapper.data_info
      if data_info == DatasusDataInfo.GINI_COEF: #df único, caso separado
         processed_df:pd.DataFrame = self.__process_df_right_shape(dfs[0],time_series_years,data_info)
      else: #lista de dataframes, um para cada ano, caso padrão
         processed_df:pd.DataFrame = self.__join_df_parts(dfs,time_series_years,data_info)

      processed_df = self.__convert_column_values(processed_df,scrapper.data_info.value["dtype"])
      processed_df = self.update_city_code(processed_df, self.CITY_CODE_COL)
     
      return ProcessedDataCollection(
         category= data_info.value["data_topic"],
         dtype= scrapper.data_info.value["dtype"],
         data_name= data_info.value["data_name"],
         time_series_years= time_series_years,
         df = processed_df
      ).fill_non_existing_cities()

if __name__ == "__main__":

   url1 = "http://tabnet.datasus.gov.br/cgi/deftohtm.exe?ibge/censo/cnv/alfbr"
   url2 = "http://tabnet.datasus.gov.br/cgi/ibge/censo/cnv/ginibr.def"
   abreviation1 = DatasusDataInfo.ILLITERACY_RATE
   abreviation2 = DatasusDataInfo.GINI_COEF

   extractor = DatasusDataExtractor()
   scrapper = DatasusLinkScrapper(url1,abreviation1)

   collection = extractor.extract_processed_collection(scrapper,"educacao","taxa de analfabetismo")


