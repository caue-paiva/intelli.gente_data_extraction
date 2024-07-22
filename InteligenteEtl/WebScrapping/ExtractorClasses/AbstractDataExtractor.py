from abc import ABC,abstractmethod
from DataClasses import ProcessedDataCollection, YearDataPoint
from WebScrapping.ScrapperClasses.AbstractScrapper import AbstractScrapper
import pandas as pd
from typing import Type
from citiesinfo import get_city_codes
from etl_config import get_config

class AbstractDataExtractor(ABC):

   """
   Essa classe fornece a interface utilizada pelas classes extratoras de dados, que devem receber um df dos dados brutos
   extraidos anteriormente e retorna uma classe (ou uma lista delas) de Coleções de dados processadas, que é a classe
   que vai ser usada para inserir os dados no BD/Data Warehouse 
   """
   
   CITY_CODE_COL:str = get_config("CITY_CODE_COL") #constantes da configuração para o nome das colunas no dataframe final 
   YEAR_COLUMN:str = get_config("YEAR_COL")
   DATA_IDENTIFIER_COLUMN:str = get_config("DATA_IDENTIFIER_COL")
   DATA_VALUE_COLUMN:str =  get_config("DATA_VALUE_COL")
   DTYPE_COLUMN:str = get_config("DTYPE_COL")

   def __init__(self) -> None:
      pass

   @abstractmethod
   def extract_processed_collection(self,scrapper:Type[AbstractScrapper])->ProcessedDataCollection:
      """
      Dado um objeto que pertence a uma classe scrapper concreta (Herda de AbstractScrapper), chama função de extrair dados 
      desse objeto, processa os dados e retorna um objeto ProcessedDataCollection

      Args:
         scrapper:Type[AbstractScrapper]: Objeto da família de classes Scrapper
      Return:
         ProcessedDataCollection: Objeto que representa os dados tratados e com os metadados presentes
      """
      pass
 
   def _concat_data_points(self,list_of_datapoints:list[YearDataPoint])->pd.DataFrame:
      """
      Recebe uma lista de objetos YearDataPoints, adiciona as colunas de ano e tipo de dado para cada df deles e dá append em
      cada df dos objetos da lista, até retornar um DF completo com todos os dados
      """

      appended_df = pd.DataFrame()
      for datapoint in list_of_datapoints:
         df = datapoint.df.copy()
         df[self.YEAR_COLUMN] = datapoint.data_year
         appended_df = pd.concat([appended_df,df],axis="index",ignore_index=True)

      return appended_df

   #funções genéricas para ajudar a processar os dados no modelo que o Data Warehouse precisa
   def parse_strings(self,str:str)->str:
      """
      remove whitespace,\ n e  coloca todos os chars em lowercase, feito para facilitar acessar nomes de colunas
      complexos ao extrair as bases
      """
      return str.lower().replace(" ","").replace("\n","")

   def check_city_code(self,df:pd.DataFrame, city_code_column:str)->bool:
      """
      checa se um código de cidade do IBGE está dentro do padrão de 7 dígitos, ou se é um código antigo com num diferente
      de dígitos. É testado apenas um valor dessa coluna
      """

      city_code:int = int(df.at[0,city_code_column]) #o dado já é inteiro, essa função é para ser safe
      if city_code >= 1000000 and city_code < 10000000 : #tem 7 dígitos exatamente
         return True #retorna o DF normalmente 
      else:
         return False

   def update_city_code(self,df:pd.DataFrame, city_code_column:str)->pd.DataFrame:
      """
      Atualiza o código dos municípios do IBGE de 6 dígitos para de 7 dígitos. Faz isso com a tabela verdade dos códigos dos municípios
      disponíveis no módulo CityDataInfo e com operações de merge/join dos dataframes

      Args:
         df (pd.DataFrame): df para ter seus códigos de municípios  atualizados
         city_code_column (str): nome da coluna com os códigos do IBGE dos munícipios do df desatualizado

      Return:
         (pd.Dataframe): df com os códigos dos municípios atualizados para 7 dígitos

      """

      if self.check_city_code(df,city_code_column): #código do município está dentro dos padrões
         return df
      else: #código do município tem 6 dígitos, vamos atualizar
         seven_digit_codes: pd.Series = pd.Series(get_city_codes()).astype("str") #cria um series do pandas com os códigos de 7 dígitos
         
         get_first_6_digits = lambda x : x[:6] #função para pegar os 6 primeiros dígitos da string do código
         
         first_6_digits_updated_codes:pd.Series = seven_digit_codes.apply(get_first_6_digits) #cria uma series com os 6 primeiro dígitos da lista dos códigos atualizado
         keys_df = pd.DataFrame({
            "seven_digits": seven_digit_codes,
            "six_digits_from_seven": first_6_digits_updated_codes
         },dtype="int")

         df[city_code_column] = df[city_code_column].astype("int")

         joined_df = df.merge(keys_df,how="left",left_on=[city_code_column],right_on=["six_digits_from_seven"])
         joined_df = joined_df.drop(["six_digits_from_seven",city_code_column],axis="columns")
         joined_df = joined_df.rename({"seven_digits":city_code_column},axis="columns")

         return joined_df
         """
         Algoritmo para consertar os códigos de menos dígitos

         1) ter um df com os codigos de 7 dígitos atualizados

         2) criar um coluna dos códigos original sem o último dígito

         3) dar merge do df original com código de 6 dígitos com o df só com códigos (7 e 6 dígitos)

         4) apagar as colunas com 6 dígitos e só deixa a com 7 dígitos 
         """

   def add_dimension_fks():
      """Função para adicionar as foreign keys para as dimensões município e dado"""
      pass