from abc import ABC,abstractmethod
from DBInterface.DataCollection import ProcessedDataCollection
import pandas as pd


class AbstractDataExtractor(ABC):
   
   CITY_COLUMN = "codigo_municipio" #constantes para o nome das colunas no dataframe final 
   YEAR_COLUMN = "ano"
   DATA_IDENTIFIER_COLUMN = "dado_identificador"
   DATA_VALUE_COLUMN = "valor"
   DTYPE_COLUMN = "tipo_dado"

   def __init__(self) -> None:
      pass

   @abstractmethod
   def extract_processed_collection(self,df:pd.DataFrame)->ProcessedDataCollection:
      pass

   def parse_strings(self,str:str)->str:
      """
      remove whitespace,\ n e  coloca todos os chars em lowercase, feito para facilitar acessar nomes de colunas
      complexos ao extrair as bases
      """
      return str.lower().replace(" ","").replace("\n","")

   #funções genéricas para ajudar a processar os dados no modelo que o Data Warehouse precisa
   def check_city_code(self,df:pd.DataFrame, city_code_column:str)->pd.DataFrame:
      """
      checa se um código de cidade do IBGE está dentro do padrão de 7 dígitos, ou se é um código antigo com num diferente
      de dígitos. É testado apenas um valor dessa coluna
      """

      city_code:int = int(df.at[0,city_code_column]) #o dado já é inteiro, essa função é para ser safe
      if city_code >= 1000000 and city_code < 10000000 : #tem 7 dígitos exatamente
         return df #retorna o DF normalmente 
      else:
         """
         TODO
         Algoritmo para consertar os códigos de menos dígitos

         1) ter um df com os codigos de 7 dígitos atualizados

         2) criar um coluna dos códigos original sem o último dígito

         3) dar merge do df original com código de 6 dígitos com o df só com códigos (7 e 6 dígitos)

         4) apagar as colunas com 6 dígitos e só deixa a com 7 dígitos 
         
         """

   def add_dimension_fks():
      """Função para adicionar as foreign keys para as dimensões município e dado"""
      pass