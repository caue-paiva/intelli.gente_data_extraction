import pandas as pd
import os
from webscrapping.extractorclasses import TableDataPoints
from webscrapping.extractorclasses import AbstractDataExtractor
from webscrapping.scrapperclasses import IbgePibCidadesScrapper
from typing import Type
from datastructures import DataTypes, BaseFileType, ProcessedDataCollection, YearDataPoint
from enum import Enum

"""
Existem cidades com nomes duplicados, vai ser usado o código de município para identificar esses municípios, porém bases antigas como o datasus 2010 usam
um código quase identico porém com 6 dígitos e não 7, isso deve ser levado em conta no ETL, Webscrapping

"""

class CitiesGDPDataInfo(Enum):
   """
   Enum para representar todas as possibilidades de dados que estão presentes do DF/tabela extraida do WebScrapping
   """
   PERCAPITA_GDP = {
      "data_category": "caracterizacao_socio_economica",
      "data_name": "PIB per capita",
      "column_name": """Produto Interno Bruto per capita, 
       a preços correntes
       (R$ 1,00)""",
      "dtype": DataTypes.FLOAT,
      "multiply_amount": 1
   }
   AGRI_GDP = {
      "data_category": "caracterizacao_socio_economica",
      "data_name": "PIB Agropecuária",
      "column_name": """Valor adicionado bruto da Agropecuária, 
   a preços correntes
   (R$ 1.000)""",
      "dtype": DataTypes.FLOAT,
      "multiply_amount": 1000
   }





class CategoryDataExtractor(AbstractDataExtractor):

   EXTRACTED_DATA_YEAR_COL = "Ano"
   EXTRACTED_DATA_CITY_CODE_COL = "Código do Município"

   def extract_processed_collection(self,scrapper: Type[IbgePibCidadesScrapper], data_info: TableDataPoints)->list[ProcessedDataCollection]:
      """ 
      TODO: Completar a função 


      extrai o dataframe bruto da base em um df menor apenas com as colunas de ano, código de cidade e os pontos de dados buscados
      
      Args:
        scrapper (Type[IbgePibCidadesScrapper]) : um objeto dessa classe ou de uma classe filha que implemente a interface extract_base 
         data_info (TableDataPoints) : Classe para especificar onde e como os dados buscados estão no df bruto
      
      Return:
         (dataframe): dataframe com as colunas: ano, código_muni, dado_identificador, valor e tipo_dado. Esse formato será similar aos 
         dados em cada tabela de categoria
      """
      
      raw_df:pd.DataFrame = scrapper.extract_database()
      processed_df = self.extract_data_points(df,data_info)

      data_collection_list: list[ProcessedDataCollection] = []

      for data_point in data_info.data_column_list:
            collection = ProcessedDataCollection(
               data_point.data_category,
               data_point.data_name,
            )

   def extract_data(self,scrapper: IbgePibCidadesScrapper):
      list_datapoints:list[YearDataPoint] = scrapper.extract_database()
      parsed_datapoints:list[YearDataPoint] = self.__drop_cols(list_datapoints)
      concatenated_df:pd.DataFrame = self._concat_data_points(parsed_datapoints,add_year_col=False)
      concatenated_df.to_csv("concat.csv")
      time_series_years:list[int] = YearDataPoint.get_years_from_list(list_datapoints)

      processed_data_list:list[ProcessedDataCollection] = []
      for data_point in CitiesGDPDataInfo:
         collection = self.__get_processed_collection(concatenated_df,data_point,time_series_years)
         processed_data_list.append(
            collection
         )
      
      return processed_data_list

   def extract_data_points(self, df: pd.DataFrame, data_info: TableDataPoints)->pd.DataFrame:
      """ 
      Extrai o dataframe bruto da base em um df menor apenas com as colunas de ano, código de cidade e os pontos de dados buscados
      
      Args:
         df (dataframe) : dataframe bruto da base de dados
         data_info (TableDataPoints) : Classe para especificar onde e como os dados buscados estão no df bruto
      
      Return:
         (dataframe): dataframe com as colunas: ano, código_muni, dado_identificador, valor e tipo_dado. Esse formato será similar aos 
         dados em cada tabela de categoria
      """

      if len(data_info.data_column_list) < 1:
         raise IOError("Lista de ponto de dados deve conter pelo menos 1 ponto de dado")
      
      df = self.update_city_code(df,data_info.city_code_column) #checa se o código dos municípios está correto
      df.columns = self.__parse_df_col_names(df.columns)

      final_df:pd.DataFrame = pd.DataFrame() #df final a ser retornado
      city_code_col:str = self.parse_strings(data_info.city_code_column) #parsing nos nomes para removes espaços e colocar em lowercase 
      year_col:str =  self.parse_strings(data_info.year_column_name)
      
      for point in data_info.data_column_list: #loop pela lista de colunas com os dados
         temp_df:pd.DataFrame = pd.DataFrame() #cria df temporário
         data_val_col:str =  self.parse_strings(point.column_name)

         temp_df[self.CITY_CODE_COL] = df[city_code_col].copy() #copia as colunas de codigo de cidade e ano
         temp_df[self.YEAR_COLUMN] = df[year_col].copy()
         temp_df[self.DATA_IDENTIFIER_COLUMN] = point.data_name #coluna de identificador do dado recebe o nome do dado
         temp_df[self.DATA_VALUE_COLUMN] = df[data_val_col].apply(point.multiply_value) #preenche a coluna de valor de dados
         temp_df[self.DTYPE_COLUMN] = point.data_type.value #coluna de tipo de dados

         final_df = pd.concat(objs=[final_df,temp_df], axis='index', ignore_index=True) #concatena as linhas do df temp no df total

      final_df[self.YEAR_COLUMN] = final_df[self.YEAR_COLUMN].astype("category") # a coluna de ano e de tipo de dado é transformado numa categoria, o que economiza memória
      final_df[self.DTYPE_COLUMN] = final_df[self.DTYPE_COLUMN].astype("category")
      final_df[self.DATA_IDENTIFIER_COLUMN] = final_df[self.DATA_IDENTIFIER_COLUMN].astype("category")
      
      return final_df

   def __parse_df_col_names(self,columns: list[str])->list[str]:
      """
      Recebe uma lista com o nome das colunas do df e retorna essa lista com os nomes processados.
      O processamento se dá por remover os espaços em branco e \ n, já que algumas bases tem nomes de colunas complexos e 
      longos que os tornam difíceis de acessar.

      Args:
         columns (list[int]): lista de strings que representam a lista de colunas do dataframe

      Return:
         (list[int]): lista de strings processadas 
      
      """
      new_cols = list(map(self.parse_strings,columns))
      return new_cols

   def __change_col_dtypes(self,df:pd.DataFrame)->pd.DataFrame:
      pass

   def __drop_cols(self,list_datapoints:list[YearDataPoint])->list[YearDataPoint]:
      """
      Dado uma lista de YearDataPoints, remove as colunas que não são o código da cidade e o nome dos dados buscados
      """

      columns_to_keep:list[str] = list(item.value["column_name"] for item in CitiesGDPDataInfo) #cria um conjunto com os nomes das colunas de todos os membros do enum      
      columns_to_keep.append(self.EXTRACTED_DATA_CITY_CODE_COL) #add a coluna dos códigos dos municípios
      columns_to_keep.append(self.EXTRACTED_DATA_YEAR_COL) #add a coluna do ano dos dados
      columns_to_keep = list(map(self.parse_strings,columns_to_keep)) #da parsing nas strings dos dados

      parsed_data_points:list[YearDataPoint] = []
      for datapoint in list_datapoints:
         df = datapoint.df
         cols:list[str] = df.columns
         cols:list[str] = list(map(self.parse_strings,cols)) #parsing no nome das colunas e transforma num conjunto
         df.columns = cols #df agora tem as colunas com nomes tratados

         cols_to_drop:list[str] = [x for x in cols if x not in columns_to_keep] #diferença de conjuntos entre todas as colunas e as que vão ficar
         new_df:pd.DataFrame = df.drop(columns=cols_to_drop) 

         new_df = new_df.rename( #renomea os nomes das colunas do df com a função de dar parsing em strings
            mapper = self.parse_strings,
            axis="columns"
         )
         parsed_city_code_col:str = self.parse_strings(self.EXTRACTED_DATA_CITY_CODE_COL) #parsing no nome da coluna de códigos do município original
         parsed_year_col:str = self.parse_strings(self.EXTRACTED_DATA_YEAR_COL)

         new_df = new_df.rename(
            {parsed_city_code_col: self.CITY_CODE_COL, parsed_year_col:self.YEAR_COLUMN},
            axis="columns"
         ) #muda o nome da coluna de código dos municípios e do ano dos dados

         new_df[self.CITY_CODE_COL] = new_df[self.CITY_CODE_COL].astype("int")
         new_df[self.YEAR_COLUMN] = new_df[self.YEAR_COLUMN].astype("category")

         print(new_df.columns)
         parsed_data_points.append(
            YearDataPoint(new_df,datapoint.data_year)
         )   
         new_df.to_csv("teste.csv")

      
      return parsed_data_points

   def __get_processed_collection(self,df:pd.DataFrame, data_info:CitiesGDPDataInfo,time_series_years:list[int])->ProcessedDataCollection:
      new_df = pd.DataFrame()

      new_df[self.CITY_CODE_COL] = df[self.CITY_CODE_COL] #copia coluna de código do município
      new_df[self.YEAR_COLUMN] = df[self.YEAR_COLUMN] #copia coluna do ano

      data_values_col:str =  self.parse_strings(data_info.value["column_name"]) #nome do dado com parsing
      print(data_values_col)
      multiply_amount:int = data_info.value["multiply_amount"] #quantas vezes o dado deve ser multiplicado para atingir o valor não truncado
      new_df[self.DATA_VALUE_COLUMN] = df[data_values_col].apply(lambda x: x*multiply_amount) #cria uma coluna do valor dos dados, multiplicando ele caso seja necessário

      new_df[self.DATA_IDENTIFIER_COLUMN] = data_info.value["data_name"] #coluna do nome final do dado
      new_df[self.DTYPE_COLUMN] = data_info.value["dtype"].value #coluna do tipo de dado

      print(new_df.info())
      return ProcessedDataCollection(
         category=data_info.value["data_category"],
         dtype=data_info.value["dtype"],
         data_name=data_info.value["data_name"],
         time_series_years=time_series_years,
         df= new_df
      )

      

if __name__ == "__main__":


   pib_percapita = {
      "data_category": "caracterizacao_socio_economica",
      "data_name": "PIB per capita",
      "column_name": """Produto Interno Bruto per capita, 
   a preços correntes
   (R$ 1,00)""",
      "data_type": DataTypes.FLOAT,
      "multiply_amount": 1
   }

   pib_agro = {
      "data_category": "caracterizacao_socio_economica",
      "data_name": "PIB Agropecuária",
      "column_name": """Valor adicionado bruto da Agropecuária, 
   a preços correntes
   (R$ 1.000)""",
      "data_type": DataTypes.FLOAT,
      "multiply_amount": 1000
   }
   
   df:pd.DataFrame = pd.read_excel(os.path.join("webscrapping","tempfiles","PIB dos Municípios - base de dados 2010-2021.xlsx"))
   df_info1 = TableDataPoints("Ano","Código do Município")
   df_info1.add_data_points_dicts([pib_agro])

  # df_info2 = TableDataPoints("Ano","Código do Município")
   df_info1.add_data_points_dicts([pib_percapita])

   url2 =  "https://www.ibge.gov.br/estatisticas/economicas/contas-nacionais/9088-produto-interno-bruto-dos-municipios.html?=&t=downloads"

   scrapper = IbgePibCidadesScrapper(url2,BaseFileType.EXCEL,True)
   extractor = CategoryDataExtractor()

   df1 = extractor.extract_data_points(scrapper,df_info1)
   print(df1.head(5))
   df1.to_csv("teste.csv")
   





