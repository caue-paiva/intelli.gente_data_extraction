import pandas as pd
import os
from WebScrapping.ExtractorClasses import TableDataPoints
from WebScrapping.ExtractorClasses import AbstractDataExtractor
from WebScrapping.ScrapperClasses import IbgePibCidadesScrapper
from typing import Type
from DBInterface import ProcessedDataCollection
from DataEnums import DataTypes, BaseFileType


"""
Existem cidades com nomes duplicados, vai ser usado o código de município para identificar esses municípios, porém bases antigas como o datasus 2010 usam
um código quase identico porém com 6 dígitos e não 7, isso deve ser levado em conta no ETL, Webscrapping

"""



class CategoryDataExtractor(AbstractDataExtractor):

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
      
      df = self.check_city_code(df,data_info.city_code_column) #checa se o código dos municípios está correto
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

   def join_category_dfs(self, df_list:list[pd.DataFrame])->pd.DataFrame:
      """TODO 
      Essa função deve juntar todos os dados de uma categoria para depois ser colocada no Banco de Dados
      
      
      """
      if len(df_list) < 2:
         raise IOError("Lista de dataframes deve conter pelo menos 2 dfs")
      
      final_df:pd.DataFrame = df_list[0] #pega uma view do primeiro df. OBS: nenhum cópia pe feita, apenas diferentes views do mesmo DF
      
      for i in range(1,len(df_list)): #loop pela lista de dataframes
         final_df = pd.concat(objs=[final_df,df_list[i]], axis='index', ignore_index=True) #concatena as linhas do df temp no df total
      
      final_df[self.DATA_IDENTIFIER_COLUMN] = final_df[self.DATA_IDENTIFIER_COLUMN].astype("category") #por motivo de como o concat funciona, essa coluna precisa ser colocado como categoria dnv, pq
      return final_df

   





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
   





