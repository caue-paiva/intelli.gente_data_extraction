import pandas as pd
import os
from TableDataPoints import TableDataPoints, DataPointTypes
from AbstractDataExtractor import AbstractDataExtractor


"""
Existem cidades com nomes duplicados, vai ser usado o código de município para identificar esses municípios, porém bases antigas como o datasus 2010 usam
um código quase identico porém com 6 dígitos e não 7, isso deve ser levado em conta no ETL, Webscrapping

"""



class CategoryDataExtractor(AbstractDataExtractor):

   def extract_processed_collection():
      pass

   def extract_data_points(self, df:pd.DataFrame, data_info: TableDataPoints)->pd.DataFrame:
      """ 
      extrai o dataframe bruto da base em um df menor apenas com as colunas de ano, código de cidade e os pontos de dados buscados
      
      Args:
         df (dataframe) : dataframe bruto da base de dados
         data_info (TableDataPoints) : Classe para especificar onde e como os dados buscados estão no df bruto
      
      Return:
         (dataframe): dataframe com as colunas: ano, código_muni, dado_identificador, valor e tipo_dado. Esse formato será similar aos 
         dados em cada tabela de categoria
      """

      if len(data_info.data_point_list) < 1:
         raise IOError("Lista de ponto de dados deve conter pelo menos 1 ponto de dado")
      
      df = self.__check_city_code(df,data_info.city_code_column) #checa se o código dos municípios está correto
      final_df:pd.DataFrame = pd.DataFrame() #df final a ser retornado
      
      for point in data_info.data_point_list: #loop pela lista de pontos de dados
         temp_df:pd.DataFrame = pd.DataFrame() #cria df temporário
      
         temp_df[self.CITY_COLUMN] = df[data_info.city_code_column].copy() #copia as colunas de codigo de cidade e ano
         temp_df[self.YEAR_COLUMN] = df[data_info.year_column_name].copy()
         temp_df[self.DATA_IDENTIFIER_COLUMN] = point.data_name #coluna de identificador do dado recebe o nome do dado
         temp_df[self.DATA_VALUE_COLUMN] = df[point.column_name].apply(point.multiply_value) #preenche a coluna de valor de dados
         temp_df[self.DTYPE_COLUMN] = point.data_type.value #coluna de tipo de dados

         final_df = pd.concat(objs=[final_df,temp_df], axis='index', ignore_index=True) #concatena as linhas do df temp no df total

      final_df[self.YEAR_COLUMN] = final_df[self.YEAR_COLUMN].astype("category") # a coluna de ano e de tipo de dado é transformado numa categoria, o que economiza memória
      final_df[self.DTYPE_COLUMN] = final_df[self.DTYPE_COLUMN].astype("category")
      final_df[self.DATA_IDENTIFIER_COLUMN] = final_df[self.DATA_IDENTIFIER_COLUMN].astype("category")
      
      return final_df

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

   def __check_city_code(self,df:pd.DataFrame, city_code_column:str)->pd.DataFrame:
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

df:pd.DataFrame = pd.read_excel(os.path.join("webscrapping","tempfiles","PIB dos Municípios - base de dados 2010-2021.xlsx"))

pib_percapita = {
   "data_category": "caracterizacao_socio_economica",
   "data_name": "PIB per capita",
   "column_name": """Produto Interno Bruto per capita, 
a preços correntes
(R$ 1,00)""",
   "data_type": DataPointTypes.FLOAT,
   "multiply_amount": 1
}

pib_agro = {
   "data_category": "caracterizacao_socio_economica",
   "data_name": "PIB Agropecuária",
   "column_name": """Valor adicionado bruto da Agropecuária, 
a preços correntes
(R$ 1.000)""",
   "data_type": DataPointTypes.FLOAT,
   "multiply_amount": 1000
}


if __name__ == "__main__":

   df_info1 = TableDataPoints("Ano","Código do Município")
   df_info1.add_data_points_dicts([pib_agro])

   df_info2 = TableDataPoints("Ano","Código do Município")
   df_info2.add_data_points_dicts([pib_percapita])

   extractor = CategoryDataExtractor()

   df1 = extractor.extract_data_points(df,df_info1)
   print(df1.head(5))
   
   df2 = extractor.extract_data_points(df,df_info2)
   print(df2.head(5))

   df3 = extractor.join_category_dfs([df1,df2])

   # print(df3.head(5))
   # print(df3.shape)
   # print(df3.info())






