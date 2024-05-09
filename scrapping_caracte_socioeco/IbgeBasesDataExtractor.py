import pandas as pd
import os
from DataPointsInfo import DataPointsInfo, DataPointTypes



"""
Existem cidades com nomes duplicados, vamos ter que usar uma nova chave para identificar elas, provavelmente o id de muncípios do ibge
talvez popular a tabela de município com isso como primary key

"""



class CategoryDataExtractor():

   CITY_COLUMN = "municipio"
   YEAR_COLUMN = "ano"

   category:str

   def __init__(self, category:str) -> None:
      self.category = category

   def extract_data_points(self, df:pd.DataFrame, data_info: DataPointsInfo)->pd.DataFrame:
      """extrai o dataframe bruto da base em um df menor apenas com os pontos de dados buscados"""

      new_df:pd.DataFrame = pd.DataFrame()

      new_df[self.CITY_COLUMN] = df[data_info.city_column_name].copy()
      new_df[self.YEAR_COLUMN] = df[data_info.year_column_name].copy()
      #new_df["dado_nome"] = data_info.

      for point in data_info.data_point_list:
         new_df[point.data_name] = df[point.column_name].apply(point.multiply_value)

      new_df[self.YEAR_COLUMN] = new_df[self.YEAR_COLUMN].astype("category") # a coluna de ano é transformado numa categoria, o que economiza memória
      return new_df

   def join_category_df(self, df_list:list[pd.DataFrame])->pd.DataFrame:
      """TODO 
      Essa função deve juntar todos os dados de uma categoria para depois ser colocada no Banco de Dados
      
      
      """
      if len(df_list) < 2:
         raise IOError("Esse função deve ser provida com uma lista de pelo menos 2 Dataframes")

      initial_df: pd.DataFrame = df_list[0]

      for i in range(1,len(df_list)):
         current_df:pd.DataFrame = df_list[i]
         initial_df = initial_df.merge(current_df,how="inner",on=["ano","municipio"]) #outer join nas colunas de município e ano

      return initial_df


df:pd.DataFrame = pd.read_excel(os.path.join("tempfiles","PIB dos Municípios - base de dados 2010-2021.xlsx"))

pib_percapita = {
   "data_name": "PIB per capita",
   "column_name": """Produto Interno Bruto per capita, 
a preços correntes
(R$ 1,00)""",
   "data_type": DataPointTypes.FLOAT,
   "multiply_amount": 1
}

pib_agro = {
   "data_name": "PIB Agropecuária",
   "column_name": """Valor adicionado bruto da Agropecuária, 
a preços correntes
(R$ 1.000)""",
   "data_type": DataPointTypes.FLOAT,
   "multiply_amount": 1000
}


df_info1 = DataPointsInfo("Ano","Nome do Município")
df_info1.add_data_points_dicts([pib_agro])

df_info2 = DataPointsInfo("Ano","Nome do Município")
df_info2.add_data_points_dicts([pib_percapita])

extractor = CategoryDataExtractor("teste")

df1 = extractor.extract_data_points(df,df_info1)
df2 = extractor.extract_data_points(df,df_info2)
duplicates_1 = df1[df1.duplicated(subset=['ano', 'municipio'], keep=False)]
duplicates_2 = df2[df2.duplicated(subset=['ano', 'municipio'], keep=False)]

print(f"Duplicate rows in initial_df:\n{duplicates_1}")
print(f"Duplicate rows in current_df:\n{duplicates_2}")


df3 = extractor.join_category_df([df1,df2])

#print(df3.head(5))
#print(df3.info())





