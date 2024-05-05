import pandas as pd
import os
from AbstractDataExtractor import DataFrameInfo, DataPoint, DataPointTypes

CITY_COLUMN = "municipio"
YEAR_COLUMN = "ano"

def extract_data_points(df:pd.DataFrame, data_info: DataFrameInfo)->pd.DataFrame:
   new_df:pd.DataFrame = pd.DataFrame()

   new_df[CITY_COLUMN] = df[data_info.city_column_name].copy()
   new_df[YEAR_COLUMN] = df[data_info.year_column_name].copy()

   for point in data_info.data_point_list:
      new_df[point.stardard_name] = df[point.column_name].apply(point.multiply_value)

   return new_df

class IbgeBasesDataExtractor():
   def __init__(self) -> None:
      pass

df:pd.DataFrame = pd.read_excel(os.path.join("tempfiles","PIB dos Municípios - base de dados 2010-2021.xlsx"))

pib_percapita = {
   "standard_name": "PIB per capita",
   "column_name": """Produto Interno Bruto per capita, 
a preços correntes
(R$ 1,00)""",
   "data_type": DataPointTypes.FLOAT,
   "multiply_amount": 1
}

pib_agro = {
   "standard_name": "PIB Agropecuária",
   "column_name": """Valor adicionado bruto da Agropecuária, 
a preços correntes
(R$ 1.000)""",
   "data_type": DataPointTypes.FLOAT,
   "multiply_amount": 1000
}

data_point_list = [DataPoint(pib_percapita),DataPoint(pib_agro)]
df_info = DataFrameInfo("Ano","Nome do Município",data_point_list)

df2 = extract_data_points(df,df_info)

print(df2.head(5))




