import pandas as pd
from etl_config import get_config, get_current_year
import pandera as pa 
from datastructures import DataTypes

"""
Classe que representa o dado que vai ser carregado no BD/Data Warehouse, com o DF sendo os dados em si e com os outros
campos da classe sendo metadadas como a categoria, nome do dado e os anos da série histórica.
"""


class ProcessedDataCollection:
   """
   Essa classe representa um conjunto de um dado específico pronta para ser inserida no banco de dados

   Ela contém a categoria (tabela) dos dados, o nome dos dados, a lista de anos da série histórica e os
   dados na forma de DF do pandas
   
   """
   category:str
   dtype: DataTypes
   data_name:str
   time_series_years:list[int]
   df: pd.DataFrame


   #constante de classe para o schema dos dataframes. Força as colunas do dataframe a serem de um certo tipo de dado e seguirem certas regras de valores
   DF_SCHEMA = pa.DataFrameSchema(
          columns={
            get_config("YEAR_COL"): pa.Column(int,pa.Check(lambda x : x <= get_current_year() and x >= get_config("OLDEST_YEAR") ,element_wise=True)), #valores da coluna de ano devem estrar entre um numero do ano mais antigo e o ano atual
            get_config("CITY_CODE_COL"): pa.Column(int, pa.Check(lambda x: x >= get_config("SMALLEST_CITY_CODE") and x <= get_config("HIGHEST_CITY_CODE"),element_wise=True)), #numero de município deve estar entre o range permitido para 7 dígitos
            get_config("DATA_IDENTIFIER_COL"): pa.Column(str),
            get_config("DTYPE_COL"):pa.Column(str,pa.Check(lambda x: x in [y.value for y in DataTypes],element_wise=True)), #valor da coluna de tipos de dados deve pertencer aos valores do enum DataTypes
            get_config("DATA_VALUE_COL"): pa.Column() #tipo da coluna de dados não é padronizado
          },
          strict=True,
          index=pa.Index(int) #index numérico
    )

    
   def __init__(self, category: str, dtype: DataTypes, data_name: str, time_series_years: list[int], df: pd.DataFrame) -> None:
        df = self.DF_SCHEMA.validate(df) #valida o schema do df passado como argumento
        self.category = category
        self.dtype = dtype
        self.data_name = data_name
        self.time_series_years = time_series_years
        self.df = df

   def __str__(self) -> str:
      return   f"""
            Nome do dado: {self.data_name},\n
            Tópico do dado: {self.category},\n
            Anos da série histórica: {self.time_series_years},\n
            Tipo de dado: {self.dtype.value} \n
            """
       
