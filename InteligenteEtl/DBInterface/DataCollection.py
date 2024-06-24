import pandas as pd
from dataclasses import dataclass
from config import get_config
from CityDataInfo import get_city_codes
from DataEnums import DataTypes

"""
Classe que representa o dado que vai ser carregado no BD/Data Warehouse, com o DF sendo os dados em si e com os outros
campos da classe sendo metadadas como a categoria, nome do dado e os anos da série histórica.
"""


#dataclass cria automaticamente métodos __init__, __eq__ entre outros, além de que mostra que o propósito principal da classe é guardar dados
@dataclass
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


   def fill_non_existing_cities(self)->'ProcessedDataCollection':
       """
       Pega a coleção de dados processados já existentes e retorna uma nova coleção, colocando valores nulos para todas as combinações de cidades e anos que estão faltando
       no DF da coleção. O resultado é uma coleção cujo DF tem o número de linhas múltiplo do número de municípios no brasil.

       Args:
            (None)
    
       Return:
            (ProcessedDataCollection): Coleção nova cujo DF é o df já existente porém com combinações de cidades e anos faltando colocadas como NULL
       
       """

       year_col :str = get_config("YEAR_COL")
       city_code_col: str = get_config("CITY_CODE_COL")
       data_identifier_col:str = get_config("DATA_IDENTIFIER_COL")
       dtype_col:str = get_config("DTYPE_COL")
       values_col: str = get_config("DATA_VALUE_COL")

       years:list[int] = self.df[year_col].unique() #lista de anos no df
       city_codes:list[int] = get_city_codes() #lista de municipios no brasil

       complete_index = pd.MultiIndex.from_product([city_codes, years], names=[city_code_col, year_col]) #index vindo de um produto cartesiano dos municipios e anos
       complete_df = pd.DataFrame(index=complete_index).reset_index() #cria um df com esse index e as colunas de ano e cidades apenas

     
     
       merged_df:pd.DataFrame = pd.merge(complete_df, self.df, on=[city_code_col, year_col], how='left') #left join (df_cartesiano X df_antigo) para preencher todasas possíveis combinações
    
       merged_df[data_identifier_col] = self.data_name #coluna de nome do dado vai ser preenchida novamente
       merged_df[dtype_col] = merged_df[dtype_col].fillna(DataTypes.NULL.value) #preenche valores NaN na coluna de tipos de dados com a string NULL
       merged_df[values_col] = merged_df[values_col].fillna(DataTypes.NULL.value) #preenche valores NaN na coluna de valores de dados com a string NULL
    
       return ProcessedDataCollection( #retorna nova coleção de dados
           category=self.category,
           dtype=self.dtype,
           data_name=self.data_name,
           time_series_years=self.time_series_years,
           df= merged_df
       )
       
