import pandas as pd
import os
from etl_config import get_config

"""
Módulo para pegar informações sobre os municípios (atualmente 5570) vindos de bases oficiais do IBGE.
Os dados estão reunidos no arquivo "info_municipios_ibge.csv" e são extraidos por meio de funções nesse módulo.
"""

__CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
__CSV_FILE_PATH = os.path.join(__CURRENT_DIR,"info_municipios_ibge.csv")
__CITY_CODE_COL =  get_config("CITY_CODE_COL")
__CITY_NAME_COL = get_config("CITY_NAME_COL")



def get_city_codes()->list[int]:
   """
   Retorna a lista de códigos de todos os municípios
   """
   df:pd.DataFrame = pd.read_csv(__CSV_FILE_PATH)
   list_of_codes: list[int] = list(df[__CITY_CODE_COL].astype("int64"))
   return list_of_codes

def get_city_names()->list[str]:
   """
   Retorna a lista de nomes de todos os municípios
   """
   df:pd.DataFrame = pd.read_csv(__CSV_FILE_PATH)
   list_of_city_names: list[str] = list(df[__CITY_NAME_COL])
   return list_of_city_names

def get_city_codes_names_map(codes_as_keys:bool = False)->dict[str,int]:
   """
   Retorna um dict com o nome de um município como key e o código como o valor por padrão, existe um argumento para trocar isso

   Args:
      codes_as_keys (bool): por padrão falso, retorna o nome do município como key. Se for true retorna o código como chave e o  nome como valor
   """
   list_of_codes:list[int] = get_city_codes()
   list_of_names:list[str] = get_city_names()

   if not codes_as_keys:
      return {name:code for name, code in zip(list_of_names,list_of_codes)}
   else:
      return {code:name for name, code in zip(list_of_names,list_of_codes)}

def get_number_of_cities()->int:
   df:pd.DataFrame = pd.read_csv(__CSV_FILE_PATH)
   return len(df[__CITY_CODE_COL])

def get_city_code_from_string(city_name:str,city_state:str)->int:
   """
   Dado o nome de um município e a sigla do Estado dele, retorna o código do IBGE que representa esse município.

   Args:
      city_name (str): nome do município
      city_state (str): Sigla do estado a qual o município pertence (ex: SP,RS...)

   Return:
      (int): Código do município do IBGE (7 Dígitos) do município
   """
   parse_string = lambda x: x.lower().replace(" ","") #parsing nas strings
   city_name = parse_string(city_name)

   df:pd.DataFrame = pd.read_csv(__CSV_FILE_PATH)

   df = df[df["sigla_uf"] == city_state] #filtra por estado
   df["nome_municipio"] = df["nome_municipio"].apply(parse_string) #parsing na coluna de nome de municípios

   df = df[ df["nome_municipio"] == city_name]

   if df.empty or df.shape[0] > 1:
      return -1
   
   return df["codigo_municipio"].iloc[-1]

def match_city_names_with_codes(df_with_city_names:pd.DataFrame,city_names_col:str,states_col:str)->pd.DataFrame:
   """
   Dado um DF com uma coluna com o nome do município e outra com a sigla do estado do Município, retorna um
   df similar com uma nova coluna que tem os códigos de municípios associados. Municípios cujo código não consiga ser inferido
   são removidos do novo df.

   Args:
      df_with_city_names (pd.DataFrame): df com colunas dos nomes da cidade e da sigla do estado
      city_names_col (str): coluna do df que tem os nomes de cada município
      states_col (str): coluna do df que tem as siglas dos estados de cada município
   
   Return:
      (pd.DataFrame): DataFrame de entrada com uma nova coluna que representa o código do IBGE de cada município
   """
   parse_string = lambda x: x.lower().replace(" ","") #parsing nas strings

   df:pd.DataFrame = pd.read_csv(__CSV_FILE_PATH)
   df["nome_municipio"] = df["nome_municipio"].apply(parse_string) #parsing na coluna de nome de municípios
   df_filtered = df.loc[:,["nome_municipio","sigla_uf","codigo_municipio"]]

   df_with_city_names[city_names_col] = df_with_city_names[city_names_col].apply(parse_string)
   merged = df_with_city_names.merge(df_filtered,how="inner",left_on=[city_names_col,states_col],right_on=["nome_municipio","sigla_uf"])

   merged.to_csv("merged.csv")
   return merged





