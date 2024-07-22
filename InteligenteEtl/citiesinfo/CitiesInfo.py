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
