import os,json
from typing import Any
from datetime import datetime
from dotenv import load_dotenv
"""
Módulo para padronizar acesso a configurações globais do projeto, definidas num arquivo JSON
"""



__SCRIPT_DIR:str = os.path.dirname(os.path.abspath(__file__))
__CONFIG_PATH: str = os.path.join(__SCRIPT_DIR,"etl_config.json")
__ENV_FILE_PATH:str = os.path.join(__SCRIPT_DIR,"keys.env")

load_dotenv(__ENV_FILE_PATH)

__config_dict: dict
try:
   with open(__CONFIG_PATH, "r") as f:
      __config_dict: dict = json.load(f)
except:
   raise RuntimeError("Não foi possível abrir o arquivo de configuração do projeto (config.json)")

#função para deixar o usuário apenas acessar o valor de uma key de configuração
def get_config(config_name:str)-> Any:
    """
    Função para pegar uma constante do json de configs do projeto, pode retornar uma string ou int

    Args:
      config_name (str): nome da constante de config

   Return:
      (Any): valor da constante de configuração
    """
    val:Any = __config_dict.get(config_name)
    if val:
       return val
    else:
       raise RuntimeError("Não foi possível acessar uma configuração com esse nome")

def get_env_var(env_var_name:str)->str|None:
   return os.getenv(env_var_name)

def get_current_year()->int:
   """
   Retorna o ano atual como int
   """
   return datetime.now().year
