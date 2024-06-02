import os,json
from typing import Any


__CONFIG_PATH: str = os.path.join("config.json")
__config_dict: dict
try:
   with open(__CONFIG_PATH, "r") as f:
      __config_dict: dict = json.load(f)
except:
   raise RuntimeError("Não foi possível abrir o arquivo de configuração do projeto (config.json)")

def get_config(config_name:str)-> Any:
    val:Any = __config_dict.get(config_name)
    if val:
       return val
    else:
       raise RuntimeError("Não foi possível acessar uma configuração com esse nome")

