import os,json
from typing import Any

__SCRIPT_DIR:str = os.path.dirname(os.path.abspath(__file__))
__CONFIG_PATH: str = os.path.join(__SCRIPT_DIR,"config.json")

__config_dict: dict
try:
   with open(__CONFIG_PATH, "r") as f:
      __config_dict: dict = json.load(f)
except:
   raise RuntimeError("Não foi possível abrir o arquivo de configuração do projeto (config.json)")

#função para deixar o usuário apenas acessar o valor de uma key de configuração
def get_config(config_name:str)-> Any:
    val:Any = __config_dict.get(config_name)
    if val:
       return val
    else:
       raise RuntimeError("Não foi possível acessar uma configuração com esse nome")

