import json, os

__SCRIPT_DIR:str = os.path.dirname(os.path.abspath(__file__))

def get_ibge_api_datamap()->dict:
   try:
      with open(os.path.join(__SCRIPT_DIR,"IbgeAgregatesApiDataMap.json"), "r") as f:
               loaded_data = json.load(f)
               if not isinstance(loaded_data,dict):
                  raise IOError("objeto json não está na forma de um dicionário do python")
               return loaded_data
   except Exception as e:
         raise RuntimeError(f"Não foi possível carregar o JSON que mapea os dados do DB para a API do IBGE, erro: {e}")

def get_anatel_api_datamap()->dict:
   try:
      with open(os.path.join(__SCRIPT_DIR,"AnatelApiDataMap.json"), "r") as f:
               loaded_data = json.load(f)
               if not isinstance(loaded_data,dict):
                  raise IOError("objeto json não está na forma de um dicionário do python")
               return loaded_data
   except Exception as e:
         raise RuntimeError(f"Não foi possível carregar o JSON que mapea os dados do DB para a API da Anatel, erro: {e}")