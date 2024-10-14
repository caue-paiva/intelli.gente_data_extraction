from webscrapping.extractorclasses import *
from apiextractors import *
import pandas as pd
import inspect

ALL_IMPORTED_CLASSES = [object_name for object_name in dir() if inspect.isclass(globals()[object_name])] #lista todas as classes importadas no escopo global do Python

def get_all_extractor_classes()->list[str]:
   return list(filter(lambda x: "extractor" in x.lower() and "abstract" not in x.lower(),ALL_IMPORTED_CLASSES))

def get_all_api_classes()->list[str]:
   return list(filter(lambda x: "api" in x.lower() and "abstract" not in x.lower(),ALL_IMPORTED_CLASSES))

def run_requested_extractions(sources_to_extract:list[str]):
   """
   dado uma lista de fontes para extrair, com essa lista vindo de acordo com a tabela de controle de dados extraídos 
   (ver a coluna ' Extraído Pela Classe:' dessa tabela), realiza a extração dos dados requisitados
   """
   #print(get_all_extractor_classes())
   #print(get_all_api_classes())

   all_classes:list[str]  = get_all_extractor_classes()
   all_classes.extend(get_all_api_classes())

   etl_classes_map:dict[str,object] = { #dict com o nome da classe (em lowercase) como string key e a classe em si como valor
      class_name.lower(): globals()[class_name] for class_name in all_classes
   }

   print("\n\n\n")
   for source in sources_to_extract:
      source = source.lower()
      if etl_classes_map.get(source) is not None:
         etl_classes_map[source]
      else:
         pass





run_requested_extractions({})