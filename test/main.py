from webscrapping.extractorclasses import *
from apiextractors import *
import inspect,json,os
from extractors_classes_handler import ExtractorClassesHandler

ALL_IMPORTED_CLASSES = [object_name for object_name in dir() if inspect.isclass(globals()[object_name])] #lista todas as classes importadas no escopo global do Python

def get_all_extractor_classes()->list[str]:
   return list(filter(lambda x: "extractor" in x.lower() and "abstract" not in x.lower(),ALL_IMPORTED_CLASSES))

def get_all_api_classes()->list[str]:
   return list(filter(lambda x: "api" in x.lower() and "abstract" not in x.lower(),ALL_IMPORTED_CLASSES))

def run_requested_extractions(sources_to_extract:dict[str,list[str]]):
   """
   dado uma lista de fontes para extrair, com essa lista vindo de acordo com a tabela de controle de dados extraídos 
   (ver a coluna ' Extraído Pela Classe:' dessa tabela), realiza a extração dos dados requisitados
   """
   #print(get_all_extractor_classes())
   #print(get_all_api_classes())

   all_classes:list[str]  = get_all_extractor_classes()
   all_classes.extend(get_all_api_classes())

   etl_classes_map:dict[str,object] = { #dict com o nome da classe (em lowercase) como string key e a classe em si como valor
      class_name.lower().replace(" ",""): globals()[class_name] for class_name in all_classes
   }

   for source in sources_to_extract:
      source_parsed:str = source.lower().replace(" ","")#variável para guardar o nome da classe em lowercase e sem espaço
      extractor_class = etl_classes_map.get(source_parsed)
      if  extractor_class is not None:
         indicators:list[str] = sources_to_extract[source]
         print(indicators)
         print(extractor_class)
         print(source)
      else:
         pass


sources: dict 
with open(os.path.join(os.getcwd(),"sources_to_extract.json"),"rb") as f:
   sources:dict =  json.load(f)

handler = ExtractorClassesHandler()
handler.run_requested_extractions(sources)
#run_requested_extractions(sources)