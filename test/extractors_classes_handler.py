from etl_config import get_config
from webscrapping.extractorclasses import *
from apiextractors import *
from webscrapping.extractorclasses import AbstractDataExtractor
from apiextractors.apiclasses import AbstractApiInterface
from datastructures import ProcessedDataCollection
from webscrapping.scrapperclasses.DatasusLinkScrapper import DatasusDataInfo
from typing import Type
import inspect,json,os


ALL_IMPORTED_CLASSES = [object_name for object_name in dir() if inspect.isclass(globals()[object_name])] #lista todas as classes importadas no escopo global do Python


class ExtractorClassesHandler:

   __etl_classes_map: dict[str,object]

   def __init__(self):
      all_classes:list[str]  = self.__get_all_extractor_classes()
      all_classes.extend(self.__get_all_api_classes())

      self.__etl_classes_map = { #dict com o nome da classe (em lowercase) como string key e a classe em si como valor
         class_name.lower().replace(" ",""): globals()[class_name] for class_name in all_classes
      }

   def __parse_str(self,string:str)->str:
      return string.lower().replace(" ","")

   def __get_all_extractor_classes(self)->list[str]:
      return list(filter(lambda x: "extractor" in x.lower() and "abstract" not in x.lower(),ALL_IMPORTED_CLASSES))

   def __get_all_api_classes(self)->list[str]:
      return list(filter(lambda x: "api" in x.lower() and "abstract" not in x.lower(),ALL_IMPORTED_CLASSES))

   def __extract_datasus_specific_indicators(self,indicators_to_extract:list[str])->list[ProcessedDataCollection]:
      data_points:list[DatasusDataInfo] = []

      for data_point in DatasusDataInfo:
         if self.__parse_str(data_point.name) in indicators_to_extract:
            data_points.append(data_point)

   def __run_requested_extraction(
            self,
            class_name:str, 
            class_obj: Type[AbstractApiInterface] | Type[AbstractDataExtractor], 
            indicators_to_extract:list[str] = []
         ) -> list[ProcessedDataCollection]:
         """
         
         """
         
         if indicators_to_extract: 
            print("caso especial")
            #Caso de Classes como DatasusExtractor que extraem vários indicadores e podem extrair
            #indicadores específicos apenas, com esses sendo passados numa lista no JSON de indicadores a extrair
            classes_with_several_indicators: list[str] = get_config("CLASSES_WITH_SEVERAL_INDICATORS")
            classes_with_several_indicators = list(
               map(lambda x: x.lower().replace(" ",""),classes_with_several_indicators)
            ) #parsing na lista para ficar minusculo e tirar espaço

            if class_name.lower().replace(" ","") not in classes_with_several_indicators:
               raise RuntimeError(
                  """
                  Função teve uma lista de indicadores específicos para extrair porem a classe extratora 
                  não suporta a especificação de indicadores específicos
                  """
               )
            if class_name == "DatasusDataExtractor":
               print("datasus")
               return self.__extract_datasus_specific_indicators(indicators_to_extract)
         else:
            return class_obj().extract_processed_collection()

   def run_requested_extractions(self,sources_to_extract:dict[str,list[str]]):
      """
      dado uma lista de fontes para extrair, com essa lista vindo de acordo com a tabela de controle de dados extraídos 
      (ver a coluna ' Extraído Pela Classe:' dessa tabela), realiza a extração dos dados requisitados
      """

      for source in sources_to_extract:
         source_parsed:str = source.lower().replace(" ","")#variável para guardar o nome da classe em lowercase e sem espaço
         extractor_class = self.__etl_classes_map.get(source_parsed)
         if extractor_class is not None:
            indicators:list[str] = sources_to_extract[source]
            list_ = self.__run_requested_extraction(
               source,
               extractor_class,
               indicators
            )
            print(list_[0].df.info())


         else:
            pass

   