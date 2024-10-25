from etl_config import get_config, ClassExtractionLog,DataPointExtractionLog
from webscrapping.extractorclasses import *
from apiextractors import *
from webscrapping.extractorclasses import AbstractDataExtractor
from apiextractors.apiclasses import AbstractApiInterface
from datastructures import ProcessedDataCollection
from webscrapping.scrapperclasses.DatasusLinkScrapper import DatasusDataInfo
from webscrapping.extractorclasses import DatasusDataExtractor
from typing import Type
import inspect
from datetime import datetime,timedelta


ALL_IMPORTED_CLASSES = [object_name for object_name in dir() if inspect.isclass(globals()[object_name])] #lista todas as classes importadas no escopo global do Python


class ExtractorClassesHandler:

   __etl_classes_map: dict[str,object] #dict que mapea o nome de cada classe a seu objeto python
   __classes_with_several_indicators: list[str] #lista de classes que extraem vários indicadores

   def __init__(self):
      all_classes:list[str]  = self.__get_all_extractor_classes()
      all_classes.extend(self.__get_all_api_classes())

      self.__etl_classes_map = { #dict com o nome da classe (em lowercase) como string key e a classe em si como valor
         class_name.lower().replace(" ",""): globals()[class_name] for class_name in all_classes
      }

      #acha todas as classes que extraem vários indicadores 
      classes_with_several_indicators: list[str] = get_config("CLASSES_WITH_SEVERAL_INDICATORS")
      self.__classes_with_several_indicators = list(
               map(lambda x: x.lower().replace(" ",""),classes_with_several_indicators)
      ) #parsing na lista para ficar minusculo e tirar espaço

   def __parse_str(self,string:str)->str:
      return string.lower().replace(" ","")

   def __extractor_class_is_valid(self,class_name:str)->bool:
      parsed_name:str = class_name.lower()
      if "abstract" in parsed_name or "quebrado" in parsed_name:
         return False
      if "extractor" not in parsed_name:
         return False
      return True

   def __get_all_extractor_classes(self)->list[str]:
      return list(filter(self.__extractor_class_is_valid,ALL_IMPORTED_CLASSES))

   def __get_all_api_classes(self)->list[str]:
      return list(filter(lambda x: "api" in x.lower() and "abstract" not in x.lower(),ALL_IMPORTED_CLASSES))

   def __extract_datasus_indicators(self,indicators_to_extract:list[str])->list[ProcessedDataCollection]:
      """
      Extrai indicadores específicos do Datasus 
      """
      extract_all_indicators:bool = True if not indicators_to_extract else False #caso tenhamos que extrair todos os indicadores
      parsed_indicators_to_extract = list(map(self.__parse_str,indicators_to_extract))

      extractor = DatasusDataExtractor()
      data_collections:list[ProcessedDataCollection] = []

      for data_point in DatasusDataInfo:
         if extract_all_indicators or (self.__parse_str(data_point.value["data_name"]) in parsed_indicators_to_extract): #dado tem que ser extraido
            collection =  extractor.extract_processed_collection(data_point)
            data_collections.extend(collection)
      
      return data_collections

   def __run_requested_extraction(
            self,
            class_name:str, 
            class_obj: Type[AbstractApiInterface] | Type[AbstractDataExtractor], 
            indicators_to_extract:list[str] = []
         ) -> list[ProcessedDataCollection]:
         """
         roda uma classe especificada para extrair os indicadores

         Args
            class_name (str): nome da classe a ser executada
            class_obj (Type[AbstractApiInterface] | Type[AbstractDataExtractor]): subclasse de api ou extratora que vai ter seu método executado
            indicators_to_extract (list[str]): Lista de indicadores específicos a serem extraidos, usados apenas em certas classes como DatasusExtractor
         
         Return:
            list[ProcessedDataCollection]: Dados extraidos
         """

         if class_name.lower().replace(" ","")  in self.__classes_with_several_indicators:
            #caso de classes que extraem vários indicadores
            
            if class_name == "DatasusDataExtractor":
               print("datasus")
               return self.__extract_datasus_indicators(indicators_to_extract)
            else:
               raise NotImplementedError(f"Implementação de extração de indicadores específicos não foi feita para a classe: {class_name}")
         
         else:
            if indicators_to_extract:
               raise RuntimeError(
                  """
                  Função teve uma lista de indicadores específicos para extrair, porém a classe extratora 
                  não suporta a especificação de quais indicadores extrair
                  """
               )

            #caso padrão para classes que implementam apenas um indicador, apenas roda o método de extrair dados 
            return class_obj().extract_processed_collection()

   def run_requested_extractions(self,sources_to_extract:dict[str,list[str]])->list[ClassExtractionLog]:
      """
      dado uma lista de fontes para extrair, com essa lista vindo de acordo com a tabela de controle de dados extraídos 
      (ver a coluna 'Extraído Pela Classe:' dessa tabela), realiza a extração dos dados requisitados.

      Args:
         sources_to_extract (dict[str,list[str]]): dicionário num formato json cuja chave é a classe a ser chamada para extração 
         e o valor é uma lista de indicadores específicos daquela classe (para extrair todos a lista deve ser vazia)
      
      Return:
         list[ClassExtractionLog]: lista de objetos de logging que representam como a extração ocorreu
      """

      logs:list[ClassExtractionLog] = []

      for source in sources_to_extract:
         source_parsed:str = source.lower().replace(" ","")#variável para guardar o nome da classe em lowercase e sem espaço
         extractor_class = self.__etl_classes_map.get(source_parsed)
         
         if extractor_class is not None:
            extraction_start_time = datetime.now()
            indicators:list[str] = sources_to_extract[source]
            list_ = self.__run_requested_extraction(
               source,
               extractor_class,
               indicators
            )
            extraction_time: timedelta = datetime.now() - extraction_start_time

            if "munic" in source_parsed:
               continue

            data_points_extracted:list[DataPointExtractionLog] = []
            for collec in list_:
               collec.df.to_csv(f"{collec.data_name}.csv",index=False)
               data_points_extracted.append(
                  DataPointExtractionLog(
                     data_point=collec.data_name,
                     time_series_years=collec.time_series_years,
                     total_df_lines=collec.df.shape[0]
                  )
               )
            logs.append(
               ClassExtractionLog(
                  class_name=source,
                  data_points_logs=data_points_extracted,
                  finish_date=datetime.now(),
                  extraction_time=extraction_time,
                  extra_info=""
               )
            )
                            
         else:
            print(f"não foi possível extrair os dados com a classe {source}, pois o objeto python relacionado não foi encontrado")
      return logs