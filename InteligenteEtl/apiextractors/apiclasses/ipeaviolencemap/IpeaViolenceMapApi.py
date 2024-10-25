from apiextractors.apiclasses.AbstractApiInterface import AbstractApiInterface
from apiextractors.apidataclasses import DataLine, RawDataCollection
from datastructures.DataCollection import ProcessedDataCollection
from datastructures import DataTypes
import requests, os, json

class IpeaViolenceMapApi(AbstractApiInterface):
   """
   Classe para extrair dados da API do Ipea, 
   Documentação da API: https://www.ipea.gov.br/atlasviolencia/api 
   """

   BASE_URL = "https://www.ipea.gov.br/atlasviolencia/" #url básico da api
   CITY_LEVEL_DATA_CODE = "4" #na api do ipea, o código 4 se refere a dados com abrangência no nível de município

   _data_map: dict[str, dict[str,dict]] #json que indica os parâmetros e outras informações sobre cada dado da api

   def __init__(self,path_to_datamap:str="")->None:
      if not path_to_datamap:
         __CURRENT_DIR = os.path.dirname(os.path.abspath(__file__)) #path para o json que mapea os dados extraidos com parâmetros de chamada da API
         api_referen_json_path: str = os.path.join(__CURRENT_DIR,"IpeaViolenceApiDataMap.json")
         self._db_to_api_data_map(api_referen_json_path)
      else:
         self._db_to_api_data_map(path_to_datamap)
           
   def _db_to_api_data_map(self, datamap_path:str)->None:
      with open(datamap_path,"r") as f:
         file_content= json.load(f)
      
      if not isinstance(file_content,dict):
         raise RuntimeError("Arquivo JSON de Datamap não é lido como um dicionário python")
      self._data_map = file_content #atributo do objeto

   def __get_api_response(self,data_series_id:int)->list[dict]:
      """
      Faz uma request à API do IPEA atlas da violência dado um id de uma série histórica passada como argumento.

      Args:
         data_series_id (int) : id da série histórica buscada
      
      Return:
         (list[dict]): retorno da API, consiste em uma lista de dicionários, cada dict tem as chaves: (id,periodo,valor,cod)
      """
      
      SERIES_URL = f"api/v1/valores-series/{data_series_id}/{self.CITY_LEVEL_DATA_CODE}" #20 é o id do tema de taxa de homicídios e 4 é a abrangencia dos dados para cada município
      path = self.BASE_URL + SERIES_URL
      response = requests.get(path)
      if response.status_code != 200:
            raise RuntimeError(f"Falha na request, erro: {response.status_code}")
      response.encoding = 'utf-8'
      return response.json()
   
   def __parse_api_response(self,response:list[dict], dtype:DataTypes)->list[DataLine]:
      """
      Transforma a resposta da API numa lista de objetos DataLine
      """
      filter_null_vals = lambda x: x["valor"] != "0"
      filtered_response:list[dict] = list(filter(filter_null_vals,response))

      parse_dates_to_years = lambda x : x[:x.find("-")] #função que transforma a string YYYY-MM-DD em uma string YYYY
      dict_to_dataline = lambda x: DataLine(
          city_id=int(x["cod"]),
          year=int(parse_dates_to_years(x["periodo"])),
          value= x["valor"],
          data_type=dtype
      )
      return list(map(dict_to_dataline,filtered_response))

   def __get_time_series_years(self,data_points:list[DataLine])->list[int]:
      years = set(map(lambda x: x.year,data_points))
      return list(years).sort() #transforma o conjunto numa lista e ordena ele

   def extract_processed_collection(self) -> list[ProcessedDataCollection]:
      raw_data_list:list[ProcessedDataCollection] = []

      for category, data_points in self._data_map.items(): #loop pelas categorias do json de mapeamento dos dados
         for data_point, attributes in data_points.items():
            id: int = int(attributes["id"])
            dtype: DataTypes = DataTypes.from_string(attributes["dtype"])
            
            api_response:list[dict] = self.__get_api_response(id)
            parsed_response:list[DataLine] = self.__parse_api_response(
               response=api_response,
               dtype=dtype
            )

            data_collection = RawDataCollection(
               category = category,
               data_name=data_point,
               data_type=dtype,
               time_series_years= self.__get_time_series_years(parsed_response),
               data_lines=parsed_response
            )
            raw_data_list.append(data_collection)
      
      return super().process_raw_data(raw_data_list)
            