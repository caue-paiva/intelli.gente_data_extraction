from apiextractors.apiclasses.AbstractApiInterface import AbstractApiInterface
import requests,os
import pandas as pd
from datastructures import DataTypes
from datamaps import get_anatel_api_datamap

from datastructures import ProcessedDataCollection

class AnatelApi(AbstractApiInterface):

   _data_map: dict[str, dict[str,dict]] #json que indica os parâmetros e outras informações sobre cada dado da api

   def __init__(self)->None:
      self._db_to_api_data_map() 

   def _db_to_api_data_map(self)->None:
      self._data_map: dict[str, dict[str,dict]]  = get_anatel_api_datamap() #carrega o datamap a partir da função importada

   def __get_zipfile_link(self)->str:
      url = "https://dados.gov.br/dados/api/publico/conjuntos-dados/8db598a8-6a3f-4b80-b9e6-0e31fb9087ea"
      headers = {
         'accept': 'application/json',
         'chave-api-dados-abertos': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJxVWtZVS00T3hwN3Y3V1JzMWhIWk5YRnlJSDhqN0kxUVNwaXE2S0trRWFDVnJrTjFrOXNuSjBRakVoTUZDZXZwVnhQdXJCUXhwRHpCVHNlYyIsImlhdCI6MTcyMzQwODE1OX0.oydu0SUYeVgkjISwr1Syyc0vC7Vn9ZEWggfFKp12rXc'
      }
      response = requests.get(url, headers=headers)
      if response.status_code == 200:
         try:
            data = response.json()
         except requests.exceptions.JSONDecodeError:
            print("Response is not valid JSON.")
      else:
         print(f"Failed to retrieve data. Status code: {response.status_code}")

      return data["recursos"][0]["link"]

   def __get_processed_collection(self,dataframe_parameters:dict[str,str],data_name:str)->ProcessedDataCollection:
      file_name:str = dataframe_parameters["file_name"]
      filter_col:str = dataframe_parameters["filter_column"]
      filter_vals:list = dataframe_parameters["filter_values"]
      val_col:str = dataframe_parameters["values_column"]
      city_code_col:str = dataframe_parameters["city_code_column"]
      year_col:str = dataframe_parameters["year_col"]
      dtype:str = dataframe_parameters["dtype"]
      category:str = dataframe_parameters["category"]
      
      extracted_files:list[str] = os.listdir(self.DOWNLOADED_FILES_PATH)
      if file_name not in extracted_files:
         raise RuntimeError("Falha ao achar o arquivo extraido da API")
      
      file_path:str = os.path.join(self.DOWNLOADED_FILES_PATH,file_name)
      df = pd.read_csv(file_path,usecols=[filter_col,val_col,city_code_col,year_col],sep=";") #le o arquivo csv apenas com as colunas necessárias
      filter_function = lambda x: x in filter_vals
      df = df[df[filter_col].apply(filter_function)]#filtra df baseado na coluna de filtragrem e nos valores permitidos

      df = df.rename(
         {
            year_col:self.DB_YEAR_COLUMN,
            val_col: self.DB_DATA_VALUE_COLUMN,
            city_code_col:self.DB_CITY_ID_COLUMN
         },
         axis="columns"
      )
      df = df.drop(filter_col,axis="columns")
      
      if dtype == DataTypes.FLOAT.value: #se o valor for um float, tem que transformar o separador decimal de , para .
         parse_float_str = lambda x: x.replace(",",".")
         df[self.DB_DATA_VALUE_COLUMN] =  df[self.DB_DATA_VALUE_COLUMN].apply(parse_float_str)

      df[self.DB_DATA_VALUE_COLUMN] =  df[self.DB_DATA_VALUE_COLUMN].astype(dtype)
      df[self.DB_DATA_IDENTIFIER_COLUMN] = data_name
      df[self.DB_DTYPE_COLUMN] = dtype

      time_series_years:list[int] = list(df[self.DB_YEAR_COLUMN].value_counts().index)

      return ProcessedDataCollection(
         category=category,
         dtype=DataTypes.from_string(dtype),
         data_name=data_name,
         time_series_years=time_series_years,
         df=df
      )

   def extract_data_points(self) -> list[ProcessedDataCollection]:
      link:str = self.__get_zipfile_link()
      self._download_and_extract_zipfile(link)
      return_data_list:list[ProcessedDataCollection] = []

      for ckan_api_id in self._data_map:
         for data_point in self._data_map[ckan_api_id]:
            print(data_point)
            data_collection = self.__get_processed_collection(self._data_map[ckan_api_id][data_point],data_point)
            return_data_list.append(data_collection)

      self._delete_download_files_dir()
      return return_data_list