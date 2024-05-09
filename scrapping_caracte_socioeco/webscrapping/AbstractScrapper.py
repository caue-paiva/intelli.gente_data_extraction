from abc import ABC,abstractmethod
from enum import Enum
import pandas as pd
import os , requests , zipfile

class BaseFileType(Enum): #enum para os tipos de arquivos que são comumente encontrados em bases oficiais do governo
   EXCEL = "xlsx"
   ODS = "ods"
   TXT = "txt"


class AbstractScrapper(ABC):

   EXTRACTED_FILES_DIR:str = "tempfiles" #diretório temporário para guardar os arquivos .zip e de dados extraidos

   @abstractmethod
   def extract_database(self, website_url:str, file_type:BaseFileType, *args)->pd.DataFrame:
      """Extrai um arquivo e retorna ele como um Dataframe da base de dados oficial dado um URL para uma página e o tipo de dado do arquivo"""
      pass
   
   @abstractmethod
   def download_database_locally(self, website_url:str, file_type:BaseFileType, object_identifier:str ,*args)->str:
      """baixa um arquivo da base de dados oficial e retorna o caminho para ele dado um URL para uma página e o tipo de dado do arquivo"""
      pass
   
   
   def _download_and_extract_zipfile(self,file_url:str, object_identifier:str)->str:
      """
      Dado um URL para baixar um arquivo zip das bases oficiais, baixa esse arquivo zip e extrai seu conteúdo,
      retornando o caminho para o arquivo de dados que extraido. Esse método é implementado na classe mãe abstrata, pois ele é genérico para a maioria
      dos scrappers de diferentes bases

      Args:
         file_url (str) : url para baixar o arquivo .zip das bases do IBGE
         object_identifier (str): para que o arquivo zip seja identificado com a subclasse que criou esse arquivo, já que não é possível pegar o 
         nome do mesmo pelo link de download
      """
     
      zip_file_name:str = object_identifier + "_zipfile.zip"
      #caso o diretório para guardar os arquivos extraidos não exista, vamos criar ele
      if not os.path.exists(self.EXTRACTED_FILES_DIR):
         os.makedirs(self.EXTRACTED_FILES_DIR)

      #baixando o arquivo zip
      response = requests.get(file_url) #request get para o link do arquivo zip 
      if response.status_code == 200: #request com sucesso
   
         with open(os.path.join(self.EXTRACTED_FILES_DIR, zip_file_name), "wb") as f:
            f.write(response.content) #escreve o arquivo zip no diretório de dados temporários
      else:
         raise RuntimeError("Falhou em baixar o arquivo .zip, status code da resposta:", response.status_code)

      #extraindo o arquivo zip
      with zipfile.ZipFile(os.path.join(self.EXTRACTED_FILES_DIR, zip_file_name), "r") as zip_ref:
            zip_ref.extractall(self.EXTRACTED_FILES_DIR)

      #no diretório de arquivos baixados e extraidos, vamos ter o .zip e o arquivo de dados extraido
      extracted_files:list[str] = os.listdir(self.EXTRACTED_FILES_DIR)
      
      data_file_name:str = ""
      for file in extracted_files:
         if ".zip" not in file: #acha o arquivo de dados, que é o único sem ser .zip
            data_file_name = file
            break

      if not extracted_files or not data_file_name: #nenhum arquivo foi extraido ou nenhum arquivo de dados foi encontrado
         raise RuntimeError("Extração do arquivo zip num diretório temporário falhou")
      
      return os.path.join(self.EXTRACTED_FILES_DIR, data_file_name) #retorna o caminho para o arquivo extraido
     

