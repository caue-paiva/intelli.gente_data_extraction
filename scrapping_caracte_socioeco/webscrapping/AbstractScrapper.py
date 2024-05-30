from abc import ABC,abstractmethod
from enum import Enum
import pandas as pd
import os , requests , zipfile

class BaseFileType(Enum): #enum para os tipos de arquivos que são comumente encontrados em bases oficiais do governo
   EXCEL = "xlsx"
   ODS = "ods"
   TXT = "txt"
   CSV = "csv"
   


class AbstractScrapper(ABC):

   EXTRACTED_FILES_DIR:str = "tempfiles" #diretório temporário para guardar os arquivos .zip e de dados extraidos

   @abstractmethod
   def __init__(self,website_url:str,file_type:str,*kwargs)-> None:
      pass

   @abstractmethod
   def extract_database(self, website_url:str, file_type:BaseFileType)->pd.DataFrame:
      """Extrai um arquivo e retorna ele como um Dataframe da base de dados oficial dado um URL para uma página e o tipo de dado do arquivo"""
      pass
   
   @abstractmethod
   def download_database_locally(self, website_url:str, file_type:BaseFileType)->str:
      """baixa um arquivo da base de dados oficial e retorna o caminho para ele dado um URL para uma página e o tipo de dado do arquivo"""
      pass
   
   def _download_and_extract_zipfile(self, file_url:str)->str:
      """
      Dado um URL para baixar um arquivo zip das bases oficiais, baixa esse arquivo zip e extrai seu conteúdo,
      retornando o caminho para o arquivo de dados que extraido. Esse método é implementado na classe mãe abstrata, pois ele é genérico para a maioria
      dos scrappers de diferentes bases

      Args:
         file_url (str): url para baixar o arquivo .zip das bases do IBGE
      """
     
      #caso o diretório para guardar os arquivos extraidos não exista, vamos criar ele
      if not os.path.exists(self.EXTRACTED_FILES_DIR):
         os.makedirs(self.EXTRACTED_FILES_DIR)

      #baixando o arquivo zip
      response = requests.get(file_url) #request get para o link do arquivo zip 
      if response.status_code == 200: #request com sucesso
         zip_file_name =  "zipfile.zip"
         zip_file_path = os.path.join(self.EXTRACTED_FILES_DIR, zip_file_name)
         print(zip_file_name)
    
         with open(zip_file_path, "wb") as f:
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
   
   def _dataframe_from_link(self, file_url:str, file_type: BaseFileType, zipfile: bool = True)->pd.DataFrame:
      """
      Dado um link para um arquivo , se ele for zip primeiro extrai e dps carrega o arquivo tabular extraido, caso não seja apenas usas as 
      funções do pandas para carregar essa arquivo em um Dataframe

      Args:
         file_url (str): url para baixa o arquivo
         file_type (BaseFileType): tipo de arquivo a ser baixado
         zipfile (bool): diz se o link é pra um arquivo zip ou não

      Return:
         (Dataframe): um df da base extraida
      """
      
      if zipfile: #link  é pra um arquivo zip, vamos extrair ele primeiro
         file_path:str = self._download_and_extract_zipfile(file_url) #chama o método da mesma classe de extrair o zipfile
      else:
         file_path:str = file_url  #link n é pra um arquivo zip, o argumento pode ser passado para o pandas direto

      df:pd.DataFrame = None
      match (file_type): #match case no tipo de dado
         case BaseFileType.EXCEL:
            df = pd.read_excel(file_path)
         case BaseFileType.ODS:
            pass
         case BaseFileType.CSV:
            pass
      """
      TODO
      Colocar os casos para os outros tipos de arquivos, caso seja possível extrair dataframes deles
      """

      if df is None: #não foi possível extrair o DF
         raise RuntimeError("não foi possível criar um dataframe a partir do link")
      
      return df
     
