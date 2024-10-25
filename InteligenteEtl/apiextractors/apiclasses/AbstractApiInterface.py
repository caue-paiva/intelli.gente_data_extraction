from apiextractors.apidataclasses import DataLine ,RawDataCollection
from datastructures.DataCollection import ProcessedDataCollection
from abc import ABC , abstractmethod
import pandas as pd
import os, zipfile,requests
from typing import Any



class AbstractApiInterface(ABC):

   """
   Classe abstrata e interface para implementar classes que extraem dados por meio de APIs, como a de agregados do IBGE
   Fornece constantes de classe para o nome das colunas de acordo com o banco de dados e um método concreto para transformar dados
   não processados em um objeto da classe de dados processados requerido para ingestão no BD.
   """


   DB_CITY_ID_COLUMN = "codigo_municipio" #constantes para o nome das colunas no dataframe final e no banco de dados
   DB_YEAR_COLUMN = "ano"
   DB_DATA_IDENTIFIER_COLUMN = "dado_identificador"
   DB_DATA_VALUE_COLUMN = "valor"
   DB_DTYPE_COLUMN = "tipo_dado"
   MAX_TIME_SERIES_LEN = 7

   DOWNLOADED_FILES_DIR:str = "tempfiles" #diretório temporário para guardar os arquivos .zip e de dados extraidos
   DOWNLOADED_FILES_PATH = os.path.join(os.getcwd(),DOWNLOADED_FILES_DIR)

   #campos que cada objeto deve ter
   api_name:str
   goverment_agency:str
   _data_map: dict[str, dict] #dicionário extraidos de um JSON para mostrar a classe como accesar os dados na API 

   @abstractmethod
   def __init__(self, api_name:str, goverment_agency:str, api_referen_json_path:str) -> None:
      pass

   @abstractmethod 
   def _db_to_api_data_map(self)->Any:
      """
      método que mapea o identificador do dado (seu nome ou id, não está decidido) que está na base de dados 
      com o identificador desses dados na API por meio de um dicionário. Os dados desse mapeamento estão disponíveis num JSON ou no própio código,
      isso depende da subclasse específica

      Ex da base de agregados do IBGE, mapeando o nome do dado com as o número de sua variável e o número do agregado que ele representa:
      
      {
         "PIB TOTAL" :  {"variavel": 37 , "agregado": 5938 }, 
         "PIB AGROPECUARIA": {"variavel": 513 , "agregado": 5938 },
         "PIB INDUSTRIA": {"variavel": 517 , "agregado": 5938 },
         "PIB SERVICOS" : {"variavel": 6575 , "agregado": 5938 },
      }

      """
      pass

   @abstractmethod   
   def extract_processed_collection(self)->list[ProcessedDataCollection]:
      pass

   def process_raw_data(self,data_collections: list[RawDataCollection])->list[ProcessedDataCollection]:
      """
      Recebe uma lista de Coleções de Dados não processados (linhas da tabela e dados sobre a categoria e série histórica), transforma esses dados em um DF e converte essa lista
      em uma lista de coleções de dados processados
    
      O algoritmo de transformar a lista em um dataframe da forma mais eficiente foi baseada nessa discussão: https://stackoverflow.com/questions/41888080/efficient-way-to-add-rows-to-dataframe/41888241#41888241

      Args:
        data_collections (list[RawDataCollection]) : Lista de objetos de coleções de dados não processados, contém listas dos dados coletados e contextualização sobre do que se trata aqueles dados e sobre a série historica

      Return:
         (pd.Dataframe) : df no formato da tabela de dados brutos do Data Warehouse
   
      """
      processed_data_list:list[ProcessedDataCollection] = []

      dict_index = 0
      for collection in data_collections: #loop por todas as variáveis
         data_point_dict: dict [int, list] = {} #dicionário com a chave sendo o index da linha e o valor sendo uma lista com os valores da linha a ser colocada no df
         data_name:str = collection.data_name #nome do dado da coleção
         for point in collection.data_lines : #constroi o dict com os dados da lista de DataPoints de cada variável
            data_point_dict[dict_index] = [point.city_id, point.year, data_name, point.value ,point.data_type.value]
            dict_index+=1
         
         columns:list[str] = [self.DB_CITY_ID_COLUMN, self.DB_YEAR_COLUMN, self.DB_DATA_IDENTIFIER_COLUMN, self.DB_DATA_VALUE_COLUMN, self.DB_DTYPE_COLUMN]
         df: pd.DataFrame = pd.DataFrame.from_dict(data_point_dict,orient="index",columns=columns) #cria um dataframe a partir do dicionário criado
         df[self.DB_YEAR_COLUMN] = df[self.DB_YEAR_COLUMN].astype("int") #transforma essas colunas em inteiros
         df[self.DB_CITY_ID_COLUMN] = df[self.DB_CITY_ID_COLUMN].astype("int")
         
         df = df.dropna(axis="index",subset=[self.DB_DATA_VALUE_COLUMN]) #remove as linhas com valores nulos
         processed_data_list.append(collection.create_processed_collection(df))
  
      return processed_data_list
   
   def print_processed_data(self,data_list:list[ProcessedDataCollection]):
      for collection in data_list:
         print(collection.data_name)
         print(collection.category)
         print(collection.df.head(5))
         print(collection.time_series_years)
   
   def save_processed_data_in_csv(self,data_list:list[ProcessedDataCollection], index_of_data_list:int)->None:
        df = data_list[index_of_data_list].df
        df.to_csv("dados_extraidos_teste.csv",index=False) 

   def _download_and_extract_zipfile(self, file_url:str)->str:
      """
      Dado um URL para baixar um arquivo zip das bases oficiais, baixa esse arquivo zip e extrai seu conteúdo,
      retornando o caminho para o arquivo de dados que extraido. Esse método é implementado na classe mãe abstrata, pois ele é genérico para a maioria
      dos scrappers de diferentes bases

      Args:
         file_url (str): url para baixar o arquivo .zip das bases do IBGE
      """
     
      #caso o diretório para guardar os arquivos extraidos não exista, vamos criar ele
      if not os.path.exists(self.DOWNLOADED_FILES_PATH):
         os.makedirs(self.DOWNLOADED_FILES_PATH)

      #baixando o arquivo zip
      response = requests.get(file_url) #request get para o link do arquivo zip 
      if response.status_code == 200: #request com sucesso
         zip_file_name =  "zipfile.zip"
         zip_file_path = os.path.join(self.DOWNLOADED_FILES_DIR, zip_file_name)
    
         with open(zip_file_path, "wb") as f:
             f.write(response.content) #escreve o arquivo zip no diretório de dados temporários
      else:
          raise RuntimeError("Falhou em baixar o arquivo .zip, status code da resposta:", response.status_code)
      

      #extraindo o arquivo zip
      with zipfile.ZipFile(os.path.join(self.DOWNLOADED_FILES_DIR, zip_file_name), "r") as zip_ref:
            zip_ref.extractall(self.DOWNLOADED_FILES_DIR)

      #no diretório de arquivos baixados e extraidos, vamos ter o .zip e o arquivo de dados extraido
      extracted_files:list[str] = os.listdir(self.DOWNLOADED_FILES_DIR)
      
      data_file_name:str = ""
      for file in extracted_files:
         if ".zip" not in file: #acha o arquivo de dados, que é o único sem ser .zip
            data_file_name = file
            break

      if not extracted_files or not data_file_name: #nenhum arquivo foi extraido ou nenhum arquivo de dados foi encontrado
         raise RuntimeError("Extração do arquivo zip num diretório temporário falhou")
      
      return os.path.join(self.DOWNLOADED_FILES_DIR, data_file_name) #retorna o caminho para o arquivo extraido
   
   def _delete_download_files_dir(self)->bool:
      files:list[str] = os.listdir(self.DOWNLOADED_FILES_PATH)
      for file in files:
            try:
               os.remove(os.path.join(self.DOWNLOADED_FILES_PATH,file))
            except Exception as e:
               print(f"falha ao deletar um arquivo extraído. Erro: {e}")
      try:
            os.rmdir(self.DOWNLOADED_FILES_PATH)
      except Exception as e:
               print(f"falha ao deletar diretório do arquivo extraído. Erro: {e}")