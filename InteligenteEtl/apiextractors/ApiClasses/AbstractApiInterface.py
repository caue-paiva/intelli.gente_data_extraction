from apiextractors.apidataclasses import DataLine ,RawDataCollection
from datastructures.DataCollection import ProcessedDataCollection
from abc import ABC , abstractmethod
import pandas as pd



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

   #campos que cada objeto deve ter
   api_name:str
   goverment_agency:str
   _data_map: dict[str, dict] #dicionário extraidos de um JSON para mostrar a classe como accesar os dados na API 

   @abstractmethod
   def __init__(self, api_name:str, goverment_agency:str, api_referen_json_path:str) -> None:
      pass

   @abstractmethod 
   def _db_to_api_data_map(self, db_data_list:list[str| int])->dict:
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
   def extract_data_points(self, cities:list[int] = [] , data_point_names:list[str] = [] ,  time_series_len: int = 0)->list[ProcessedDataCollection]:
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
         
         for point in collection.data_lines : #constroi o dict com os dados da lista de DataPoints de cada variável
            data_point_dict[dict_index] = [point.city_id, point.year, point.data_name,point.value ,point.data_type.value]
            dict_index+=1
         columns:list[str] = [self.DB_CITY_ID_COLUMN, self.DB_YEAR_COLUMN, self.DB_DATA_IDENTIFIER_COLUMN, self.DB_DATA_VALUE_COLUMN, self.DB_DTYPE_COLUMN]
         df: pd.DataFrame = pd.DataFrame.from_dict(data_point_dict,orient="index",columns=columns) #cria um dataframe a partir do dicionário criado
         processed_data_list.append(collection.create_processed_collection(df)
  
      return processed_data_list
   
   def print_processed_data(self,data_list:list[ProcessedDataCollection]):
      for collection in data_list:
         print(collection.data_name)
         print(collection.category)
         print(collection.df.head(5))
         print(collection.time_series_years)
   
   def save_processed_data_in_csv(self,data_list:list[ProcessedDataCollection], index_of_data_list:int)->None:
        df = data_list[index_of_data_list].df
        df.to_csv("dados_extraidos_teste.csv") 
