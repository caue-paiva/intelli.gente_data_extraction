import os, psycopg2
from psycopg2.extensions import connection,cursor
from dotenv import load_dotenv
from contextlib import contextmanager 
from typing import Generator,Any
import atexit

load_dotenv(os.path.join(os.path.dirname(__file__), "db_connection.env"))

class DBconnection():
   __CONNECTION: connection | None = None #variável de classe para conexão com db

   @classmethod
   def get_connection(cls)-> connection:
      if cls.__CONNECTION is None:
         cls.__CONNECTION = psycopg2.connect(
            host=os.getenv("DB_HOST"),      # e.g., "192.168.1.100"
            port=os.getenv("DB_PORT"), 
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USERNAME"),
            password=os.getenv("DB_PASSWORD")
         )
      return cls.__CONNECTION
   
   @classmethod
   def close_connection(cls)->None:
      if cls.__CONNECTION is not None:
         cls.__CONNECTION.close()
         cls.__CONNECTION = None
    
   @classmethod
   @contextmanager
   def get_cursor(cls,rollback_on_exception:bool=True)->Generator[cursor,Any,Any]:
      """
      context manager para gerenciar cursores da conexão com BD
      Ex:

      with DBconnection.get_cursor() as c:
         c.execute(query)
      """
      if cls.__CONNECTION is None: #cria uma conexão caso seja necessário
         cls.__CONNECTION = cls.get_connection()
      cursor = cls.__CONNECTION.cursor()
      try:
         yield cursor  # da o cursor para o context
      except psycopg2.Error as e:
        print(f"Database error: {e}")
        print(f"pgcode: {e.pgcode}")
        print(f"pgerror: {e.pgerror}")
        if e.diag:
            print(f"diag message primary: {e.diag.message_primary}")
        if rollback_on_exception:
            cls.__CONNECTION.rollback()
            print("Transaction rolled back due to the error.")
        raise
      finally:
         cursor.close()  # garante que o cursor é fechado
            

   @classmethod
   def execute_query(cls,query:str,return_data:bool=True)->list[tuple]:
      query_result:list = []
      with cls.get_cursor() as c:
         c.execute(query)
         if return_data:
            query_result = c.fetchall()
         cls.__CONNECTION.commit() # type: ignore
      if not query_result and return_data:
         raise RuntimeError(f"Falha ao executar a Query: {query}")
      return query_result
   
   @classmethod
   def insert_many_values(
      cls,
      table_name:str,
      columns_tuple:tuple,
      values_list:list[tuple],
      batch_size: int = 1500    
   )->None:
      if cls.__CONNECTION is None: #cria uma conexão caso seja necessário
         cls.__CONNECTION = cls.get_connection()
         
      columns = ", ".join(columns_tuple)
      with cls.get_cursor() as c:
        for i in range(0, len(values_list), batch_size):
            batch_values = values_list[i:i + batch_size] #cria um lote de valores para inserir
            placeholders = ", ".join( #join entre cada tupla de valores para inserir com a virgula no final de cada, executa isso o numero de elementos no lote vezes
                ["(" + ", ".join(["%s"] * len(columns_tuple)) + ")"] * len(batch_values) #cria a tupla de valores para inserir, cada um no estilo (%s,%s,%s)
            )

            flattened_values = [item for sublist in batch_values for item in sublist] #cria uma lista pegando cada item de cada tupla na lista
            query = f"INSERT INTO {table_name} ({columns}) VALUES {placeholders};" #cria a query
            try:
                c.execute(query, flattened_values)
            except psycopg2.Error as e:
               print(f"Database error: {e.pgerror}")
               cls.__CONNECTION.rollback()
               raise
        cls.__CONNECTION.commit()  # Commit the transaction after all inserts
     

   
atexit.register(DBconnection.close_connection) # type: ignore #fecha a conexão quando o programa parar de executar

DEFAULT_VAL_STR_COLS = 'n/a'
DEFAULT_VAL_INT_COLS = -1


from . import DBconnection,DEFAULT_VAL_STR_COLS,DEFAULT_VAL_INT_COLS