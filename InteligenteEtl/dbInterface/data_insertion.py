from dbInterface import DBconnection
from dbInterface.utils import parse_topic_table_name
from dbInterface.dimension_tables import get_datapoint_dim_table_info
import pandas as pd

def insert_df_into_fact_table(df:pd.DataFrame,data_name:str,time_series_years:list[int])->int:
   """
   Tenta inserir um df numa tabela de fatos, retorna quantas linhas foram adicionadas
   """
   fact_table_info: dict | None  = get_datapoint_dim_table_info(data_name)
   if fact_table_info is None:
      print("Dado não foi encontrado na tabela de dimensao_dados")
      return 0

   data_point_fk: int = fact_table_info["dado_id"]
   existing_time_series_years:list[int] = fact_table_info["anos_serie_historica"]
   fact_table_name = fact_table_info["topico"][:20] #nome da coluna pode ter no max 20 chars
   fact_table_name:str = parse_topic_table_name(fact_table_name)  #nome da tabela de fato é o tópico com parsing para ser um nome válido no SQL


   years_to_insert:list[int] = [year for year in time_series_years if year not in existing_time_series_years]
   if not years_to_insert: #nenhum ano novo foi extraído
      print("nenhum novo ano foi encontrado nos dados")
      return 0

   df = __prepare_df_for_database(df=df,data_point_fk=data_point_fk,years_to_extract=years_to_insert)
   
   df_rows:list[tuple] = []
   for row in df.itertuples(index=False,name=None):
      df_rows.append(
         tuple(map(lambda x: x.lower()[:20] if isinstance(x,str) else x, row)) #slice até 20 chars pq esse é o tamanho do VARCHAR da coluna de nome do dado da tbela
      )
   
   #df.to_csv("antes_de_entrar.csv")
   __insert_values_fact_table(fact_table_name,df_rows)

   all_years: list[int] = list(set(time_series_years + existing_time_series_years)) #pega todos os anos únicos dos dados
   all_years.sort() #ordena eles
   __update_time_series_year(data_name,all_years)

   return len(df_rows) #retorna numero de linhas do df que foi inserido

def __insert_values_fact_table(fact_table:str,values:list[tuple])->None:
   fact_table_cols =  (
      'dado_id',
      'municipio_id',
      'ano',
      'tipo_dado',
      'valor'
   )
   if len(values) == 0:
      raise RuntimeError("Inserindo uma lista vazia de valores")

   if len(fact_table_cols) != len(values[0]):
      raise RuntimeError("Valores a serem inseridos não batem com as colunas da tabela de fato")

   DBconnection.insert_many_values(
      table_name=fact_table,
      columns_tuple=fact_table_cols,
      values_list=values,
      batch_size=2500
   )

def __update_time_series_year(data_name:str,new_time_series_years:list[int])->None:
   query = f"""--sql
   UPDATE dimensao_dado
   SET anos_serie_historica = ARRAY[{", ".join(map(str,new_time_series_years))}] --atualiza a série histórica
   WHERE LOWER(REPLACE(nome_dado, ' ', '')) = LOWER(REPLACE('{data_name}', ' ', '')); --filtra pelo dado
   """
   DBconnection.execute_query(query,False)

def __replace_city_codes_with_pk(city_codes:pd.Series)->pd.Series:
   query = """
   SELECT municipio_id,codigo_municipio FROM dimensao_municipio;
   """
   query_result = DBconnection.execute_query(query)
   city_code_to_pk:dict[int,int] = {city_code:city_pk for city_pk,city_code  in query_result} #dict cuja key é o codigo do munic e o valor é a pk da tabela de dimensao do municipio

   return city_codes.map(city_code_to_pk)

def __prepare_df_for_database(df:pd.DataFrame, data_point_fk:int, years_to_extract:list[int])->pd.DataFrame:
   """
   Transforma o dataframe num formato final so para entrar no banco de dados. Troca o nome do dado pela chave estrangeira da tabela dimensão de dado
   e o código de município pela chave extrangeira que aponta pra tabela de municípios e por ultimo troca a ordem das colunas para ser igual a da tabela de fato
   """
   city_code_example = df["codigo_municipio"].iloc[-1]
   if city_code_example < 1000000 or city_code_example > 9999999:
      raise RuntimeError("Código do muncípio não tem 7 dígitos")
   
   df["dado_identificador"] = data_point_fk #troca coluna de nome de dados por uma foreign key que referencia a tabela dimen
   df["codigo_municipio"] = __replace_city_codes_with_pk(df["codigo_municipio"]) #troca código do município pela fk desse munic na tabela de dimensao
   df = df.rename(
      {
         "dado_identificador":"dado_id",
         "codigo_municipio": "municipio_id"
      }
      ,axis="columns"
   )
   fact_table_cols =  [
      'dado_id',
      'municipio_id',
      'ano',
      'tipo_dado',
      'valor'
   ]
   df = df[fact_table_cols]
   df = df[df["ano"].apply(lambda x: x in years_to_extract)] #filtra o df para ter apenas anos para extrair

   return df