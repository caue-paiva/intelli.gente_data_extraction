from dbInterface import DBconnection

def get_datapoint_dim_table_info(data_point_name:str)-> dict | None:
   """
   Dado o nome de um dado, retorna a tabela de fato que ele pertence e os anos da série histórica desse dado.
   Caso esse dado não esteja mapeado na tabela dimensao_dados, retorna none

   Args:
      data_point_name (str): Nome do dado

   Return:
      (dict):
      {
            "topico": topico_do_dado,
            "dado_id": id_do_dado,
            "anos_serie_historica": lista_anos_serie_historica
      }
   """
   query = f"""--sql
   SELECT topico,dado_id,anos_serie_historica FROM dimensao_dado
   WHERE  LOWER(REPLACE(dimensao_dado.nome_dado, ' ', '')) = LOWER(REPLACE('{data_point_name}', ' ', ''));
   """
   result:list[tuple] = DBconnection.execute_query(query)
   if not result:
      return None

   return {
      "topico": result[0][0],
      "dado_id": result[0][1],
      "anos_serie_historica":result[0][2]
}