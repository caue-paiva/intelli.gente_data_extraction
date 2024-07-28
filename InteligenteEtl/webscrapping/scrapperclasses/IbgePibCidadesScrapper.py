import requests
import re 
from re import Match
import pandas as pd
from .AbstractScrapper import AbstractScrapper, BaseFileType
from datastructures import YearDataPoint

"""
TODO
1) Atualmente esse código foi testado apenas na página de PIB de cidades do IBGE, testar em páginas similares
url da página: https://www.ibge.gov.br/estatisticas/economicas/contas-nacionais/9088-produto-interno-bruto-dos-municipios.html?t=downloads&c=1100023


"""

class IbgePibCidadesScrapper(AbstractScrapper):

   BASE_REGEX_PATTERN:str = r'base.{0,70}\.' #regex padrão pra qualquer substr com a palavra base
   URL = "https://www.ibge.gov.br/estatisticas/economicas/contas-nacionais/9088-produto-interno-bruto-dos-municipios.html?t=downloads&c=1100023"
   EXTRACTED_DATA_YEAR_COL = "Ano"

   file_type:BaseFileType
   file_is_zip:bool

   def __init__(self, file_type:BaseFileType, file_is_zip:bool) -> None:
      self.file_type = file_type
      self.file_is_zip = file_is_zip

   def extract_database(self)->list[YearDataPoint]:
      """
      Extrai um arquivo e retorna ele como um Dataframe da base de dados do IBGE 

      Return:
         list[YearDataPoint]: lista de objetos, cada um com um df e o ano dos dados associados a ele
      """
      
      file_link,list_of_years  = self._get_file_link()
      df =  super()._dataframe_from_link(file_link,self.file_type, self.file_is_zip) #retorna o dataframe do link extraido
      
      return  self.__separate_df_by_years(df,list_of_years)
      
   def __separate_df_by_years(self,df:pd.DataFrame,years_in_data:list[int])->list[YearDataPoint]:
      data_by_year:list[YearDataPoint] = []
      df[self.EXTRACTED_DATA_YEAR_COL] = df[self.EXTRACTED_DATA_YEAR_COL].astype("int") #transforma a coluna de anos em int para a comparação

      for year in years_in_data:
         data_single_year:pd.DataFrame = df[  df[self.EXTRACTED_DATA_YEAR_COL] == year  ]
         data_by_year.append(
            YearDataPoint(data_single_year,year)
         )
      
      return data_by_year


   def _file_type_to_regex(self)->str:
      """
      cria um padrão de regex para pegar links para arquivos de dados com o nome base e de um tipo específico (.zip,.xlsx...)
      """
      file_types_list:list[str] = ["zip", self.file_type.value]
      list_size:int = len(file_types_list)
      
      base_str = "("
      for i in range(list_size):
         if i < list_size -1:
            base_str += (file_types_list[i] + "|")
         else:
            base_str += (file_types_list[i])
      base_str +=  ")"
      return self.BASE_REGEX_PATTERN + base_str

   def _get_file_link(self)->tuple[str,list[int]]:
      """
      realiza o web-scrapping e retorna o link para o arquivo da base mais atual e com o tipo de arquivo passado como argumento
      
      Return:
         (tuple[str,list[int]]): tupla cujo primeiro elemento é o link pro arquivo e o segundo é a lista de anos dos dados
      
      """
      response = requests.get(self.URL) #request get pro site
      page_html: str = response.content.decode()  #pega conteudo html
      regex_pattern:str = self._file_type_to_regex() #cria regex para pegar o tipo de arquivo
      
      databases_match:list[Match] = list(re.finditer(regex_pattern, page_html,re.IGNORECASE)) #match no HTML com a string que identifica as bases de dados do ibge
      file_str_and_index: dict = {page_html[x.start():x.end()]:x.start() for x in databases_match} #cria um dict da string do link de bases e seu index na str do HTML
      
      str_list:list[str] = list(file_str_and_index.keys()) #lista das strings dos links
      data_info:dict = self._extract_best_dataset(str_list) #extrai o melhor dataset baseado na qntd de dados e/ou dados mais recentes
      
      file_name:str = data_info["file_name"] #nome do arquivo escolhido
      file_index: int = file_str_and_index[file_name] #index desse arquivo
      final_link:str = self._get_whole_link(page_html,file_index)

      years_range:tuple[int] = data_info["series_range"]
      list_of_years:list[int] = range(years_range[0],years_range[1]+1)

      return final_link, list_of_years

   def _get_whole_link(self,html:str, substr_index:int)->str:
      """
      Dado uma substr de um link para um arquivo com dados e o index dessa substr, retorna o link completo desse arquivo.
      Faz uma busca do index até achar um " na esq e direita, pois todos os links no html tem isso
      
      """
      link_end:int = html.find('"',substr_index)
      link_start:int = html.rfind('"',0,substr_index)

      return html[link_start+1:link_end]
 
   def _extract_best_dataset(self,file_name_list:list[str])->dict:
      """
      Return:
         {
         "file_name": "base/base_de_dados_2002_2009_xls.zip"
         "time_series_len" : 3
         "series_range" : (2002,2009)\n
         }
      """
      
      year_patern:str = r'(?<!\d)(\d{4})(?!\d)' #padrão regex para achar os anos no nome do arquivo
      most_recent_year:int = 0
      max_years_in_series:int = 0
      series_range:tuple[int] = ()
      return_file:str = file_name_list[0]

      for file in file_name_list: #loop pelos arquivos da lista de arquivos
         years_str: list[str] = list(re.findall(year_patern,file))
         if not years_str:
            raise RuntimeError("Não foi possível extrair o ano do link para o arquivo")
         years_int: list[int] = list(map(int,years_str)) #lista de anos agora como int
         
         cur_years_in_series: int =  1 if len(years_int) == 1 else years_int[-1] - years_int[0] #numero de anos na série histórica
         cur_series_range: tuple[int] = (years_int[0],years_int[-1]) if len(years_int) != 1 else (years_int[0])
         #1 se a lista tiver so um ano ou o ano final - o inicial se tiver mais que 1 
         
         if max(years_int) > most_recent_year: #se o ano máximo na lista for maior que a variável do ano mais recente
            return_file = file #arquivo de retorno muda
            max_years_in_series = cur_years_in_series #atualiza outras variável
            most_recent_year = max(years_int)
            series_range = cur_series_range


      return {
            "file_name": return_file,
            "time_series_len" : max_years_in_series,
            "series_range" : series_range
      }


if __name__ == "__main__":
   url = "https://www.ibge.gov.br/estatisticas/sociais/educacao/10586-pesquisa-de-informacoes-basicas-municipais.html?edicao=29466&t=downloads"
   url2 =  "https://www.ibge.gov.br/estatisticas/economicas/contas-nacionais/9088-produto-interno-bruto-dos-municipios.html?=&t=downloads"
   url2020 = "https://www.ibge.gov.br/estatisticas/sociais/educacao/10586-pesquisa-de-informacoes-basicas-municipais.html?edicao=32141&t=downloads"
   scrapper = IbgePibCidadesScrapper(url2,BaseFileType.EXCEL,True)
   df:pd.DataFrame = scrapper.extract_database()
   print(df.head())
