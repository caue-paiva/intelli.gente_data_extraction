import requests
from bs4 import BeautifulSoup
from re import Match
import re
import pandas as pd
from html_parser import MyHTMLParser

"""
def extract(website_url:str, file_type:str):
   DATA_BASE_IDENTIFIER_PATTERN: str = r"Base \d{4}-\d{4}" #regex para achar a string que identifica onde está os arquivos da base de dados
   HTML_TAG_IDENTIFIER:str = "li" #essas são constantes internas da lógica de webscrapping de página desse tipo 

   driver = selenium.webdriver.Chrome()
   driver.get(website_url) #acessa a página do url
   page_source:str = driver.page_source #pega o HTML da página
   driver.quit() #fecha o webdriver 

   databases_match = re.finditer(DATA_BASE_IDENTIFIER_PATTERN, page_source) #match no HTML com a string que identifica as bases de dados do ibge
   most_recent_data: Match = max(databases_match,key= lambda x : x.group()) #acha a data mais recente entre as matches de banco de dados, isso por que 2010 > 2000 na comparação

   html_parser = MyHTMLParser(HTML_TAG_IDENTIFIER) #instancia um objeto de parser de html
   substr:str = page_source[most_recent_data.start(): ] #corta o html da página a partir da str que representa o banco de dados do IBGE
      
   parsed_html:str = html_parser.get_limited_html_block(substr) #extrai apenas o bloco de html com a tag que tem os links dos arquivos do banco
   lista_links:list[str] = html_parser.get_all_links( parsed_html) #pega todos os links do html extraido
   final_link:str = ""

   for link in lista_links: #acha o link certo para o tipo de dado passado como argumento
      if file_type in link: #link pra um arquivo que tem o tipo de dado correto
               final_link = link
               break
         
   if not final_link:
      raise RuntimeError("não foi possível extrai o link para o arquivo de dados")
   return final_link
"""

def file_types_to_regex(file_types_list:list[str])->str:
   list_size:int = len(file_types_list)
   
   base_str = "("
   for i in range(list_size):
      if i < list_size -1:
         base_str += (file_types_list[i] + "|")
      else:
          base_str += (file_types_list[i])
   base_str +=  ")"
   return r'base.{0,30}\.' + base_str

def extract_best_dataset(file_name_list:list[str], priority_to_series_len:bool)->dict:
   """

   Return:
      {
      "file_name": "base/base_de_dados_2002_2009_xls.zip"
      "time_series_len" : 3
      "series_range" : (2002,2009)\n
      }
   """
   
   year_patern:str =  r'\b\d{4}\b'
   most_recent_year:int = 0
   max_years_in_series:int = 0
   series_range:tuple[int] = ()
   return_file:str

   for file in file_name_list:
      print(file)
      years_str: list[str] = list(re.findall(file,year_patern))
      print(years_str)
      years_int: list[int] = list(map(int,years_str))
      cur_years_in_series: int =  1 if len(years_int) == 1 else years_int[-1] - years_int[0] #numero de anos na série histórica
      cur_series_range: tuple[int] = (years_int[0],years_int[-1]) if len(years_int) != 1 else (years_int[0])
      #1 se a lista tiver so um ano ou o ano final - o inicial se tiver mais que 1 
      
      if priority_to_series_len: #prioridade para coletar com a maior série histórica
         if cur_years_in_series >= max_years_in_series and max(years_int) > most_recent_year: #só um ano no dataset
            return_file = file
            max_years_in_series = cur_years_in_series
            most_recent_year = max(years_int)
            series_range = cur_series_range
      else:
         if max(years_int) > most_recent_year:
            return_file = file
            max_years_in_series = cur_years_in_series
            most_recent_year = max(years_int)
            series_range = cur_series_range


   return {
         "file_name": return_file,
         "time_series_len" : max_years_in_series,
         "series_range" : series_range
   }



def extract2(website_url:str)->str:
   response = requests.get(website_url)
   html_content = response.content  # or response.text for a string representation

   soup = BeautifulSoup(html_content, 'html.parser')
   page_html = soup.prettify().lower()  #
   pattern = file_types_to_regex(["zip","xlsx","ods"])

   databases_match:list[Match] = list(re.finditer(pattern, page_html)) #match no HTML com a string que identifica as bases de dados do ibge
   str_list:list[str] = list(map(lambda x : page_html[x.start():x.end()],  databases_match))
   print(str_list)
   dict_ = extract_best_dataset(str_list,False)
   
   
   #most_recent_data: Match = max(databases_match,key= lambda x : x.group()) #acha a data mais recente entre as matches de banco de dados, isso por que 2010 > 2000 na comparação
   #substr:str = page_html[most_recent_data.start():most_recent_data.start() + 500]

   return "dict_"


if __name__ == "__main__":

   url:str = "https://www.ibge.gov.br/estatisticas/economicas/contas-nacionais/9088-produto-interno-bruto-dos-municipios.html?=&t=downloads"
   print(extract2(url))

