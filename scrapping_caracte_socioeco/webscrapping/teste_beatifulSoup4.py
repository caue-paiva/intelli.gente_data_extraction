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


def extract2(website_url:str)->str:
   response = requests.get(website_url)
   html_content = response.content  # or response.text for a string representation

   soup = BeautifulSoup(html_content, 'html.parser')
   page_html = soup.prettify().lower()  #
   pattern = file_types_to_regex(["zip","xlsx","ods"])
   print(pattern)

   with open("teste.html", "w") as f:
      f.write(page_html)



   databases_match = re.finditer(pattern, page_html) #match no HTML com a string que identifica as bases de dados do ibge
   
   for match_ in databases_match:
      print(page_html[match_.start(): match_.end()])
   
   most_recent_data: Match = max(databases_match,key= lambda x : x.group()) #acha a data mais recente entre as matches de banco de dados, isso por que 2010 > 2000 na comparação

   substr:str = page_html[most_recent_data.start():most_recent_data.start() + 500]

   return substr


if __name__ == "__main__":

   url:str = "https://www.ibge.gov.br/estatisticas/economicas/contas-nacionais/9088-produto-interno-bruto-dos-municipios.html?=&t=downloads"
   extract2(url)

