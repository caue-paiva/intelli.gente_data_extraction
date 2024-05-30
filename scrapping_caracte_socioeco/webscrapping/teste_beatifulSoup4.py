import requests , time
import selenium.webdriver
from re import Match
import re , os


def get_whole_link(html:str, substr_index:int)->str:
   
   link_end:int = html.find('"',substr_index)
   link_start:int = html.rfind('"',0,substr_index)

   return html[link_start+1:link_end]
 
def file_types_to_regex(file_types_list:list[str])->str:
   list_size:int = len(file_types_list)
   
   base_str = "("
   for i in range(list_size):
      if i < list_size -1:
         base_str += (file_types_list[i] + "|")
      else:
          base_str += (file_types_list[i])
   base_str +=  ")"
   return r'base.{0,70}\.' + base_str

def extract_best_dataset(file_name_list:list[str], priority_to_series_len:bool)->dict:
   """

   Return:
      {
      "file_name": "base/base_de_dados_2002_2009_xls.zip"
      "time_series_len" : 3
      "series_range" : (2002,2009)\n
      }
   """
   
   year_patern:str = r'(\d{4})' #padrão regex para achar os anos no nome do arquivo
   most_recent_year:int = 0
   max_years_in_series:int = 0
   series_range:tuple[int] = ()
   return_file:str = file_name_list[0]

   for file in file_name_list:
      years_str: list[str] = list(re.findall(year_patern,file))

      years_int: list[int] = list(map(int,years_str))
      print(years_int)
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
   ##response = requests.get(website_url) #request get pro site
   #html_content = response.content.decode()  #pega conteudo html

   response = requests.get(website_url) #request get pro site
   page_html: str = response.content.decode()  #pega conteudo html
   
   with open("teste2.html","w") as f:
      f.write(page_html)
   
   pattern:str = file_types_to_regex(["zip","xlsx","ods"]) #cria padrão regex para achar certo tipo de arquivo

   databases_match:list[Match] = list(re.finditer(pattern, page_html,re.IGNORECASE)) #match no HTML com a string que identifica as bases de dados do ibge
   print(len(databases_match))
   file_str_and_index: dict = {page_html[x.start():x.end()]:x.start() for x in databases_match} #cria um dict da string do link de bases e seu index na str do HTML
   
   str_list:list[str] = list(file_str_and_index.keys()) #lista das strings dos links
   print(str_list)
   data_info:dict = extract_best_dataset(str_list,True) #extrai o melhor dataset baseado na qntd de dados e/ou dados mais recentes
   file_name:str = data_info["file_name"] #nome do arquivo escolhido
   file_index: int = file_str_and_index[file_name] #index desse arquivo

   entire_link:str = get_whole_link(page_html,file_index)
   print(entire_link)
   

   return  entire_link

if __name__ == "__main__":

   url:str = "https://www.ibge.gov.br/estatisticas/economicas/contas-nacionais/9088-produto-interno-bruto-dos-municipios.html?=&t=downloads"
   url2019:str = "https://www.ibge.gov.br/estatisticas/sociais/educacao/10586-pesquisa-de-informacoes-basicas-municipais.html?edicao=29466&t=downloads"
   url2021:str = "https://www.ibge.gov.br/estatisticas/sociais/educacao/10586-pesquisa-de-informacoes-basicas-municipais.html?edicao=35765&t=downloads"
   url2020:str = "https://www.ibge.gov.br/estatisticas/sociais/educacao/10586-pesquisa-de-informacoes-basicas-municipais.html?edicao=32141&t=downloads"
   link = extract2(url2020)

   """patt:str = r'base.{0,70}\.ods'
   str_ = "base_abastecimento_agua_esgotamento_sanitario_MUNIC2017.ods"

   matches = re.findall(patt,str_)
   print(matches)"""



   """response = requests.get( link) #request get para o link do arquivo zip 
   if response.status_code == 200: #request com sucesso
         zip_file_name =  "zipfile.zip"
         zip_file_path = os.path.join("zip")
         with open(zip_file_path, "wb") as f:
            f.write(response.content) #escreve o arquivo zip no diretório de dados temporários"""

  
 

