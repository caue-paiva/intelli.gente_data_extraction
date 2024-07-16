import requests, re
from bs4 import BeautifulSoup
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains


"""
https://www.tesourotransparente.gov.br/ckan/dataset/9ff93162-409e-48b5-91d9-cf645a47fdfc/resource/86636c19-b38a-4b9e-8fff-30fc4208dd04/download/CAPAG-Municipios---Ano-Base-2022.xlsx
"""

URL = "https://www.tesourotransparente.gov.br/ckan/dataset/capag-municipios/resource/6a218451-f1b4-4fce-ac2a-00a3675bf4eb?inner_span=True"


def __get_data_ids_and_years()->list[tuple[str]]:
   """
   Faz uma request no site da receita e retorna uma lista com os links com os ids dos dados de CAPAG de cada ano e o ano referente
   
   Args:
      (None)
   Return:
      list[tuple[str]]: lista de tuplas cujo primeiro elemento é o link com os ids dos dados e o segundo elemento sendo a string do ano que se refere
   """

   response = requests.get(url=URL)
   if response.status_code != 200:
      raise RuntimeError("Falha na request para pegar o HTML do server")

   html_text: str = response.text
   soup = BeautifulSoup(html_text, 'html.parser') #parser de HTML do Beatiful soup

   #acha o elemento ul com a classe "unstyled nav nav-simple"
   ul_element = soup.find('ul', class_='unstyled nav nav-simple')

   # Initialize a list to store the tuples
   result:list[tuple] = []

   # Check if the ul element is found
   if ul_element:
      # Find all li elements within the ul
      li_elements = ul_element.find_all('li', class_='nav-item')
      
      for li in li_elements:
         a_tag = li.find('a')
         span_tag = a_tag.find('span')
         
         if a_tag and span_tag:
               href = a_tag.get('href')
               text = span_tag.text.strip()
               result.append((href, text))
   
   return result

def __parse_link_year_tuples(list_of_tuples:list[tuple[str]])->list[tuple[str]]:
   get_dataset_id = lambda x: x[x.rfind("/")+1: x.rfind("?")] #ex: dado a str: capag-municipios/resource/86636c19-b38a-4b9e-8fff-30fc4208dd04?inner_span=True retorna 
   #86636c19-b38a-4b9e-8fff-30fc4208dd04

   year_regex_pattern = r'\b\d{4}\b'
   get_data_year = lambda x: "" if len(re.findall(year_regex_pattern,x)) == 0 else re.findall(year_regex_pattern,x)[0]  #dado "CAPAG Municipios 2024" retorna 2024
   transform_tuples = lambda x: (get_dataset_id(x[0]), get_data_year(x[1])) #aplica ambas as funcoes em uma tupla e retorna uma nova tupla
   
   transformed_list:list[tuple[str]] = list(map(transform_tuples,list_of_tuples))
   transformed_list = list(filter(lambda x: x[1] != "",transformed_list))

   return transformed_list

def __most_recent_data_by_year(list_of_tuples:list[tuple[str,int]])->list[tuple[str,int]]:
   """
   Caso um ano apareça mais de uma vez na lista isso significa ou que teve um correção dos dados ou que os dados estão mensais ainda 
   (o ano não acabou), em ambos os casos é melhor pegar o ultimo ID do dataset desse ano

   Args:
      list_of_tuples (list[tuple[str,int]]): lista de tuplas com o id dos dados e o ano
   Return:
      list[tuple[str,int]]: lista de tuplas com o id dos dados e o ano, mas sem anos repetidos
   """

   non_repeated_years =  {} #dict para guardar os anos únicos como key e seu primeiro id como value
   list_of_tuples.reverse() #reverte a lista, indo do mais recente pro mais novo

   for data_id,year in list_of_tuples:
      if year not in non_repeated_years: #caso o ano nao tenha aparecido
         non_repeated_years[year] = data_id
   
   return [(data_id,year) for year,data_id in non_repeated_years.items()] #transforma o dict em uma lista de tuplas denovo

def __build_csv_file_link(data_id:str)->str:
   """
   Dado um id do dado, constroi o link do CSV 
   
   """

def __extract_csv_link(driver:webdriver.Chrome,new_window_link:str)->str:
   
   driver.execute_script("window.open(arguments[0]);", new_window_link)
   driver.switch_to.window(driver.window_handles[-1])
   href = ""
    # Extract the href of the a tag inside a p tag with class "muted ellipsis"
   try:
        p_tag = driver.find_element(By.CSS_SELECTOR, 'p.muted.ellipsis')
        a_inside_p = p_tag.find_element(By.TAG_NAME, 'a')
        href = a_inside_p.get_attribute('href')
   except Exception as e:
        print(f'Error extracting href: {e}')

   # Close the new window
   driver.close()
   driver.switch_to.window(driver.window_handles[0])
   return href

def __select_all_years():
   driver = webdriver.Chrome()  # Update the path to your chromedriver
   driver.maximize_window()

   # Load the webpage
   driver.get(URL)

   # Find the ul element with the class "unstyled nav nav-simple"
   ul_element = driver.find_element(By.CSS_SELECTOR, 'ul.unstyled.nav.nav-simple')

   # Find all li elements within the ul
   li_elements = ul_element.find_elements(By.CSS_SELECTOR, 'li.nav-item')

   year_regex_pattern = r'\b\d{4}\b'
   list_of_csv_links:list[str] = []
   for ele in li_elements:
      a_tag = ele.find_element(By.TAG_NAME, 'a')
      link = a_tag.get_attribute('href')
      element_text:str = ele.text
     
      year_regex_matches:list[str] = re.findall(year_regex_pattern,element_text)
      if not year_regex_matches:
         continue
      year: str = year_regex_matches[0]
      csv_link:str = __extract_csv_link(driver,link)
      list_of_csv_links.append(csv_link)
   
   driver.quit()
   return list_of_csv_links  

def __fill_non_existent_years(list_of_link_year_tuples:list[tuple[str,int]])->None:
   previous_year:int = 0
   for i in range(len(list_of_link_year_tuples)):
      link:str = list_of_link_year_tuples[i][0]
      year:int = list_of_link_year_tuples[i][1]
      if year == 0: #ano nulo
         list_of_link_year_tuples[i] = (link,previous_year+1)  #coloca o valor do ano como ano anterior +1
         continue #ano anterior continua o mesmo
      
      previous_year = year #ano não nulo, atualiza o ano anterior

def __match_links_with_their_years(list_of_links:list[str])->list[tuple[str,int]]:
   year_regex_pattern = r'(?<!\d)\d{4}(?!\d)' #pega 4 números juntos e no fim e começo desses números tem um char não numerico
   NON_FOUND_YEAR = 0 #valor para quando o ano não foi inferido do link

   get_link_year_tuple = lambda x: (x, int(re.findall(year_regex_pattern,x)[-1])) if len(re.findall(year_regex_pattern,x)) > 0 else (x,NON_FOUND_YEAR) #cria uma tupla do link e seu ano 
   replace_non_years_with_empty_str = lambda x: (x[0], x[1]) if x[1] < 2050 else (x[0],NON_FOUND_YEAR) #caso um número  extraido não seja um ano ele vai ser trocado por uma string vazia
   fix_year_of_corrected_data = lambda x: (x[0],x[1]) if "corrigido" not in x[0] else (x[0], x[1]-1) #caso o dado seja corrigido, ele tem um ano no nome mas se refere ao ano anterior
   #pelo menos isso é o caso nos dados corrigidos de 2022

   compose_funcs = lambda x: fix_year_of_corrected_data(replace_non_years_with_empty_str(get_link_year_tuple(x))) #compoe as funções acima

   list_of_tuples:list[tuple[str,int]] = list(map(compose_funcs,list_of_links))
   __fill_non_existent_years(list_of_tuples)

   convert_years_to_str = lambda x: (x[0],str(x[1])) #o padrão no projeto é trabalho com anos como string, então os anos ints serão convertidos
   list_of_tuples = list(map(convert_years_to_str,list_of_tuples))

   return list_of_tuples

def dataframes_from_links_and_years(list_of_link_year_tuples:list[tuple[str,int]])->list[tuple[pd.DataFrame,str]]:
   list_dfs_years:list[tuple[pd.DataFrame,str]] = []

   EXCEL_SHEET_NAME = "capag" #sheets do excel com esse nome são as buscadas
   filter_sheet_names = lambda x: True if EXCEL_SHEET_NAME in x.lower() else False #filtra o nome da sheet do excel correta

   for link,year in list_of_link_year_tuples:
      df:pd.DataFrame
      if "csv" in link:
         df = pd.read_csv(link)
      elif ".xlsx" in link : #arquivo é do tipo excel
         excel_file = pd.ExcelFile(link)
         sheet_names:list[str] = excel_file.sheet_names
         print(sheet_names,year)
         filtered_sheets:list[str] = list(filter(filter_sheet_names,sheet_names))
         if len(filtered_sheets) > 1:
            raise RuntimeError("Falha ao achar o nome da única sheet do excel correspondente aos dados buscados, mais de uma sheet correspondete foi encontrada")
         correct_sheet:str = filtered_sheets[0]
         df = pd.read_excel(excel_file,sheet_name=correct_sheet)
      else:
         raise RuntimeError("tipo de arquivo do link não é excel ou csv, falha no processamento")
      
      list_dfs_years.append(df,year)

   return list_dfs_years



list_ = __select_all_years()
list_ = __match_links_with_their_years(list_)
list_ = __most_recent_data_by_year(list_)

dfs = dataframes_from_links_and_years(list_)

print(dfs)

"""
TODO
o arquivo de revisão de 2022 está como "2023 corrigido, lembrar disso e colocar alguma lógica para lidar com isso
"""

# result = __get_data_ids_and_years()
# result = __parse_link_year_tuples(result)
# print(result)

# result = __most_recent_data_by_year(result)
# print(result)