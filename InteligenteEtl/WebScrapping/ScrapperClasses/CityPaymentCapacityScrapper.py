import requests, re
from bs4 import BeautifulSoup

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

def __most_recent_data_by_year(list_of_tuples:list[tuple[str]])->list[tuple[str]]:
   """
   Caso um ano apareça mais de uma vez na lista isso significa ou que teve um correção dos dados ou que os dados estão mensais ainda 
   (o ano não acabou), em ambos os casos é melhor pegar o ultimo ID do dataset desse ano

   Args:
      list_of_tuples (list[tuple[str]]): lista de tuplas com o id dos dados e o ano
   Return:
      list[tuple[str]]: lista de tuplas com o id dos dados e o ano, mas sem anos repetidos
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
        print(f'Extracted href: {href}')
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
   for ele in li_elements:
      a_tag = ele.find_element(By.TAG_NAME, 'a')
      link = a_tag.get_attribute('href')
      element_text:str = ele.text
     
      year_regex_matches:list[str] = re.findall(year_regex_pattern,element_text)
      if not year_regex_matches:
         continue
      year: str = year_regex_matches[0]
      __extract_csv_link(driver,link)

     # Open the link in a new window

  

   # Optionally, close the browser
   driver.quit()

__select_all_years()

"""
TODO
o arquivo de revisão de 2022 está como "2023 corrigido, lembrar disso e colocar alguma lógica para lidar com isso
"""

# result = __get_data_ids_and_years()
# result = __parse_link_year_tuples(result)
# print(result)

# result = __most_recent_data_by_year(result)
# print(result)