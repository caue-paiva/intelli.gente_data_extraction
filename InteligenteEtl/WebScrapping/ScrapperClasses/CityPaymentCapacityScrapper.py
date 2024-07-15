import requests, re
from bs4 import BeautifulSoup
"""
https://www.tesourotransparente.gov.br/ckan/dataset/9ff93162-409e-48b5-91d9-cf645a47fdfc/resource/86636c19-b38a-4b9e-8fff-30fc4208dd04/download/CAPAG-Municipios---Ano-Base-2022.xlsx
"""
URL = "https://www.tesourotransparente.gov.br/ckan/dataset/capag-municipios/resource/6a218451-f1b4-4fce-ac2a-00a3675bf4eb?inner_span=True"

response = requests.get(url=URL)
if response.status_code != 200:
   raise RuntimeError("Falha na request para pegar o HTML do server")

html_text: str = response.text
soup = BeautifulSoup(html_text, 'html.parser')

# Find the ul element with the class "unstyled nav nav-simple"
ul_element = soup.find('ul', class_='unstyled nav nav-simple')

# Initialize a list to store the tuples
result = []

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

def __parse_link_year_tuples(list_of_tuples:list[tuple[str]])->list[tuple[str]]:
   
   
   get_dataset_id = lambda x: x[x.rfind("/")+1: x.rfind("?")] #ex: dado a str: capag-municipios/resource/86636c19-b38a-4b9e-8fff-30fc4208dd04?inner_span=True retorna 
   #86636c19-b38a-4b9e-8fff-30fc4208dd04

   year_regex_pattern = r'\b\d{4}\b'
   get_data_year = lambda x: re.findall(year_regex_pattern,x) #dado "CAPAG Municipios 2024" retorna 2024
   transform_tuples = lambda x: (get_dataset_id(x[0]), get_data_year(x[1])) #aplica ambas as funcoes em uma tupla e retorna uma nova tupla
   
   return list(map(transform_tuples,list_of_tuples))
 

# Output the result
result = __parse_link_year_tuples(result)
print(result)