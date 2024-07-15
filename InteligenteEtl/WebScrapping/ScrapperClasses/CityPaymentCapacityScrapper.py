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
   get_data_year = lambda x: "" if len(re.findall(year_regex_pattern,x)) == 0 else re.findall(year_regex_pattern,x)[0]  #dado "CAPAG Municipios 2024" retorna 2024
   transform_tuples = lambda x: (get_dataset_id(x[0]), get_data_year(x[1])) #aplica ambas as funcoes em uma tupla e retorna uma nova tupla
   
   transformed_list:list[tuple[str]] = list(map(transform_tuples,list_of_tuples))
   transformed_list = list(filter(lambda x: x[1] != "",transformed_list))

   return transformed_list

def __most_recent_data_by_year(list_of_tuples:list[tuple[str]])->list[tuple[str]]:
   """
   Caso um ano apareça mais de uma vez na lista isso significa ou que teve um correção dos dados ou que os dados estão mensais ainda 
   (o ano não acabou), em ambos os casos é melhor pegar o ultimo ID do dataset desse ano
   """

   non_repeated_years =  {} #dict para guardar os anos únicos como key e seu primeiro id como value
   list_of_tuples.reverse() #reverte a lista, indo do mais recente pro mais novo

   for data_id,year in list_of_tuples:
      if year not in non_repeated_years: #caso o ano nao tenha aparecido
         non_repeated_years[year] = data_id
   
   return [(data_id,year) for year,data_id in non_repeated_years.items()] #transforma o dict em uma lista de tuplas denovo

# Output the result
result = __parse_link_year_tuples(result)
print(result)

result = __most_recent_data_by_year(result)
print(result)