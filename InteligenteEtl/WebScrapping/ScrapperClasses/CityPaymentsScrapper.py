import re, os
import pandas as pd
from DataEnums.DataEnums import BaseFileType
from .AbstractScrapper import AbstractScrapper
from selenium import webdriver
from selenium.webdriver.common.by import By


class CityPaymentsScrapper(AbstractScrapper):

   URL = "https://www.tesourotransparente.gov.br/ckan/dataset/capag-municipios/resource/6a218451-f1b4-4fce-ac2a-00a3675bf4eb?inner_span=True"

   def __most_recent_data_by_year(self,list_of_tuples:list[tuple[str,int]])->list[tuple[str,int]]:
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

   def __extract_csv_link(self,driver:webdriver.Chrome,new_window_link:str)->str:
      
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

   def __select_all_years(self):
      driver = webdriver.Chrome()  # Update the path to your chromedriver
      driver.maximize_window()

      # Load the webpage
      driver.get(self.URL)

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

   def __fill_non_existent_years(self,list_of_link_year_tuples:list[tuple[str,int]])->None:
      previous_year:int = 0
      for i in range(len(list_of_link_year_tuples)):
         link:str = list_of_link_year_tuples[i][0]
         year:int = list_of_link_year_tuples[i][1]
         if year == 0: #ano nulo
            list_of_link_year_tuples[i] = (link,previous_year+1)  #coloca o valor do ano como ano anterior +1
            continue #ano anterior continua o mesmo
         
         previous_year = year #ano não nulo, atualiza o ano anterior

   def __match_links_with_their_years(self,list_of_links:list[str])->list[tuple[str,int]]:
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

   def __dataframes_from_links_and_years(self,list_of_link_year_tuples:list[tuple[str,int]])->list[tuple[pd.DataFrame,str]]:
      list_dfs_years:list[tuple[pd.DataFrame,str]] = []
      filter_non_complete_sheet_names = lambda x: True if "prévia" in x.lower() else False #arquivos de anos não finalizados (ex: 2024 17/05) tem prévia como o nome da sheet
      #do arquivo excel com prévia no nome
      CSV_SEP = ";" #separador do CSV
      EXCEL_HEADER_LINE = 2 #linha do arquivo excel onde está o nome das colunas

      for link,year in list_of_link_year_tuples:
         df:pd.DataFrame
         if "csv" in link:
            df = pd.read_csv(link,sep = CSV_SEP)
         elif ".xlsx" in link : #arquivo é do tipo excel
            excel_file = pd.ExcelFile(link)
            sheet_names:list[str] = excel_file.sheet_names
            correct_sheet = ""
            
            if len(sheet_names) > 1: #caso de dados não representantes do ano todo (ex: 2024 17/05)
               filtered_sheets:list[str] = list(filter(filter_non_complete_sheet_names,sheet_names))
               if len(filtered_sheets) > 1:
                  raise RuntimeError("Falha ao achar sheet única do arquivo de excel para processar")
               correct_sheet = filtered_sheets[0]
               df = pd.read_excel(excel_file,sheet_name=correct_sheet,header=EXCEL_HEADER_LINE)

            else:
               correct_sheet = sheet_names[0]
               df = pd.read_excel(excel_file,sheet_name=correct_sheet)
         else:
            raise RuntimeError("tipo de arquivo do link não é excel ou csv, falha no processamento")
         
         print(year,df.info())
         list_dfs_years.append( (df,year) )

      return list_dfs_years

   def extract_database(self)->list[tuple[pd.DataFrame,str]]:
      links:list[str] = self.__select_all_years()
      links_and_years:tuple[str,int] = self.__match_links_with_their_years(links)
      links_and_years:tuple[str,int] = self.__most_recent_data_by_year(links_and_years)
      dfs = self.__dataframes_from_links_and_years(links_and_years)

      return dfs

   def download_database_locally(self,path:str= "") -> str:
      dfs = self.extract_database()
      for i in range(len(dfs)):
         if path:
            dfs[i][0].to_csv(os.path.join(path,f"CAPAG{i}"))
         else:
            dfs[i][0].to_csv(f"CAPAG{i}")
      
      if path:
         return path #caso o path seja dado retorna ele mesmo
      else:
         return os.getcwd() # retorna diretório atual caso o path n seja dado



obj = CityPaymentsScrapper()
obj.download_database_locally()


"""
TODO
o arquivo de revisão de 2022 está como "2023 corrigido, lembrar disso e colocar alguma lógica para lidar com isso
"""

# result = __get_data_ids_and_years()
# result = __parse_link_year_tuples(result)
# print(result)

# result = __most_recent_data_by_year(result)
# print(result)