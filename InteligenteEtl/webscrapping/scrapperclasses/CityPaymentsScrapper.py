import re
import pandas as pd
from .AbstractScrapper import AbstractScrapper
from selenium import webdriver
from selenium.webdriver.common.by import By
from datastructures import YearDataPoint


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
         print(f'Erro ao extrair href: {e}')

      # Close the new window
      driver.close()
      driver.switch_to.window(driver.window_handles[0])
      return href

   def __select_all_years(self):
      options = webdriver.ChromeOptions()
      options.add_argument("--headless")  # Enable headless mode
      options.add_argument("--disable-gpu")  # Disable GPU for compatibility
      options.add_argument("--no-sandbox")  # Required for some environments
      options.add_argument("--disable-dev-shm-usage")  # Prevent memory issues
      options.add_argument("--window-size=1920,1080")  # Simulate maximized window size
      driver = webdriver.Chrome(options=options)  # Update the path to your chromedriver


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
         csv_link:str = self.__extract_csv_link(driver,link)
         list_of_csv_links.append(csv_link)
      
      driver.quit()
      return list_of_csv_links  

   def __fill_non_existent_years(self,list_of_link_year_tuples:list[tuple[str,int]])->None:
      """
      Alguns links não tem o ano dos dados, mas como os links são extraidos em ordem é possível inferir qual o ano
      baseado no ano anterior.
      """
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
      replace_incorrect_numbers = lambda x: (x[0], x[1]) if x[1] < 2050 else (x[0],NON_FOUND_YEAR) #caso um número  extraido não seja um ano ele vai ser trocado por um valor que representa um ano inválido
      #esse tratament existe por que algumas vezes não tem o ano do dado no link mas tem outros padrões numéricos que o regex pega

      fix_year_of_corrected_data = lambda x: (x[0],x[1]) if "corrigido" not in x[0] else (x[0], x[1]-1) #caso o dado seja corrigido, ele tem um ano no nome mas se refere ao ano anterior
      #pelo menos isso é o caso nos dados corrigidos de 2022

      compose_funcs = lambda x: fix_year_of_corrected_data(replace_incorrect_numbers(get_link_year_tuple(x))) #compoe as funções acima

      list_of_tuples:list[tuple[str,int]] = list(map(compose_funcs,list_of_links)) #aplica as funções na lista de links
      self.__fill_non_existent_years(list_of_tuples)

      return list_of_tuples

   def __dataframes_from_links_and_years(self,list_of_link_year_tuples:list[tuple[str,int]])->list[tuple[pd.DataFrame,int]]:
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
         
         list_dfs_years.append( (df,int(year)))

      return list_dfs_years

   def extract_database(self)->list[YearDataPoint]:
      links:list[str] = self.__select_all_years()
      links_and_years:tuple[str,int] = self.__match_links_with_their_years(links)
      links_and_years:tuple[str,int] = self.__most_recent_data_by_year(links_and_years)
      dfs = self.__dataframes_from_links_and_years(links_and_years)
     
      list_of_data_points:list[YearDataPoint] = list(map(YearDataPoint.from_tuple,dfs)) #transforma a lista de tuplas em uma lista de datapoints
      
      return list_of_data_points
