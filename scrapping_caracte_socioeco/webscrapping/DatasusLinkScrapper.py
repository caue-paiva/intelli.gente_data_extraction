import pandas as pd
from AbstractScrapper import AbstractScrapper
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
import time , os , re 
from enum import Enum

class DatasusAbreviations(Enum):
   """
   Abreviações que o DataSus usa para designar os dados, usados no HTML para identificar botões
   """
   GINI_COEF = "ginibr"
   ILLITERACY_RATE = "alfbr"
    


class DatasusLinkScrapper(AbstractScrapper):
   
   def extract_database(self,website_url: str, data_abrevia:DatasusAbreviations)->list[pd.DataFrame]:
      data_values_col:str = data_abrevia.value #o nome da coluna com os dados no DF será a abreviação de cada dado, vindo do enum

      if data_abrevia == DatasusAbreviations.GINI_COEF:
         driver = webdriver.Chrome()
         driver.get(website_url) 
         time.sleep(3) #espera a página carrega
         html:str = driver.page_source
         link:str = self.__get_link_from_html(html,data_abrevia)
         list_of_years:list[int] = self.__get_years_from_html(driver)
         df =  self.__process_and_clean_df(self._dataframe_from_link(link,data_abrevia),list_of_years,data_values_col)
         return [df]

      else:
         csv_link_list,year_options_list = self.__selenium_page_interaction(website_url,data_abrevia)
         final_df: pd.DataFrame = pd.DataFrame()
         for link ,year in zip(csv_link_list,year_options_list):
            new_df =  self.__process_and_clean_df(self._dataframe_from_link(link,data_abrevia.value),[year],data_values_col )
            final_df = pd.concat(objs=[final_df,new_df],axis="index",ignore_index=True)
         
         return final_df
    
   def download_database_locally(self,website_url: str,data_abrevia:DatasusAbreviations)-> str:
      df:pd.DataFrame =  self.extract_database(website_url,data_abrevia)
      df.to_csv(os.path.join(self.EXTRACTED_FILES_DIR, "datasus_" + data_abrevia.value + ".csv"))

   def __selenium_page_interaction(self, website_url:str,data_abrevia:DatasusAbreviations)->tuple[list[str],list[int]]:
      driver = webdriver.Chrome()
      driver.get(website_url) 
      select_element = driver.find_element(By.ID, 'A') #acha o botão de selecionar os anos

      csv_link_list: list[str] = []
      select_button = Select(select_element) #elemento de selecionar
      year_options_list: list[str] = list(map(lambda x :x.text, select_button.options))
      for year_option in year_options_list:
         csv_link_list.append(self.__get_csv_link_by_year(driver, select_button,data_abrevia,year_option))
      driver.close()

      return csv_link_list, year_options_list

   #overwride no método de achar o df pelo link, pq o csv do datasus é bem quebrado
   def _dataframe_from_link(self, file_url: str, data_abrevia:DatasusAbreviations) -> pd.DataFrame:
      header_row:int
      if data_abrevia == DatasusAbreviations.GINI_COEF:
         header_row = 2
      else:
         header_row = 3
      
      df = pd.read_csv(file_url, encoding="latin-1", sep=";",header=header_row)
      if df is None:
         raise RuntimeError("falha em gerar o df a partir do link")
      
      return df

   def __process_and_clean_df(self,df:pd.DataFrame,list_of_years:list[int],data_value_col:str)->pd.DataFrame:
      df = df.dropna(how = "any", axis= "index") #da drop nos NaN que vem de linhas do CSV com informações sobre os estudos 
      df = df[df["Município"] != "Total"]
      for col in df.columns:
         df[col] = df[col].replace("...",None)
 
      if len(list_of_years) == 1:
         df["ano"] = list_of_years[0]
      else:
         df = pd.melt(df, id_vars=['Município'], var_name='ano', value_name=data_value_col)

      return df

   def __get_csv_link_by_year(self, driver:webdriver.Chrome,select_elem:Select ,data_abrevia:DatasusAbreviations, year_str:str)->str:
      last_two_digits:str = year_str[-2:] #ultimos dois dígitos do número em forma de str
      year_button_identifier: str = data_abrevia.value + last_two_digits + ".dbf"
      
      select_elem.deselect_all()
      select_elem.select_by_value(year_button_identifier)
      time.sleep(1)

      csv_table_button = driver.find_element(By.CLASS_NAME, 'mostra')
      csv_table_button.click() 
      time.sleep(3)
      window_handles = driver.window_handles
      driver.switch_to.window(window_handles[1])

      html:str = driver.page_source
      csv_link:str = self.__get_link_from_html(html,data_abrevia)

      driver.close()
      driver.switch_to.window(window_handles[0])
      
      return csv_link
 
   def __get_link_from_html(self,html:str, abreviation:DatasusAbreviations)->str:
      CSV_LINK_IDENTIFIER: str = ".csv"
      HTTP_REQUEST_STR: str = "http://tabnet.datasus.gov.br"
     
      link_index:int = html.find(CSV_LINK_IDENTIFIER)
      if (link_index == -1):
         raise RuntimeError("Não foi possível achar o link do CSV")
         
      link_end:int = html.find('"',link_index)
      link_start:int = html.rfind('"',0,link_index)

      if abreviation == DatasusAbreviations.GINI_COEF: #caso especial do link pra pegar o coef de gini
         str_end = html[link_start+1:link_end]
         str_end = str_end[2:]
         return HTTP_REQUEST_STR + "/cgi/ibge/censo" + str_end
      else:
         return HTTP_REQUEST_STR + html[link_start+1:link_end]

   def __get_years_from_html(self,driver:webdriver.Chrome)->list[int]:
      td_element = driver.find_element(By.XPATH, '//td[@colspan="3"]')
      inner_text:str = td_element.text

      re_pattern:str = r"(?<!\d)\d{4}(?!\d)"
      list_of_years:list[int] = list(map(int,re.findall(re_pattern,inner_text)))
      return list_of_years



url1 = "http://tabnet.datasus.gov.br/cgi/deftohtm.exe?ibge/censo/cnv/alfbr"
url2 = "http://tabnet.datasus.gov.br/cgi/ibge/censo/cnv/ginibr.def"
abreviation1 = DatasusAbreviations.ILLITERACY_RATE
abreviation2 = DatasusAbreviations.GINI_COEF
scrapper = DatasusLinkScrapper()
df = scrapper.extract_database(url2,abreviation2)

print(df.head())
print(df.info())

df.to_csv(os.path.join("tempfiles","gini_per_capita_processado.csv"))

#df.to_csv(os.path.join("tempfiles","datasus_analfabetismo.csv"))