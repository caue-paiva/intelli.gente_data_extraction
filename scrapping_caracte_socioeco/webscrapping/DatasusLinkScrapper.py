import pandas as pd
from AbstractScrapper import AbstractScrapper, BaseFileType
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
import time , os
from enum import Enum

class DatasusAbreviations(Enum):
   """
   Abreviações que o DataSus usa para designar os dados, usados no HTML para identificar botões
   """
   GINI_COEF = "ginibr"
   ILLITERACY_RATE = "alfbr"
    


class DatasusLinkScrapper(AbstractScrapper):
   
   @classmethod
   def extract_database(cls,website_url: str, data_abreviation:DatasusAbreviations)-> pd.DataFrame:
      driver = webdriver.Chrome()
      driver.get(website_url) 
      select_element = driver.find_element(By.ID, 'A') #acha o botão de selecionar os anos

      csv_link_list: list[str] = []
      select_button = Select(select_element) #elemento de selecionar
      year_options_list: list[str] = list(map(lambda x :x.text, select_button.options))
      for year_option in year_options_list:
         csv_link_list.append(cls.__get_csv_link_by_year(driver, select_button,data_abreviation,year_option))
      driver.close()

      final_df: pd.DataFrame = pd.DataFrame()
      for link ,year in zip(csv_link_list,year_options_list):
         print(link)
         new_df = cls._dataframe_from_link(link)
         new_df["ano"] = year
         final_df = pd.concat(objs=[final_df,new_df],axis="index",ignore_index=True)
      
      return final_df
   """
   @classmethod   
   def download_database_locally(cls,website_url: str,data_abreviation:DatasusAbreviations)-> str:
      df:pd.DataFrame =  cls.extract_database(website_url,data_abreviation)
      df.to_csv(os.path.join(cls.EXTRACTED_FILES_DIR, "datasus_" + data_abreviation.value + ".csv"))
   """
   #overwride no método de achar o df pelo link, pq o csv do datasus é bem quebrado
   @classmethod
   def _dataframe_from_link(cls, file_url: str) -> pd.DataFrame:
      df = pd.read_csv(file_url, encoding="latin-1", sep=";",header=3)
      if df is None:
         raise RuntimeError("falha em gerar o df a partir do link")
      
      df = df.dropna(how = "any", axis= "index")
      df = df[df["Município"] != "Total"]

      return df
      
   @classmethod
   def __get_csv_link_by_year(cls, driver:webdriver.Chrome,select_elem:Select ,data_abreviation:DatasusAbreviations, year_str:str)->str:
      last_two_digits:str = year_str[-2:] #ultimos dois dígitos do número em forma de str
      year_button_identifier: str = data_abreviation.value + last_two_digits + ".dbf"
      
      select_elem.deselect_all()
      select_elem.select_by_value(year_button_identifier)
      time.sleep(1)

      csv_table_button = driver.find_element(By.CLASS_NAME, 'mostra')
      csv_table_button.click() 
      time.sleep(3)
      window_handles = driver.window_handles
      driver.switch_to.window(window_handles[1])

      html:str = driver.page_source
      csv_link:str = cls.__csv_link_from_html(html)

      driver.close()
      driver.switch_to.window(window_handles[0])
      
      return csv_link
 
   def __csv_link_from_html(html:str)->str:
      CSV_LINK_IDENTIFIER: str = ".csv"
      HTTP_REQUEST_STR: str = "http://tabnet.datasus.gov.br"

      link_index:int = html.find(CSV_LINK_IDENTIFIER)
      if (link_index == -1):
         raise RuntimeError("Não foi possível achar o link do CSV")
      
      link_end:int = html.find('"',link_index)
      link_start:int = html.rfind('"',0,link_index)

      return HTTP_REQUEST_STR + html[link_start+1:link_end]
   

url = "http://tabnet.datasus.gov.br/cgi/deftohtm.exe?ibge/censo/cnv/alfbr"
abreviation = DatasusAbreviations.ILLITERACY_RATE
DatasusLinkScrapper.extract_database(url,abreviation)

#df.to_csv(os.path.join("tempfiles","datasus_analfabetismo.csv"))