import pandas as pd
from AbstractScrapper import AbstractScrapper, BaseFileType
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
import time 
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
      for year_option in select_button.options:
         csv_link_list.append(cls.__get_csv_link_by_year(driver, select_button,data_abreviation,year_option))
      driver.close()

      final_df: pd.DataFrame = pd.DataFrame()
      for link in csv_link_list:
         new_df = super()._dataframe_from_link(link,BaseFileType.CSV,False)
         final_df = pd.concat(objs=[final_df,new_df],axis="index",ignore_index=True)
      
      return final_df
   
   @classmethod   
   def download_database_locally(cls,website_url: str)-> str:
      return super().download_database_locally(website_url,BaseFileType.CSV)
   
   @classmethod
   def __get_csv_link_by_year(cls, driver:webdriver.Chrome,select_elem:Select ,data_abreviation:DatasusAbreviations, year_str:str)->str:
      last_two_digits:str = year_str[-2:] #ultimos dois dígitos do número em forma de str
      year_button_identifier: str = data_abreviation.value + last_two_digits + ".dbf"
      print(year_button_identifier)

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

      link_index:int = html.find(CSV_LINK_IDENTIFIER)
      if (link_index == -1):
         raise RuntimeError("Não foi possível achar o link do CSV")
      
      link_end:int = html.find('"',link_index)
      link_start:int = html.rfind('"',0,link_index)

      return html[link_start+1:link_end]
      

def selenium_test():
   # Initialize the Chrome WebDriver
   driver = webdriver.Chrome()

   # Open the web page containing the select element
   driver.get("http://tabnet.datasus.gov.br/cgi/tabcgi.exe?ibge/censo/cnv/alfbr.def")  # Replace with the actual URL

   # Locate the select element
   select_element = driver.find_element(By.ID, 'A')

   # Create a Select object
   select = Select(select_element)
   print(select.options)
   # Select options by value
   select.select_by_value('alfbr10.dbf')
   time.sleep(1)
   # select.select_by_value('alfbr00.dbf')
   # time.sleep(1)
   # select.select_by_value('alfbr91.dbf')

   # Wait to see the selected options (optional)
   time.sleep(3)

   mostra_button = driver.find_element(By.CLASS_NAME, 'mostra')

   # Click the "Mostra" button
   mostra_button.click()
   window_handles = driver.window_handles

   # Switch to the new tab
   driver.switch_to.window(window_handles[1])

   # Perform actions in the new tab
   # Example: print the title of the new tab
   print(driver.title)

   # Wait to observe actions in the new tab (optional)
   time.sleep(3)

   # Switch back to the original tab
   driver.switch_to.window(window_handles[0])

   # Perform actions in the original tab
   # Example: print the title of the original tab
   print(driver.title)

   # Wait to observe actions in the original tab (optional)
   time.sleep(3)

   # Close the browser
   driver.quit()

driver = webdriver.Chrome()

   # Open the web page containing the select element
driver.get("http://tabnet.datasus.gov.br/cgi/tabcgi.exe?ibge/censo/cnv/alfbr.def")  # Replace with the actual URL
select_element = driver.find_element(By.ID, 'A')

   # Create a Select object
select = Select(select_element)
for option in select.options:
   print(option.text)


#DatasusLinkScrapper.__get_csv_link_by_year(driver,select,DatasusAbreviations.ILLITERACY_RATE,2010)