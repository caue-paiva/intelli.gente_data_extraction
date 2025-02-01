from .AbstractScrapper import AbstractScrapper
from datastructures import YearDataPoint
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
import time,os
import pandas as pd


URL = "http://www.atlasbrasil.org.br/ranking"

class IdhScrapper(AbstractScrapper):
  
   DOWNLOADED_FILE_NAME = "data.xlsx"

   def __setup_webdriver(self)->webdriver.Chrome:
      
      CHROME_DRIVER_PREFEREN =  {
         "safebrowsing.enabled": False,  
         "safebrowsing.disable_download_protection": True, 
         "download.default_directory": self.DOWNLOADED_FILES_PATH,  
         "profile.default_content_settings.popups": 0,
         "download.prompt_for_download": False,  
         "download.directory_upgrade": True,
         "profile.default_content_setting_values.automatic_downloads": [1, {"behavior": 'allow'}] 
      }
      CHROME_DRIVER_ARGUMENTS = [
         "--allow-running-insecure-content",
         f"--unsafely-treat-insecure-origin-as-secure={URL}",
         "--headless",
         "--disable-gpu",
         "--no-sandbox",
         "--disable-dev-shm-usage"
      ]

      options = webdriver.ChromeOptions()
      for arg in CHROME_DRIVER_ARGUMENTS:
         options.add_argument(arg) 
      options.add_experimental_option("prefs", CHROME_DRIVER_PREFEREN)
      driver = webdriver.Chrome(options=options)  # Replace with the appropriate driver for your browser if not Chrome
      driver.get(URL)  

      return driver
   
   def __get_year_data_collection(self, year:int)->YearDataPoint:
         """
         Le o arquivo excel extraido, transforma ele num dataframe e depois deleta ele
         """
         #asprint(year)
         file_path:str = os.path.join(self.DOWNLOADED_FILES_PATH,self.DOWNLOADED_FILE_NAME)
         df = pd.read_excel(file_path)
         os.remove(file_path)
         return YearDataPoint(df=df,data_year=year)

   def extract_database(self)->list[YearDataPoint]:
         driver = self.__setup_webdriver() #cria e configura o driver do chrome para a webscrapping
         super()._create_downloaded_files_dir()
         
         #seleciona os dados para serem a nível do município
         select_cities = Select(driver.find_element(By.ID, 'camadaR'))
         select_cities.select_by_visible_text('Municípios')
         time.sleep(3)

         #cria o select dos anos
         select_years = Select(driver.find_element(By.ID, 'anoR'))

         data_points:list[YearDataPoint] = []
     
         for option in select_years.options:
            option.click() #clica na opção dquela ano
            time.sleep(3)

            button = driver.find_element(By.XPATH, '//button[@onclick="listarConsulta()"]') #acha o botão para baixar os arquivos
            button.click()
            time.sleep(6)
            data_point:YearDataPoint = self.__get_year_data_collection(int(option.text)) #extrai o df e cria um objeto YearDataPoint
            data_points.append(data_point)

         super()._delete_download_files_dir()
         return data_points


   
