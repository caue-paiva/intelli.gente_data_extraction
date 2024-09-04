from selenium import webdriver
from selenium.webdriver.common.by import By
from datastructures import YearDataPoint, DataTypes
from .AbstractScrapper import AbstractScrapper
from selenium.webdriver.chrome.options import Options
import pandas as pd
import time,os
from enum import Enum
from etl_config import get_env_var

class RaisDataInfo(Enum):
   TECH_JOBS = {
      "data_identifier":"Empregos em TIC",
      "saved_query":"Empregos em TIC 2",
      "topic":"Inovação",
      "dtype": DataTypes.INT,
   }
   TECH_COMPANIES = {
      "data_identifier":"Empresas de TICs no municipio",
      "saved_query":"Empresas de TIC",
      "topic":"Inovação",
      "dtype":DataTypes.INT,
   }
   TOURISM_JOBS = {
      "data_identifier":"Empregos em Turismo",
      "saved_query":"Empregos em Turismo",
      "topic":"Território",
      "dtype":DataTypes.INT,
   }



class RaisScrapper(AbstractScrapper):
   URL = "https://bi.mte.gov.br/bgcaged/login.php"
   USERNAME = get_env_var("RAIS_USERNAME")
   PSSWD = get_env_var("RAIS_PSSWD")

   DATA_YEAR = 2022 #por enquanto esse dado está hardcoded para extrair apenas o ano de 2022 (extrair todos os anos do jeito atual
   #iria demorar mto tempo)

   data_point_to_extract:RaisDataInfo

   def __init__(self,data_point_to_extract:RaisDataInfo) -> None:
      if self.USERNAME is None or self.PSSWD is None:
         raise RuntimeError("Username e/ou senha do RAIS não estão no arquivo keys.env no folder etl_config")
      self.data_point_to_extract = data_point_to_extract

   def __click_right_query(self,query_tr_tag,correct_query:str)->bool:
      query_params:list = query_tr_tag.find_elements(By.TAG_NAME,"td") #elementos td com o o input e nome da query
      input_elem = query_params[0].find_element(By.TAG_NAME,"input")
      query_name:str = query_params[1].text

      correct_query = correct_query.lower().replace(" ","")
      query_name = query_name.lower().replace(" ","")

      if query_name == correct_query:
         input_elem.click()
         time.sleep(5)
         return True
      else:
         return False

   def __finalize_query(self,driver:webdriver.Chrome)->None:
      """
      Clica no botão de queries, espera a janela de carregar dados passar e clicar no botão pra baixar o CSV
      """
      load_query_elem = driver.find_element(By.ID,"carregar") #clica em carregar a query salva e fecha a janela sobrando
      load_query_elem.click()
      time.sleep(5)

      window_handlers:list[str] = driver.window_handles
      driver.switch_to.window(window_handlers[1]) #muda pra nova janela

      time.sleep(5)
      driver.maximize_window()

      time.sleep(4)

      driver.switch_to.frame(driver.find_element(By.XPATH, "//iframe[@id='iFrm']"))
      time.sleep(3)


      load_queries_button = driver.find_element(By.XPATH, "//a[.//img[@title='Execução da consulta']]")
      old_num_windows:int = len(driver.window_handles)
      load_queries_button.click()
      new_num_windows:int = len(driver.window_handles)

      while new_num_windows != old_num_windows: #loop enquanto a tabela de realizar query está aparecendo
         time.sleep(2)
         new_num_windows:int = len(driver.window_handles)

      time.sleep(5) #espera nova página dos resultados carregar

      html = driver.page_source #sem isso o código não funciona!
      #obs: deve ser pq quando vc faz isso o selenium deleta a janela stale (n existe mais) e seleciona a janela certa

      driver.switch_to.frame(driver.find_element(By.NAME, "botoes")) #vai pro prox frame
      time.sleep(5)

      city_codes_elem = driver.find_element(By.XPATH, "//img[@name='ROT']") #exporta o arquivo
      windows_before_click:set[str] = set(driver.window_handles)
      old_window_handler:str = driver.current_window_handle #salva janela antiga

      city_codes_elem.click() #clica para abrir os tópicos 
      time.sleep(5)
      windows_after_click:set[str] = set(driver.window_handles)

      new_window:str = list(windows_after_click.difference(windows_before_click))[0] #acha a janela nova
      driver.switch_to.window(new_window) #vai para nova janela

      a_tags = driver.find_elements(By.CSS_SELECTOR, "a.TDN")
      for tag in a_tags:
         text:str = tag.text.replace(" ","").lower()
         if text == "código":
            tag.click() #clica na tag certa
            print("clicou")


      td_element = driver.find_element(By.XPATH, "//td[@align='CENTER']")
      confirm_elem = td_element.find_element(By.TAG_NAME,"a")
      confirm_elem.click() #clica no botão verde de confirmar seleção
      time.sleep(5)

      driver.switch_to.window(old_window_handler) #muda pra janela antiga
      time.sleep(3)

      driver.switch_to.frame(driver.find_element(By.NAME, "botoes")) #vai pro prox frame
      time.sleep(5)

      img_element = driver.find_element(By.XPATH, "//img[@name='EXCEL']") #exporta o arquivo
      img_element.click()

      time.sleep(10) #espera baixar o arquivo

   def __login(self,driver:webdriver.Chrome)->None:
      input_elements = driver.find_elements(By.TAG_NAME,"input")
      submit_element = None
      for ele in input_elements:
         tag_type_attr:str = ele.get_attribute("type").replace(" ","")
         if tag_type_attr == "text": #input do username
            ele.send_keys(self.USERNAME)
         elif tag_type_attr == "password": #input da senha
            ele.send_keys(self.PSSWD)
         elif tag_type_attr == "submit": #botão para fazer login
            submit_element = ele

      if submit_element is None:
         raise RuntimeError("Falha ao achar o botão de login")

      submit_element.click() #logar no portal
      time.sleep(1.5)
 
   def __run_saved_query(self,saved_query_name:str):
      #baixar arquivos zips
      downloaded_files_dir: str = self.DOWNLOADED_FILES_PATH 
      chrome_options = Options()
      chrome_options.add_experimental_option("prefs", {
            "download.default_directory": downloaded_files_dir,  # Set the download directory
            "download.prompt_for_download": False,  # Disable the prompt for download
            "download.directory_upgrade": True,  # Ensure directory upgrade
            "safebrowsing.enabled": False  # Enable safe browsing
      })

      # Set up the Chrome driver
      driver = webdriver.Chrome(options=chrome_options)
      driver.get(self.URL)
      driver.maximize_window()
      self.__login(driver)

      #entrar na página de selecionar queries
      #clicar no botão do rais
      a_element = driver.find_element(By.XPATH, "//a[@href='rais.php']")
      a_element.click()
      time.sleep(2)

      #clica em RAIS VINCULOS
      show_job_affiliations = driver.find_element(By.XPATH, "//div[@class='area closedarea' and @headerindex='1h']")
      show_job_affiliations.click()
      time.sleep(5)

      #seleciona a série histórica correta 
      recent_time_series_tag  = driver.find_element(By.XPATH, "//a[contains(text(), 'Ano corrente a 2002')]")
      driver.execute_script("arguments[0].click();", recent_time_series_tag)
      time.sleep(3)

      #muda para o iframe correto
      driver.switch_to.frame(driver.find_element(By.XPATH, "//iframe[@id='iFrm']"))
      time.sleep(3)

      #abre janela de queries salvas
      load_queries_button = driver.find_element(By.XPATH, "//a[.//img[@title='Carrega definição']]")
      load_queries_button.click() 
      time.sleep(5)

      window_handler:str = driver.current_window_handle
      for handler in driver.window_handles:
         if handler != window_handler:
            driver.switch_to.window(handler)
            break
      
      time.sleep(5)

      saved_queries_table_div = driver.find_element(By.ID,"divtab")
      saved_queries_table_div = saved_queries_table_div.find_element(By.TAG_NAME,"tbody")

      saved_queries:list = saved_queries_table_div.find_elements(By.TAG_NAME,"tr")
      for tag in saved_queries:
         if self.__click_right_query(tag,saved_query_name):
            break
      else:
         raise RuntimeError("Falha ao achar query Salva no site")

      self.__finalize_query(driver) #roda a query, espera ela carregar e baixa o CSV
      # Close the browser
      driver.quit()

   def __get_df(self)->pd.DataFrame:
      files:list[str] = os.listdir(self.DOWNLOADED_FILES_PATH)

      if len(files) > 1:
         raise RuntimeError("Mais de um arquivo do folder de arquivos extraidos, não é possível saber qual deles está correto")
      file_name:str = files[0]

      if ".csv" not in file_name:
         raise RuntimeError("Arquivo não é .csv")
      
      file_path:str = os.path.join(self.DOWNLOADED_FILES_PATH,file_name)
      return pd.read_csv(file_path,sep=";",encoding="latin-1",on_bad_lines='skip')

   def extract_database(self) -> list[YearDataPoint]:
      self._create_downloaded_files_dir() #criar folder pros arquivos baixados
      self.__run_saved_query(self.data_point_to_extract.value["saved_query"]) #faz webscrapping do site com uma query especifica 
      time.sleep(5)
      df:pd.DataFrame = self.__get_df() #pega o df do csv
      self._delete_download_files_dir() #deleta folder dos arquivos baixados
      data_point = YearDataPoint(
         df=df,
         data_year=self.DATA_YEAR
      )
      return [data_point]