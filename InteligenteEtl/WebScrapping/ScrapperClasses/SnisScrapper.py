from selenium import webdriver
from selenium.webdriver.common.by import By
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.remote.webelement import WebElement

# Specify the path to the chromedriver executable

# Initialize the Chrome driver

YEARS_EXTRACTED_PER_RUN = 1
MAX_YEARS = 2
INDICATOR_CATEGORIES = ["AE - Informações gerais","AE - Indicadores operacionais - água","AE - Indicadores operacionais - esgotos",
"RS - Informações sobre coleta domiciliar e pública","RS - Informações sobre coleta seletiva e triagem"]


def close_select_window(element)->bool:
   try:
      close_window = element.find_element(By.CLASS_NAME, "ui-multiselect-close")
      close_window.click()
      return True
   except:
      return False

def click_query_button(driver:webdriver.Chrome)->bool:
   WebDriverWait(driver, 10).until(
      EC.presence_of_element_located((By.ID, "botaoconsultar"))
   )
   botaoconsultar_div = driver.find_element(By.ID, "botaoconsultar")

   a_tag = botaoconsultar_div.find_element(By.TAG_NAME, "a")
   a_tag.click()
   time.sleep(2)

def wait_for_progress_bar(driver:webdriver.Chrome, loading_data_table:bool = False)->bool:
   
   if not loading_data_table:
      WebDriverWait(driver, 10).until(
         EC.presence_of_element_located((By.CLASS_NAME, "progress-striped"))
      )
      WebDriverWait(driver, 20).until(
         EC.invisibility_of_element_located((By.CLASS_NAME, "progress-striped"))
      )
      WebDriverWait(driver, 20).until(
         EC.invisibility_of_element_located((By.CLASS_NAME, "blockTitle"))
      )
   else:
      WebDriverWait(driver, 10).until(
         EC.presence_of_element_located((By.CLASS_NAME, "blockTitle"))
      )
      WebDriverWait(driver, 80).until(
         EC.invisibility_of_element_located((By.CLASS_NAME, "blockTitle"))
      )

def get_csv_link(driver:webdriver.Chrome)->str:
   try:
      time.sleep(6)
      generate_table_button: WebElement = driver.find_element(By.ID,"bt_relatorio")
      generate_table_button.click()

      time.sleep(10)
   
      class HrefContainsCsv: #classe para usar no selenium, serve para verificar e esperar caso o link do CSV n apareça
        def __init__(self, locator):
            self.locator = locator

        def __call__(self, driver):
            elements = driver.find_elements(*self.locator)
            for element in elements:
                href = element.get_attribute('href')
                if href and ".csv" in href:
                    return element
            return False

      a_tag = WebDriverWait(driver, 30).until(
        HrefContainsCsv((By.CLASS_NAME, "btn-af"))
      )

      href_link = a_tag.get_attribute('href')
      print("The href link is:", href_link)
      return href_link
   except Exception as e:
      print(e)
      return ""

def select_years(input_elements:list[WebElement], list_of_years:list[int])->bool:
   try:
      for input_element in input_elements:
         year:int = int(input_element.get_attribute('value'))
         if year in list_of_years:
            input_element.click()
      return True
   except:
      return False
      
def select_necessary_indicators(element:WebElement)->bool:
   try: 
      options_list = element.find_elements(By.TAG_NAME,"input")
      parsed_categories:list[str] = list(map(lambda x: x.lower().replace(" ","") ,INDICATOR_CATEGORIES))
      for option in options_list:
         option_name:str = option.get_attribute("title")
         parsed_option:str = option_name.lower().replace(" ","")  
         if parsed_option in parsed_categories:
            option.click()
      return True
   except Exception as e:
      print(f"Falha ao selecionar os indicadores necessários: {e}")
      return False

def __extraction_run(driver:webdriver.Chrome, all_years:list[int] ,first_run:bool =False)->tuple[str,list[int]]:   
   csv_link:str = ""
   try:   
      #clica na aba de dados dos munícipios agrupados
      link_element = driver.find_element(By.XPATH, "//a[@link='/serieHistorica/consolidadoMunicipio/index']")
      time.sleep(2)
      link_element.click() #vai para página com dados dos muncípios
      time.sleep(2)

      WebDriverWait(driver, 10).until(
         EC.presence_of_element_located((By.ID, "ano_ref")) #vai na aba de selecionar os anos dos dados
      )

      select_tag = driver.find_element(By.ID, "ano_ref")
      parent_element = select_tag.find_element(By.XPATH, "..")
      show_years_button = parent_element.find_element(By.TAG_NAME,"button")
      show_years_button.click()
      time.sleep(2)

      #seleciona os anos a serem extraidos
      select_years_div = driver.find_element(By.ID, "multiselect_menu_ano_ref")
      input_elements = select_years_div.find_elements(By.NAME, "multiselect_ano_ref")

      if first_run: #primeira rodada da extração
         for input_element in input_elements:
            year:str = input_element.get_attribute('value')
            all_years.append(int(year))
            if len(all_years) >= MAX_YEARS:
               break

      years_to_extract_in_run = all_years[:YEARS_EXTRACTED_PER_RUN] #anos para serem extraidos nessa rodada
      
      select_years(input_elements,years_to_extract_in_run) #seleciona o anos que vão ser extraidos
      close_select_window(select_years_div)  #fecha a janela de opções de ano

      print("List of years cur:", years_to_extract_in_run)
      print("List of years next:", all_years[YEARS_EXTRACTED_PER_RUN:])

      fieldsets = driver.find_elements(By.CLASS_NAME, "grupo")
      for fieldset in fieldsets:
         legend = fieldset.find_element(By.TAG_NAME, "legend")
         if "município" in legend.text.lower(): #clica no 
               button = fieldset.find_element(By.TAG_NAME, "button")
               button.click()
               wait_for_progress_bar(driver)
               break
   
      WebDriverWait(driver, 10).until(
         EC.presence_of_element_located((By.ID, "multiselect_menu_cod_mun"))
      )

      multiselect_div = driver.find_element(By.ID, "multiselect_menu_cod_mun")

      WebDriverWait(driver, 10).until(
         EC.presence_of_element_located((By.XPATH, ".//a[.//span[text()='Marcar todos']]"))
      )

      marcar_todos_a = multiselect_div.find_element(By.XPATH, ".//a[.//span[text()='Marcar todos']]")
      time.sleep(2)
      marcar_todos_a.click()

      close_window = multiselect_div.find_element(By.CLASS_NAME, "ui-multiselect-close")
      close_window.click()
      
      click_query_button(driver) #clica botão de pesquisa, vai pra segunda parte da busca

      time.sleep(2)
      WebDriverWait(driver, 10).until(
         EC.presence_of_element_located((By.ID, "div_informacao"))
      )
      #acha o elemento <p> com o botão para clicar que mostra as opções de indicadores
      p_element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, "//p[label[@for='cod_fam_info']]"))
      )

      #clica no botão para mostra o menu de selecionar os indicadores
      button = p_element.find_element(By.TAG_NAME, "button")
      button.click()

      indacator_options_div = WebDriverWait(driver, 10).until(
         EC.presence_of_element_located((By.ID, "multiselect_menu_cod_fam_info"))
      )

      time.sleep(3)
      select_necessary_indicators(indacator_options_div) #seleciona os indicadores necessários
      
      time.sleep(2)
      close_select_window(indacator_options_div) #fecha janela de seleção das opções
      time.sleep(2)
      click_query_button(driver)
      wait_for_progress_bar(driver,True)
      time.sleep(2)
      csv_link = get_csv_link(driver)
      return csv_link,years_to_extract_in_run #retorna o link do CSV e a lista de anos a que ele se refere
   
   except Exception as e:
      print(f"Falha ao tentar extrair os anos {years_to_extract_in_run}: {e}")
      return "",[]

def extract_snis()->bool:
   url = "http://app4.mdr.gov.br/serieHistorica/municipio/index/"
   driver = webdriver.Chrome()
   driver.get(url)
   driver.maximize_window()
   years_to_extract_list:list[int] = []
   list_of_csvs_and_their_years:list[tuple[str,list[int]]] = []

   __extraction_run(driver,years_to_extract_list,True) #primeira rodada do algoritmo, pega a lista de todos os anos
   print(years_to_extract_list)
   
   years_to_extract_list = years_to_extract_list[YEARS_EXTRACTED_PER_RUN:]
   while years_to_extract_list:
      driver.refresh()
      time.sleep(3)
      run_result:tuple[str,list[int]] = __extraction_run(driver,years_to_extract_list,False)
      list_of_csvs_and_their_years.append(run_result)
      years_to_extract_list = years_to_extract_list[YEARS_EXTRACTED_PER_RUN:]

extract_snis()