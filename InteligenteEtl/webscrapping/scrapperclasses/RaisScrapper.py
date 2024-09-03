from selenium import webdriver
from selenium.webdriver.common.by import By
import time
from etl_config import get_env_var

def click_right_query(query_tr_tag,correct_query:str)->bool:
   query_params:list = query_tr_tag.find_elements(By.TAG_NAME,"td") #elementos td com o o input e nome da query
   input_elem = query_params[0].find_element(By.TAG_NAME,"input")
   query_name:str = query_params[1].text

   correct_query = correct_query.lower().replace(" ","")
   query_name = query_name.lower().replace(" ","")

   if query_name == correct_query:
      input_elem.click()
      print("clicou")
      time.sleep(5)
      return True
   else:
      return False

def run_query(driver:webdriver.Chrome)->None:
   pass

URL = "https://bi.mte.gov.br/bgcaged/login.php"
USERNAME = get_env_var("RAIS_USERNAME")
PSSWD = get_env_var("RAIS_PSSWD")

if USERNAME is None or PSSWD is None:
   raise RuntimeError("Username e/ou senha do RAIS não estão no arquivo keys.env no folder etl_config")

driver:webdriver.Chrome = webdriver.Chrome()
driver.get(URL)
driver.maximize_window()

input_elements = driver.find_elements(By.TAG_NAME,"input")


submit_element = None
for ele in input_elements:
   tag_type_attr:str = ele.get_attribute("type").replace(" ","")
   if tag_type_attr == "text": #input do username
      ele.send_keys(USERNAME)
   elif tag_type_attr == "password": #input da senha
      ele.send_keys(PSSWD)
   elif tag_type_attr == "submit": #botão para fazer login
      submit_element = ele

if submit_element is None:
   raise RuntimeError("Falha ao achar o botão de login")

submit_element.click() #logar no portal




time.sleep(1.5)

#clicar no botão do rais
a_element = driver.find_element(By.XPATH, "//a[@href='rais.php']")
a_element.click()
time.sleep(2)

show_job_affiliations = driver.find_element(By.XPATH, "//div[@class='area closedarea' and @headerindex='1h']")
show_job_affiliations.click()
time.sleep(5)


recent_time_series_tag  = driver.find_element(By.XPATH, "//a[contains(text(), 'Ano corrente a 2002')]")
driver.execute_script("arguments[0].click();", recent_time_series_tag)
time.sleep(3)


driver.switch_to.frame(driver.find_element(By.XPATH, "//iframe[@id='iFrm']"))
time.sleep(3)

load_queries_button = driver.find_element(By.XPATH, "//a[.//img[@title='Carrega definição']]")
print(load_queries_button.text)

first_window_handler:str = driver.window_handles[0]
load_queries_button.click() #abre janela de queries

time.sleep(5)

window_handler:str = driver.current_window_handle
windows:list[str] = driver.window_handles
print(windows)

for handler in driver.window_handles:
   if handler != window_handler:
      driver.switch_to.window(handler)
      break
time.sleep(5)

saved_queries_table_div = driver.find_element(By.ID,"divtab")
saved_queries_table_div = saved_queries_table_div.find_element(By.TAG_NAME,"tbody")

saved_queries:list = saved_queries_table_div.find_elements(By.TAG_NAME,"tr")

for tag in saved_queries:
   if click_right_query(tag,"Empregos em TIC 2"):
      break

load_query_elem = driver.find_element(By.ID,"carregar") #clica em carregar a query salva e fecha a janela sobrando
load_query_elem.click()
print("voltou")
time.sleep(5)

window_handlers:list[str] = driver.window_handles
driver.switch_to.window(window_handlers[1]) #muda pra nova janela

print("mudou pra og")
time.sleep(5)
driver.maximize_window()


with open("teste.html","w") as f:
   f.write(driver.page_source)

print(window_handlers)
time.sleep(4)

driver.switch_to.frame(driver.find_element(By.XPATH, "//iframe[@id='iFrm']"))
time.sleep(3)


load_queries_button = driver.find_element(By.XPATH, "//a[.//img[@title='Execução da consulta']]")
load_queries_button.click()

#fechar janela de queries

time.sleep(20)
# Close the browser
driver.quit()
