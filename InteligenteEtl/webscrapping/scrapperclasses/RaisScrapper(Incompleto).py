from selenium import webdriver
from selenium.webdriver.common.by import By
import time
from etl_config import get_env_var
from selenium.webdriver.support import expected_conditions as EC

URL = "https://bi.mte.gov.br/bgcaged/login.php"
USERNAME = get_env_var("RAIS_USERNAME")
PSSWD = get_env_var("RAIS_PSSWD")

driver:webdriver.Chrome = webdriver.Chrome()
driver.get(URL)

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

submit_element.click()




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
load_queries_button.click()

time.sleep(20)
# Close the browser
driver.quit()
