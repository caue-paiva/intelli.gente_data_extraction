
from selenium import webdriver
from selenium.webdriver.common.by import By
import time,re
from .AbstractScrapper import AbstractScrapper
from typing import Iterable

"""
Essa classe foi inicialmente criada para extrair os dados: Escala de acesso a banda larga fixa  e Escala de acesso a banda larga móvel
porém a extração pelo site não funcionou devido ao arquivo zip extraido tem 650mb. O código da função __get_link() da classe abaixo pode ainda servir para 
extrair outros dados da página que estejam na mesma aba (que apareceu ao clicar o botão dados brutos) que os dados anteriores
"""


class AnatelFixedConnectionScrapper(AbstractScrapper):

   URL = "https://informacoes.anatel.gov.br/paineis/acessos"


   def __get_link(self)->str:
      print("link")
      driver = webdriver.Chrome()

      driver.get(self.URL)

      time.sleep(20)
      print("terminou de esperar")

      target_li = driver.find_element(By.XPATH, '//ul[@class="menu"]//li[@class="item-170 deeper parent"]')
      target_class = target_li.get_attribute('class')

      print(f"The target <li> class is: {target_class}")
      a_tag = target_li.find_element(By.TAG_NAME,"a")
      a_tag.click()
      time.sleep(5)

      data_list_tag = target_li.find_element(By.TAG_NAME,"ul")
      print(data_list_tag.get_attribute("outerHTML"))

      data_options = data_list_tag.find_elements(By.TAG_NAME,"li")

      correct_element = None
      for option in data_options:
         if option.text == "Banda Larga Fixa":
            correct_element = option
            break

      final_a_tag = correct_element.find_element(By.TAG_NAME,"a")
      link = final_a_tag.get_attribute("href")

      driver.quit()
      return link

   def yield_csv_files(self,files:list[str])->Iterable[ tuple[str,int]]:
      print("func")

      year_regex_pattern = r"(\d{4})"
      get_data_year = lambda x: re.findall(year_regex_pattern,x)

      for file in files:
         data_year = get_data_year(file)
         if not data_year: #não achou o ano dos dados
            continue
         yield (file,data_year[0])

