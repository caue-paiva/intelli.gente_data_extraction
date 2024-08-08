from selenium import webdriver
from selenium.webdriver.common.by import By
import time,csv,re, unicodedata
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.remote.webelement import WebElement
import pandas as pd

from datastructures import YearDataPoint
from .AbstractScrapper import AbstractScrapper

class SnisScrapper(AbstractScrapper):
   """
   TODO: completar a classe de scrapper a classe de extrator associada
   """

   URL = "http://app4.mdr.gov.br/serieHistorica/municipio/index/"
   INDICATORS = [
    "IN015_AE",  # Índice de coleta de esgoto
    "IN015_RS",  # Taxa de cobertura do serviço de coleta de resíduo doméstico em relação à população total do município
    "IN022_AE",  # Consumo médio percapita de água
    "IN049_AE",  # Índice de perdas na distribuição
    "IN055_AE",  # Índice de atendimento total de água
    "CS001",     # Existe coleta seletiva formalizada pela prefeitura no município?
    "IN056_AE",  # Índice de atendimento total de esgoto referido aos municípios atendidos com água
    "IN024_AE",  # Índice de atendimento urbano de esgoto referido aos municípios atendidos com água
    "IN053_RS",  # Taxa de material recolhido pela coleta seletiva (exceto mat. orgânica) em relação à quantidade total coletada de resíduos sól. domésticos
    "IN016_AE"   # Índice de tratamento de esgoto
   ]
   EXTRACTED_YEAR_COL = 'Ano de Referência'

   def __close_select_window(self,element)->bool:
      try:
         close_window = element.find_element(By.CLASS_NAME, "ui-multiselect-close")
         close_window.click()
         return True
      except:
         return False

   def __click_query_button(self,driver:webdriver.Chrome)->bool:
      WebDriverWait(driver, 10).until(
         EC.presence_of_element_located((By.ID, "botaoconsultar"))
      )
      botaoconsultar_div = driver.find_element(By.ID, "botaoconsultar")

      a_tag = botaoconsultar_div.find_element(By.TAG_NAME, "a")
      a_tag.click()
      time.sleep(2)

   def __wait_for_progress_bar(self,driver:webdriver.Chrome, loading_data_table:bool = False)->bool:
      
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

   def __get_csv_link(self,driver:webdriver.Chrome)->str:
      print("get_csv_link")
      try:
         time.sleep(15)
         generate_table_button: WebElement = driver.find_element(By.ID,"bt_relatorio")
         generate_table_button.click()
         time.sleep(30)
      
         class HrefContainsCsv: #classe para usar no selenium,dando overwrite na classe padrão de um método ,serve para verificar e esperar caso o link do CSV n apareça
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
         return href_link
      except Exception as e:
         print(e)
         return ""

   def __select_years(self,input_elements:list[WebElement])->bool:
      try:
         for input_element in input_elements:
            #year:int = int(input_element.get_attribute('value'))
            #if year in list_of_years:
            input_element.click()
         return True
      except:
         return False
         
   def __select_necessary_indicators_families(self,element:WebElement)->bool:
      try: 
         options_list = element.find_elements(By.TAG_NAME,"input")
         for option in options_list:
            option.click()
         return True
      except Exception as e:
         print(f"Falha ao selecionar os indicadores necessários: {e}")
         return False

   def __select_necessary_indicators(self,driver:webdriver.Chrome)->bool:
      p_tag = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, "//p[label[@for='fk_glossario']]"))
      )
      print("achou o p")
      
      button = p_tag.find_element(By.TAG_NAME, "button")
      button.click()
      time.sleep(3)

      first_input_num: int = 0
      while True:
         try:
          label = driver.find_element(By.XPATH, f"//label[@for='ui-multiselect-fk_glossario-option-{first_input_num}']")
          input = label.find_element(By.TAG_NAME,"input")
          indicator_text:str = input.get_attribute("title").upper() #deixa em maiusculo por que os indicadores estão em maiúsculo
          for indicator in self.INDICATORS: #todos ja estão selecionados, vamos clicar para deselecionar os que não queremos
            if indicator in indicator_text:  #ele é um dos buscados, não desclica
                 break
          else: #não teve break no for
             input.click() 
             
          first_input_num += 1
         except:
            break
      time.sleep(4)
      menu_div = driver.find_element(By.ID,"multiselect_menu_fk_glossario")
      self.__close_select_window(menu_div)

   def __extraction_run(self,driver:webdriver.Chrome)->str:   
      """
      Realiza uma rodada da extração (carregar a página, selecionar os todos e uma lista de X anos) e extrair esses dados
      Múltiplas rodadas são necessárias pois o sistema da problema quando muitos anos são selecionados de uma vez

      Args:
         driver (webdriver.chrome): driver do selenium pro chrome
      Return:
         tuple[str,list[int]]: link para o arquivo CSV com os dados e os anos contidos nele
      """
      
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

         self.__select_years(input_elements) #seleciona o anos que vão ser extraidos
         self.__close_select_window(select_years_div)  #fecha a janela de opções de ano

         fieldsets = driver.find_elements(By.CLASS_NAME, "grupo")
         for fieldset in fieldsets:
            legend = fieldset.find_element(By.TAG_NAME, "legend")
            if "município" in legend.text.lower(): #clica no 
                  button = fieldset.find_element(By.TAG_NAME, "button")
                  button.click()
                  self.__wait_for_progress_bar(driver)
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
         
         self.__click_query_button(driver) #clica botão de pesquisa, vai pra segunda parte da busca

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

         time.sleep(6)
         self.__select_necessary_indicators_families(indacator_options_div) #seleciona todas as famílias de indicadores
         time.sleep(3)
         self.__close_select_window(indacator_options_div) #fecha janela de seleção das opções
         time.sleep(3)
         self.__select_necessary_indicators(driver) #seleciona os indicadores necessários
         time.sleep(3)

         time.sleep(5)
         print("query")
         self.__click_query_button(driver)
         print("começouu de esperar")
         self.__wait_for_progress_bar(driver,True)
         print("terminou de esperar")
         time.sleep(2)
         csv_link = self.__get_csv_link(driver)
         return csv_link #retorna o link do CSV 
      
      except Exception as e:
         print(f"Falha ao tentar extrair os dados do Snis: {e}")
         return ""

   def __char_is_printable(self,char: str) -> bool:
    category = unicodedata.category(char)
    return category.startswith(('L', 'M', 'N', 'P', 'Z', 'S'))

   def __only_alphanum_or_space(self,input_str:str)->str:
      """
      parsing nas strings para somente conter chars alfa-numericos ou espaço
      """
      return ''.join(char for char in input_str if self.__char_is_printable(char))

   def __parse_col_names_and_vals(self,df:pd.DataFrame)->pd.DataFrame:
      """
      Parsing básico nas colunas do df para remover espaços e outros chars chatos que tornam difícil de trabalhar com as colunas
      """

      cols:list[str] = df.columns
      remove_weird_chars = lambda x: self.__only_alphanum_or_space(re.sub(r'["\n\t\r]', '', x).strip())
      parse_float_nums = lambda x: x.replace(".","").replace(",",".") # '.' é a divisão do milhar (ex: 1.210) no CSV e ',' é a casa decimal
      #o pandas precisa que o . do milhar seja removido e o ',' vire '.'
      parse_cols = lambda x: parse_float_nums(remove_weird_chars(x))
      cols = list(map(parse_cols,cols))
      
      
      df = df.drop(df.columns[-1], axis=1) #tira a ultima coluna do df
      cols = cols[1:] #a primeira coluna de código do IBGE não é lida corretamente, isso faz com que as colunas do DF fiquem erradas, precisamos
      #remover ela e reassinalar as colunas
      df.columns = cols

      df = df.dropna(axis="index",ignore_index=True)
      for col in df.columns:
         df[col] = df[col].apply(parse_cols)

      return df
      
   def __create_datapoints(self,df:pd.DataFrame,years:list[int])->list[YearDataPoint]:
      data_points:list[YearDataPoint] = []
      for year in years:
         df_for_year = df[df[self.EXTRACTED_YEAR_COL] == year]
         data_points.append(
            YearDataPoint(df_for_year,year)
         )
      return data_points

   def extract_database(self) -> list[YearDataPoint]:
      driver = webdriver.Chrome()
      driver.get(self.URL)
      driver.maximize_window()
      csv_link:str =  self.__extraction_run(driver) #primeira rodada do algoritmo, pega a lista de todos os anos
      df = pd.read_csv(csv_link,
                encoding="latin",
                sep=";",
                quotechar='"',
                doublequote=False,
                quoting=csv.QUOTE_MINIMAL,
                engine='python',
      ) 
      df = self.__parse_col_names_and_vals(df) #parsing nos nomes das colunas e valores,
      df = df.reset_index(drop=True) #reseta o index

      df = df[df[self.EXTRACTED_YEAR_COL] != '---']  #tira esse valor estranho da coluna de anos
      df[self.EXTRACTED_YEAR_COL] = df[self.EXTRACTED_YEAR_COL].astype("int")
      time_series_years:list[int] = df[self.EXTRACTED_YEAR_COL].value_counts().index.to_list()

      return self.__create_datapoints(df,time_series_years)
