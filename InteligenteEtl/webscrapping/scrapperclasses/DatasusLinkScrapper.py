import pandas as pd
from .AbstractScrapper import AbstractScrapper
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from datastructures import DataTypes, YearDataPoint
import time , os , re 
from enum import Enum

class DatasusDataInfo(Enum):
   """
   Enums para cada dado a ser extraido do datasus, contém a abreviação que o DataSus usa para designar os dados 
   no HTML para identificar botões que quando clicados levam ao link do csv.
   Também tem outras infos como o nome do dado, seu tópico, tipo e se é necessário selecionar opções de linha,coluna e conteúdo na página para baixar o dado
   certo
   
   """
   GINI_COEF = {
      "url":"http://tabnet.datasus.gov.br/cgi/ibge/censo/cnv/ginibr.def",
      "data_abrev":"ginibr",
      "data_name":"Índice de GINI da renda domiciliar per capita",
      "data_topic": "saúde",
      "content_to_select":[], #botões de conteúdo para clicar
      "columns_to_select":[], #botões de colunas para clicar
      "lines_to_select":[],  #botões de linha para clicar
      "monthly_data":False, #dado é mensal, se não for é anual
      "dtype":DataTypes.FLOAT
   }
   ILLITERACY_RATE =  {
      "url":"http://tabnet.datasus.gov.br/cgi/deftohtm.exe?ibge/censo/cnv/alfbr.csv",
      "data_abrev":"alfbr", 
      "data_name":"Taxa de analfabetismo ",
      "data_topic": "Educação",
      "content_to_select":[],
      "columns_to_select":[],
      "lines_to_select":[],
      "monthly_data":False,
      "dtype":DataTypes.FLOAT
   }
   MATERNAL_MORTALITY =  {
      "url":"http://tabnet.datasus.gov.br/cgi/tabcgi.exe?sim/cnv/mat10br.def",
      "data_abrev":"matbr", 
      "data_name":"obitos maternos",
      "data_topic": "saúde",
      "content_to_select":["Óbitos maternos"],
      "columns_to_select":[],
      "lines_to_select":[],
      "monthly_data":False,
      "dtype":DataTypes.INT
   }
   LIVE_BIRTHS = {
      "url": "http://tabnet.datasus.gov.br/cgi/tabcgi.exe?sinasc/cnv/nvbr.def",
      "data_abrev":"nvbr", 
      "data_name":"nascidos vivos",
      "data_topic": "saúde",
      "content_to_select":[],
      "columns_to_select":[],
      "lines_to_select":[],
      "monthly_data":False,
      "dtype":DataTypes.INT
   }
   NUMBER_OF_MEDICS = {
      "url": "http://tabnet.datasus.gov.br/cgi/deftohtm.exe?cnes/cnv/prid02br.def",
      "data_abrev":"pfbr", 
      "data_name":"Médicos disponíveis na rede pública municipal",
      "data_topic": "saúde",
      "content_to_select":[],
      "columns_to_select":["Médicos"],
      "lines_to_select":["Município"],
      "monthly_data":True,
      "dtype":DataTypes.INT
   }
   NUMBER_HOSPITAL_BEDS = {
      "url":"http://tabnet.datasus.gov.br/cgi/tabcgi.exe?cnes/cnv/leiintbr.def",
      "data_abrev":"ltbr", 
      "data_name":"Leitos hospitalares na rede pública municipal",
      "data_topic": "saúde",
      "content_to_select":["Quantidade SUS"],
      "columns_to_select":[],
      "lines_to_select":["Município"],
      "monthly_data":True,
      "dtype":DataTypes.INT
   }
   LOW_PRENATAL_BIRTHS = {
      "url": "http://tabnet.datasus.gov.br/cgi/tabcgi.exe?sinasc/cnv/nvbr.def",
      "data_abrev":"nvbr", 
      "data_name":"NVPN: Nascidos Vivos com Baixo Pré-Natal",
      "data_topic": "saúde",
      "content_to_select":[],
      "columns_to_select":["Consult pré-natal"],
      "lines_to_select":[],
      "monthly_data":False,
      "dtype":DataTypes.INT
   }
   LOW_WEIGHT_BIRTHS = {
      "url": "http://tabnet.datasus.gov.br/cgi/tabcgi.exe?sinasc/cnv/nvbr.def",
      "data_abrev":"nvbr", 
      "data_name":"NVBP: Nascidos Vivos com Baixo Peso",
      "data_topic": "saúde",
      "content_to_select":[],
      "columns_to_select":["Peso ao nascer"],
      "lines_to_select":[],
      "monthly_data":False,
      "dtype":DataTypes.INT
   }

    


class DatasusLinkScrapper(AbstractScrapper):
   """
   Classe que implementa a extração de dados do site do datasus, existem 2 casos principais do scrapping dessa base

   1) Botões referente as datas, que precisam ser todos clicados para extrair a série histórica (maioria dos dados):
   Esse é o caso base, ele é implementado usando um script de automação web do selenium para clicar nos botões e extrair cada ano de uma vez

   2) Um link apenas com um dataset de 3 anos (exceção do dado do coeficiente de gini):
   Nesse caso o webscrapping vai no link, procura no HTML o link do CSV e cria um df a partir desse CSV. O formato desse CSV é um pouco diferente
   dos DFs do caso base 1, mas a lógica de como processa ele é implementada na classe DatasusExtractor
   
   
   """
   HTML_YEAR_BUTTON_ID = "A" #id do botão de selecionar os anos na página do datasus
   HTML_CONTENT_SELECTION_DIV = "conteudo"
   HTML_COLUMNS_SELECTION_DIV = "coluna"
   HTML_LINE_SELECTION_DIV = "linha"
   SELECTED_MONTH_NAME = "dez" #caso os dados sejam mensais, escolhe o mês de dezembro
   SELECTED_MONTH_NUM = "12"
   EXTRACTED_TABLE_CITY_COL = "Município"

   website_url: str
   data_info:DatasusDataInfo

   def __init__(self,data_info:DatasusDataInfo):
      self.website_url = data_info.value["url"] #pega o url associada ao dado que vai ser extraido
      self.data_info = data_info
   
   def extract_database(self)->list[YearDataPoint]:
      """
      Função da interface das classes Scrapper, ela é um pouco fora do comum porque 
      retorna uma tupla com a lista de dataframes e a lista de anos dos dados.

      Return:
         list[YearDataPoint]: lista de objetos YearDataPoint, cada um com um df e o ano dos dados que ele se Refere
      """

      if self.data_info == DatasusDataInfo.GINI_COEF: #caso específico do dado coeficiente de gini
         driver = webdriver.Chrome()
         driver.get(self.website_url) 
         time.sleep(3) #espera a página carrega
         html:str = driver.page_source
         link:str = self.__get_link_from_html(html)
         if not link:
            return []
         list_of_years:list[int] = self.__get_years_from_html(driver)
         df =  self._dataframe_from_link(link)
         dfs_by_year = self.__separate_gini_coef_df(df)

         return YearDataPoint.from_lists(dfs_by_year,list_of_years)

      else: #caso base da extração da maioria dos dados
         csv_link_list, year_options_list = self.__selenium_page_interaction()
         list_of_dfs:list[pd.DataFrame] = []
         for link  in csv_link_list:
            list_of_dfs.append(self._dataframe_from_link(link))

         return YearDataPoint.from_lists(list_of_dfs,year_options_list) #retorna lista de objetos YearDataPoint

   def __separate_gini_coef_df(self,df:pd.DataFrame)->list[pd.DataFrame]:
      """
      Como o DF do coeficiente de gini tem 3 colunas, uma para cada ano ele precisa ser quebrado em 3 dataframes diferentes
      um para cada ano.
      """
      df_list:list[pd.DataFrame] = []
      columns:list[str] = df.columns

      for col in columns:
         if col == self.EXTRACTED_TABLE_CITY_COL:
            continue #pula coluna dos municípios
         new_df = pd.DataFrame() #cria novo df
         new_df[self.EXTRACTED_TABLE_CITY_COL] = df[self.EXTRACTED_TABLE_CITY_COL].copy() #copia coluna de municípios
         new_df[DatasusDataInfo.GINI_COEF.value["data_name"]] = df[col] #copia a coluna dos dados daquele ano pra a coluna no novo df
         #que é nomeada somente com o nome do dado

         df_list.append(new_df)

      return df_list
      
   def __agregate_cols(self,df:pd.DataFrame)->pd.DataFrame:
         """
         Agrega (soma) coluna de certos dados do datasus (nascidos vivos com baixo peso ou baixo prénatal)
         """
         df = df.copy()
         df = df.dropna()

         ceiling:int  #teto do valor
         regex_pattern:str #padrão de regex para achar osnúmeros dentro das colunas
         cols_to_sum:list[str] #lista de colunas para somar
         if self.data_info == DatasusDataInfo.LOW_WEIGHT_BIRTHS:
            ceiling = 2499
            regex_pattern = r"\d{3,4}"
            cols_to_sum = []
         elif self.data_info == DatasusDataInfo.LOW_PRENATAL_BIRTHS:
            ceiling = 6
            regex_pattern = r"\d{1}"
            cols_to_sum = ["Nenhuma"] #ja começa com essa coluna pq ela n tem número mas ainda sim vai ser somada

         for col in df.columns:
            numbers_in_col:list[str] = re.findall(regex_pattern,col) #acha todos os números dentro de cada coluna do df
            if not numbers_in_col: #não achou número na lista 
                  continue
            numbers_in_col:list[int] = list(map(int,numbers_in_col)) #lista de valores como int
            if any(map(lambda x: x > ceiling,numbers_in_col)): #algum dos valores na coluna é maior que o teto
                  break
            cols_to_sum.append(col)
         
         for col in cols_to_sum:
            df[col] = df[col].replace({"-":"0"}) #esse traça representa o zero nesses dados
            df[col] = df[col].astype("int") #transforma em inr

         df["valor"] = sum((df[col] for col in cols_to_sum)) #cria uma coluna com o valor da soma
         cols_to_drop:list[str] = [ col for col in df.columns if col not in ["valor",self.EXTRACTED_TABLE_CITY_COL] ] 
         df = df.drop(cols_to_drop,axis="columns") #dropa as outras colunas

         return df

   def __selenium_page_interaction(self)->tuple[list[str],list[int]]:
      """
      Script do selenium na página inicial do link do datasus para um dado, ele seleciona a caixa de opções dos
      anos dos dados e faz um loop clicando em todos os botões para extrair dados de todos os anos

      Return:
         tuple[list[str],list[int]]: tupla com a lista de links para os arquivos csv e a lista de anos que cada csv se refere
      """
      driver = webdriver.Chrome()
      driver.get(self.website_url) #driver do selenium vai pro site
      select_element = driver.find_element(By.ID, self.HTML_YEAR_BUTTON_ID) #acha o botão de selecionar os anos
      self.__select_data_options(driver) #caso seja necessário, seleciona as opções nas 3 tabelas do topo da página do datasus para mostrar os dados
      #certos

      csv_link_list: list[str] = []
      select_button = Select(select_element) #elemento de selecionar o botão dos dados anuais
      
      if self.data_info.value["monthly_data"]:
         year_options_list: list[str] = list(map(lambda x :x.text, select_button.options)) #pega a lista de anos 
         year_options_list: list[str] = list(filter( lambda x : self.SELECTED_MONTH_NAME in x.lower(),year_options_list)) #filtra apenas os anos com de
         year_options_list = list(map(lambda x: x[x.find("/")+1:], year_options_list)) #pega a string depois da / , que é o  ano
      
      else:
         year_options_list: list[str] = list(map(lambda x :x.text, select_button.options)) #pega a lista de anos 
      
      extracted_years:list[int] = [] #lista de anos dos dados que foram extraídos
      for year_option in year_options_list[:1]: #loop por todos os anos de dados disponíveis
         link  = self.__get_csv_link_by_year(driver, select_button,year_option)
         if not link: #não foi possívei extrair o link
            continue

         extracted_years.append(int(year_option)) #extraiu o ano com sucesso
         csv_link_list.append(link) #adiciona o link na lista de CSV
      
      driver.close() #fecha o driver do selenium

      return csv_link_list, extracted_years

   #overwride no método de achar o df pelo link, pq o csv do datasus é bem quebrado
   def _dataframe_from_link(self,file_link:str)->pd.DataFrame:
      """
      Função de extrair o df do pandas a partir do link obtido, essa função é um overwrite da função disponível
      na classe abstrata pois o separador e header inicial do CSV são diferentes nesse CSV

      """
      header_row:int
      if self.data_info == DatasusDataInfo.GINI_COEF:
         header_row = 2
      elif self.data_info == DatasusDataInfo.NUMBER_OF_MEDICS:
         header_row = 4
      else:
         header_row = 3

      if self.data_info == DatasusDataInfo.NUMBER_OF_MEDICS:
         all_medic_types_col:str = "Total" #coluna com o total de médicos por município
         df = pd.read_csv(file_link, encoding="latin-1", sep=";",header=header_row,usecols=[self.EXTRACTED_TABLE_CITY_COL,all_medic_types_col])
      
      elif self.data_info in [DatasusDataInfo.LOW_WEIGHT_BIRTHS, DatasusDataInfo.LOW_PRENATAL_BIRTHS]:
         df = pd.read_csv(file_link, encoding="latin-1", sep=";",header=header_row)
         df = self.__agregate_cols(df)
      
      else:
         df = pd.read_csv(file_link, encoding="latin-1", sep=";",header=header_row)
      
      if df is None:
         raise RuntimeError("falha em gerar o df a partir do link")
      
      

      return df

   def __select_data_options(self,driver:webdriver.Chrome)->bool:
      """
      Seleciona as opções de filtragem dos dados nas 3 tabelas (linha,coluna e conteúdo) na parte superior do site do datasus
      
      """
   
      data_options_dict = { #dict onde a key é a tag html da tabela de filtragem do datasus e o valor é o uma key para o dict do ENUM, para acessar
         self.HTML_CONTENT_SELECTION_DIV: "content_to_select", #os valores de cada tabela para extrair aquele dado
         self.HTML_COLUMNS_SELECTION_DIV: "columns_to_select",
         self.HTML_LINE_SELECTION_DIV: "lines_to_select"
      }
      for html_class,data_option_key in data_options_dict.items(): #loop pelas 3 tabelas de filtragem de conteúdo
         content_to_select:list[str] = self.data_info.value[data_option_key] #pega os conteúdos que precisam ser selecionados na página para mostrar o dado certo
         if not content_to_select: #não tem conteúdo para selecionar, continua loop
            continue

         try:
            conteudo_div = WebDriverWait(driver, 10).until(
               EC.presence_of_element_located((By.CLASS_NAME, html_class))
            )
            select_element = conteudo_div.find_element(By.TAG_NAME, 'select')
            select = Select(select_element)
            if select_element.get_attribute('multiple'):
                select.deselect_all()

            for content in content_to_select: #seleciona os conteudos para mostrar o dado certo
               select.select_by_visible_text(content)
      
         except Exception as e:
            print(f"Erro ao selecionar botão de conteúdo: {e}")
            return False
      if self.data_info == DatasusDataInfo.NUMBER_OF_MEDICS:
         self.__select_botton_page_option(driver)

      return True

   def __select_botton_page_option(self,driver:webdriver.Chrome)->bool:
      """
      Função para o caso específico de extrair a quantidade de médicos, seleciona a opção de "atende no sus" na parte de baixo da página
      """
      corposelecoes_div = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, 'corposelecoes'))
      )

      img_element = corposelecoes_div.find_element(By.ID, 'fig31')
      img_element.click()

      select_element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.NAME, 'SAtende_no_SUS'))
      )

      select = Select(select_element)
      select.deselect_all()
   
      select.select_by_value('1') #seleciona o botão para filtrar os médicos que atendem no sus
      time.sleep(1.5)

   def __get_csv_link_by_year(self, driver:webdriver.Chrome,select_elem:Select, year_str:str)->str:
      """
      Com um ano dos dados selecionado na pag web, pega o link do CSV para esse ano
      """
      
      last_two_digits:str = year_str[-2:] #ultimos dois dígitos do número em forma de str
      if self.data_info.value["monthly_data"]:
         year_button_identifier: str = self.data_info.value["data_abrev"] + last_two_digits +  self.SELECTED_MONTH_NUM + ".dbf"
      else:
         year_button_identifier: str = self.data_info.value["data_abrev"] + last_two_digits + ".dbf"
      
      select_elem.deselect_all()
      select_elem.select_by_value(year_button_identifier)

      csv_table_button = driver.find_element(By.CLASS_NAME, 'mostra')
      csv_table_button.click() 
      time.sleep(4)
      window_handles = driver.window_handles
      driver.switch_to.window(window_handles[1])

      html:str = driver.page_source
      csv_link:str = self.__get_link_from_html(html)

      driver.close()
      driver.switch_to.window(window_handles[0])
      
      return csv_link
 
   def __get_link_from_html(self,html:str)->str:
      CSV_LINK_IDENTIFIER: str = ".csv"
      HTTP_REQUEST_STR: str = "http://tabnet.datasus.gov.br"
     
      link_index:int = html.find(CSV_LINK_IDENTIFIER)
      if (link_index == -1):
         print("Não foi possível achar o link do CSV")
         return ""
         
      link_end:int = html.find('"',link_index)
      link_start:int = html.rfind('"',0,link_index)

      if self.data_info == DatasusDataInfo.GINI_COEF: #caso especial do link pra pegar o coef de gini
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
