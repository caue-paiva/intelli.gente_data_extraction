from selenium import webdriver
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time , os
import pandas as pd
#from .AbstractScrapper import AbstractScrapper, BaseFileType
from .AbstractScrapper import AbstractScrapper ,BaseFileType
from  DataClasses import YearDataPoint

class FormalJobsScrapper(AbstractScrapper):
    
    DOWNLOADED_FILES_DIR = os.path.join(os.getcwd(),"formal_jobs_file_dir")

    CHROME_DRIVER_PREFEREN =  {
        "download.default_directory": DOWNLOADED_FILES_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    BASE_URL: str = "https://www.ibge.gov.br/apps/snig/v1/?loc=0&cat=-1,-2,-3,128&ind=4732"
    FILE_TYPE: BaseFileType = BaseFileType.CSV
    TIME_SERIES_YEARS:list[str] = ["2000","2010"] #anos da série histórica,hard-coded por enquanto
    EXTRACTED_TABLE_CITY_COL = "Cod. Loc." #nome da coluna dos códigos do municípios na tabela extraida
    EXTRACTED_DATA_NAME = "População ocupada com vínculo formal"



    def __click_return_button(self,driver:Chrome):
        
        try:
            voltar_icon = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div#lista-base-busca .voltar.icon-left-big'))
            )
            voltar_icon.click()
        except Exception as e:
            print(f"Erro ao clicar botão de voltar: {e}")

    def __create_downloaded_files_dir(self)->str:
        dir_path: str = os.path.join(os.getcwd(),self.DOWNLOADED_FILES_DIR)
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)
        
        return dir_path
    
    def __delete_download_files_dir(self)->bool:
        files:list[str] = os.listdir(self.DOWNLOADED_FILES_DIR)

        for file in files:
            try:
                os.remove(os.path.join(self.DOWNLOADED_FILES_DIR,file))
            except Exception as e:
                print(f"falha ao deletar arquivo CSV extraído. Erro: {e}")
        
        try:
            os.rmdir(self.DOWNLOADED_FILES_DIR)
        except Exception as e:
                print(f"falha ao deletar diretório do arquivo extraído. Erro: {e}")

    def __click_on_all_states(self, driver:Chrome)->bool:
        index_of_the_state: int = 0
        while True:
            # Wait for the UL with class 'list body-list' to appear
            
            
            state_selection_box = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'ul.list.body-list'))
            )

            #extrai a lista de estados da tabela de input no canto inferior esquerdo da tela
            list_of_states = state_selection_box.find_elements(By.CSS_SELECTOR, 'li.option.with-child')
            state_to_be_clicked = list_of_states[index_of_the_state] #estado atual a ser selecionado
            try:

                    #clica na div com o nome do estado, va aparecer todos os municípios de lá
                    local_tag = state_to_be_clicked.find_element(By.CLASS_NAME, 'local')
                    local_tag.click()
                    time.sleep(0.3)

                    #acha denovo o elemento ul, dessa vez pra  lista de opções de municipios dentro de um estado
                    city_selection_box = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'ul.list.body-list'))
                    )

                    #acha e depois clica no botão de selecionar todas as cidades
                    select_all_cities_button = city_selection_box.find_element(By.CSS_SELECTOR, 'li.option')
                    input_tag = select_all_cities_button.find_element(By.CSS_SELECTOR, 'div.input > input')
                    input_tag.click()
                    time.sleep(0.3)

                    #clica o botão de voltar para voltar pro menu com todos os estados e clicar num novo
                    self.__click_return_button(driver)
                    
                    index_of_the_state+=1 #pega prox elemento da lista de estados
                    if index_of_the_state >= len(list_of_states): #chegou no ultimo estado, da break no loop
                         break
            except Exception as e:
                    print(f"Erro ao clicar no botão de selecionar o estado específico: {e}")

    def __separate_df_by_year(self,df:pd.DataFrame)->list[YearDataPoint]:
        """
        Separa o dataframe em 2 dfs cada um com 2 colunas, um para cada ano do dado:
        
        Args:
            df (pd.Dataframe): df original extraido

        Return:
            list[YearDataPoint]: lista de objetos desse tipo, cada um com o df e o ano dos dados dele
        """

        city_code_col:pd.Series = df[self.EXTRACTED_TABLE_CITY_COL]
        data_list:list[YearDataPoint] = []
        for col in df.columns:
            if col in self.TIME_SERIES_YEARS:  
                new_df = pd.DataFrame() 
                new_df[self.EXTRACTED_TABLE_CITY_COL] = city_code_col.copy() #copia coluna de municípios
                new_df[self.EXTRACTED_DATA_NAME] = df[col].copy() #copia coluna dos dados daquele ano
                data_list.append(
                    YearDataPoint(new_df,col) #novo objeto com o dataframe do ano e o ano desse dado
                )           
        return data_list 
            
    def download_database_locally(self, url:str="")->str:
        """
        Método que realiza webscrapping, baixa os dados localmente e retorna o caminho para esse arquivo baixado localmente
        """
        if not url:
            url = self.BASE_URL

        self.__create_downloaded_files_dir() #cria diretório para os dados extraidos caso ele não exista

        options = webdriver.ChromeOptions()
        options.add_experimental_option("prefs", self.CHROME_DRIVER_PREFEREN)
        driver = webdriver.Chrome(options=options)

        try:
            # abre a página web maximizada
            driver.maximize_window()
            driver.get(self.BASE_URL)
            time.sleep(7)

            #div para as filtragens por atributos da tabela
            dimensoes_div = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, 'dimensoesDiv'))
            )

            # acha a div que tem os períodos dos dados
            periodo_div = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, './/div[@id="dimensoesDiv"]//div[contains(text(), "Período")]'))
            )

            #div com o ano 2000
            label_2000 = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, './/div[@id="dimensoesDiv"]//label[input[@value="129"] and contains(text(), "2000")]'))
            )

            #clica no botão para incluir o ano 2000
            input_2000 = label_2000.find_element(By.XPATH, './input')
            input_2000.click()

            #espera até aparecer o elemento de selecionar o local
            local_selector_div = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, 'local-selector'))
            )
            time.sleep(3)
            #clica no botão de selecionar o local
            step_div = local_selector_div.find_element(By.CLASS_NAME, 'step')
            step_div.click()

            #vai pro meio da tela
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
            ul_list = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '#local-selector .body-list'))
            )# espera aparecer a lista de opções de separações geográficas (regiões,estados...)

            if not ul_list:
                raise Exception("lista de opções de localidade dos dados não foi encontrada")

            #acha a opção de municípios 
            municipio_li = None
            li_elements = ul_list.find_elements(By.TAG_NAME, 'li')
            for li in li_elements:
                if "Municípios" in li.text:
                    municipio_li = li
                    break
            
            if not municipio_li:
                raise Exception("opção de municípios não  foi encontrada")            

            
            span_inside_li = municipio_li.find_element(By.TAG_NAME, 'span')
            span_inside_li.click()

            self.__click_on_all_states(driver) #clica na opção "todos os municípios" de cada estado na página

            time.sleep(2)
            download_div = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'downloadDiv'))
            ) #espera achar o botão de baixar os arquivos

            #acha a tag "a" dentro da div do botão de download
            download_link = download_div.find_element(By.TAG_NAME, 'a')
            download_link.click() #clica pra baixa o arquivo
            time.sleep(10)  #espera o download_acabar
            driver.quit() #fecha o webdriver

            downloaded_files = os.listdir(self.DOWNLOADED_FILES_DIR)            
           
            return os.path.join(self.DOWNLOADED_FILES_DIR,downloaded_files[0])

        except Exception as e:
            raise Exception(e)

    def extract_database(self, website_url: str = "", delete_extracted_files:bool = True)->list[YearDataPoint]:
        path_to_csv:str = self.download_database_locally(website_url)
        df: pd.DataFrame = pd.read_csv(path_to_csv,sep=";")

        if delete_extracted_files:
            self.__delete_download_files_dir()
        return self.__separate_df_by_year(df)
    
if __name__ == "__main__":
    obj = FormalJobsScrapper()
    df = obj.extract_database()
    print(df.head())