from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time , requests , os
import pandas as pd
#from .AbstractScrapper import AbstractScrapper, BaseFileType

# Set up the WebDriver

options = webdriver.ChromeOptions()
download_dir = os.path.join(os.getcwd(),"teste_dir")  # Change this to your download directory

prefs = {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
}
options.add_experimental_option("prefs", prefs)
driver = webdriver.Chrome(options=options)

def click_voltar_icon(driver):
    # Function to click the 'voltar icon' button
    try:
        voltar_icon = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div#lista-base-busca .voltar.icon-left-big'))
        )
        voltar_icon.click()
    except Exception as e:
        print(f"Error clicking on voltar icon: {e}")

try:
    # Open the webpage
    driver.maximize_window()
    driver.get("https://www.ibge.gov.br/apps/snig/v1/?loc=0&cat=-1,-2,-3,128&ind=4732")
    time.sleep(10)

    
    local_selector_div = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, 'local-selector'))
    )
    time.sleep(3)
    step_div = local_selector_div.find_element(By.CLASS_NAME, 'step')
    step_div.click()

    driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
    ul_list = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, '#local-selector .body-list'))
    )

    # Debugging: Print if ul_list is found
    if ul_list:
        print("UL list found.")

    municipio_li = None
    li_elements = ul_list.find_elements(By.TAG_NAME, 'li')
    for li in li_elements:
        if "Municípios" in li.text:
            municipio_li = li
            break
    print(municipio_li.text)
    
    if municipio_li:
         # Click on the label inside the list item
         span_inside_li = municipio_li.find_element(By.TAG_NAME, 'span')
         span_inside_li.click()

         index_li: int = 0
         while True:
            # Wait for the UL with class 'list body-list' to appear
            
            next_ul = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'ul.list.body-list'))
            )

            list_of_states = next_ul.find_elements(By.CSS_SELECTOR, 'li.option.with-child')
            state_to_be_clicked = list_of_states[index_li]
            print(state_to_be_clicked.text)
            try:
                    # Click on the 'local' tag inside each 'li'
                    local_tag = state_to_be_clicked.find_element(By.CLASS_NAME, 'local')
                    local_tag.click()
                    time.sleep(0.3)

                    # Re-find the 'ul' element to refresh the context
                    next_ul = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'ul.list.body-list'))
                    )

                    first_li = next_ul.find_element(By.CSS_SELECTOR, 'li.option')
                    input_tag = first_li.find_element(By.CSS_SELECTOR, 'div.input > input')
                    input_tag.click()
                    time.sleep(0.3)

                    # Click the 'voltar icon' button to go back to the previous state
                    click_voltar_icon(driver)
                    
                    index_li+=1 #pega prox elemento da lista de estados
                    if index_li >= len(list_of_states): #chegou no ultimo estado, da break no loop
                         break
                      
            except Exception as e:
                    print(f"Error clicking on local tag: {e}")
    
         time.sleep(2)
         download_div = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'downloadDiv'))
         )

         # Find the 'a' tag inside the 'downloadDiv' div
         download_link = download_div.find_element(By.TAG_NAME, 'a')

         download_link.click()

        # Wait for the download to complete (this may vary depending on the file size and internet speed)
         time.sleep(10)  # Adjust the sleep time if necessary

        # Verify the download
         downloaded_files = os.listdir(download_dir)
         print("Downloaded files:", downloaded_files)
    else:
        print("Element with text 'Municípios' not found.")

finally:
    # Close the browser
    driver.quit()


class FormalJobsScrapper():
    
    DOWNLOADED_FILES_DIR = os.path.join(os.getcwd(),"formal_jobs_file_dir")

    CHROME_DRIVER_PREFEREN =  {
        "download.default_directory": DOWNLOADED_FILES_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }

    BASE_URL: str = "https://www.ibge.gov.br/apps/snig/v1/?loc=0&cat=-1,-2,-3,128&ind=4732"

    def __click_return_button(driver):
        
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
    
    def __click_on_all_states(self, driver)->bool:
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

    def __download_files_page(self,url="")->str:
        """
        Método que realiza webscrapping, baixa os dados localmente e retorna o caminho para esse arquivo baixado localmente
        """
        if not url:
            url = self.BASE_URL

        self.__create_downloaded_files_dir() #cria diretório para os dados extraidos caso ele não exista

        options = webdriver.ChromeOptions()
        options.add_experimental_option("prefs", prefs)
        driver = webdriver.Chrome(options=options)

        try:
            # abre a página web maximizada
            driver.maximize_window()
            driver.get(self.BASE_URL)
            time.sleep(7)

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
            if len(downloaded_files) > 1:
                raise Exception("Mais de um arquivo no diretório temporário criado para extração dos dados")
            
            return os.path.join(self.DOWNLOADED_FILES_DIR,downloaded_files[0])

        except Exception as e:
            print(f"falha ao extrai página web, erro {e}")
            return ""
