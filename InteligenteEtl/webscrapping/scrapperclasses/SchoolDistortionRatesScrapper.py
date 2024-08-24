import os
import re
import time
import zipfile
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
from functools import reduce
from datastructures import YearDataPoint
from .AbstractScrapper import AbstractScrapper

class SchoolDistortionRatesScrapper(AbstractScrapper):

    URL = "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/indicadores-educacionais/taxas-de-distorcao-idade-serie"

    def __init__(self):
        self.files_folder_path = self._create_downloaded_files_dir()

    def __extract_links(self)->list[str]:
        driver = webdriver.Chrome()
        driver.maximize_window()
        driver.get(self.URL)

        # Esperar um pouco para garantir que a página carregue completamente
        driver.implicitly_wait(10)

        # Fechar o pop-up inicial clicando no meio da tela
        self.__close_start_popup(driver)

        # Processar os anos desejados
        anos = range(2023,2022,-1)
        self.__click_on_all_years(driver, anos)

        # Obter o conteúdo da página renderizada após clicar em todos os anos
        html_content = driver.page_source
        driver.quit()

        # Padrão regex para encontrar os links específicos
        regex_pattern = r'https://download\.inep\.gov\.br/informacoes_estatisticas/indicadores_educacionais/\d{4}/TDI_\d{4}_MUNICIPIOS.zip'

        pattern = re.compile(regex_pattern)
        links = pattern.findall(html_content)
        print(f"Links encontrados: {links}")

        return links

    def __close_start_popup(self, driver):
        try:
            driver.implicitly_wait(5)
            window_size = driver.get_window_size()
            width = window_size['width']
            height = window_size['height']
            center_x = width / 2
            center_y = height / 2
            actions = ActionChains(driver)
            actions.move_by_offset(center_x, center_y).click().perform()
            print("Fechou o pop-up inicial clicando no meio da tela.")
        except Exception as e:
            print(f"Não foi possível fechar o pop-up inicial clicando no meio da tela: {e}")

    def __click_side_arrows(self, driver, direcao):
        if direcao == "esquerda":
            selector = "#content-core > div.govbr-tabs.swiper-container-initialized.swiper-container-horizontal.swiper-container-free-mode > div.button-prev > span"
        elif direcao == "direita":
            selector = "#content-core > div.govbr-tabs.swiper-container-initialized.swiper-container-horizontal.swiper-container-free-mode > div.button-next > span"

        try:
            seta_element = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
            )
            seta_element.click()
            print(f"Clicou na seta {direcao} para mostrar anos anteriores.")
        except Exception as e:
            print(f"Não foi possível clicar na seta {direcao}: {e}")

    def __click_on_all_years(self, driver, anos):
        max_tries:int = 3
        for i, ano in enumerate(anos):
            cur_try:int = 0
            while cur_try < max_tries:
                try:
                    print(f"Processando o ano {ano}...")
                    ano_element = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.LINK_TEXT, str(ano)))
                    )
                    driver.execute_script("arguments[0].scrollIntoView();", ano_element)
                    driver.execute_script("arguments[0].click();", ano_element)
                    time.sleep(2)
                    break
                except Exception as e:
                    cur_try += 1
                    print(f"Erro ao processar o ano {ano}: {e}")
                    if i > 0 and ano > anos[i - 1]:
                        self.__click_side_arrows(driver, "esquerda")
                    else:
                        print("direita")
                        self.__click_side_arrows(driver, "direita")

    def extract_database(self)->list[YearDataPoint]:
        year_data_points = []

        # Extração dos links das páginas
        links = self.__extract_links()[:1]
        self.__download_and_extract_zipfiles(links)
        print("extraiu todos os zipfiles")
     
        inner_folder = os.listdir(self.DOWNLOADED_FILES_PATH)
        for folder in inner_folder:
            folder_correct_path = os.path.join(self.DOWNLOADED_FILES_PATH, folder)
            if not os.path.isdir(folder_correct_path):
                print(f"Não é um diretório: {folder_correct_path}")
                continue

            year_data_point = self.__data_dir_process(folder_correct_path)
            if year_data_point:
                year_data_points.append(year_data_point)
                print(f"Processamento concluído na pasta {folder_correct_path} com sucesso.")
            else:
                print(f"Processamento falhou na pasta {folder_correct_path}")

        print(f"Total de YearDataPoints processados: {len(year_data_points)}")

        self._delete_download_files_dir()
        return year_data_points

    def __download_and_extract_zipfiles(self,urls:list[str])->None:
        #baixar arquivos zips
        downloaded_files_dir: str = self.DOWNLOADED_FILES_PATH 
        chrome_options = Options()
        chrome_options.add_experimental_option("prefs", {
            "download.default_directory": downloaded_files_dir,  # Set the download directory
            "download.prompt_for_download": False,  # Disable the prompt for download
            "download.directory_upgrade": True,  # Ensure directory upgrade
            "safebrowsing.enabled": True  # Enable safe browsing
        })

        # Set up the Chrome driver
        driver = webdriver.Chrome(options=chrome_options)
        if not os.path.isdir(downloaded_files_dir):
            os.mkdir(downloaded_files_dir)

        extracted_zipfile_count:int = 0 #variavel para contar quantos zipfiles existes
        for url in urls:
            # Open the browser and navigate to the URL
            driver.get(url)
            time.sleep(5)

            files_in_dir:list[str] =  os.listdir(downloaded_files_dir)
            new_zipfiles_count:int = reduce(lambda count,filename: count+1 if ".zip" in filename else count,files_in_dir,0) #conta numero de zipfiles no folder

            while new_zipfiles_count == extracted_zipfile_count: #enquanto nenhum arquivo no dir for um .zip
                time.sleep(5)
                files_in_dir = os.listdir(downloaded_files_dir)
                new_zipfiles_count:int = reduce(lambda count,filename: count+1 if ".zip" in filename else count,files_in_dir,0) #conta numero de zipfiles no folder
            
            extracted_zipfile_count = new_zipfiles_count #atualiza variável de arquivos zip extraidos
            print(extracted_zipfile_count)
            time.sleep(5)  
        driver.quit()
    
        #extrair arquivos zips
        files = os.listdir(self.DOWNLOADED_FILES_PATH)

        for file in files: #loop pelos arquivos do diretório
            if not ".zip" in file: 
                continue
            with zipfile.ZipFile(os.path.join(self.DOWNLOADED_FILES_PATH, file), "r") as zip_ref:
                zip_ref.extractall(self.DOWNLOADED_FILES_PATH) #extrai arquivo zip
        
        new_files = os.listdir(self.DOWNLOADED_FILES_PATH)
        for file in new_files:
            if ".zip" in file or ".crdownload" in file:
                os.remove(os.path.join(self.DOWNLOADED_FILES_PATH,file)) #remove os arquivos zips ou arquivos temporários do chrome

    def __data_dir_process(self, folder_path: str)-> YearDataPoint|None:
        print(f"Entrando na pasta: {folder_path}")
        files_list:list[str] = os.listdir(folder_path)

        for file in files_list:
            if file.endswith(".xlsx") and "TDI_MUNICIPIOS" in file:
                    file_correct_path = os.path.join(folder_path, file)
                    cols = ['Unnamed: 3','Unnamed: 5','Unnamed: 6','Total']
                    df = pd.read_excel(file_correct_path, header=6, skiprows=[7, 8],usecols=cols)

                    if df is not None:
                        year = self.__extract_year_from_path(folder_path)
                        print(f"path: {file_correct_path}, ano: {year}")
                        if year:
                            print(f"Criando YearDataPoint para o ano {year}")
                            return YearDataPoint(df=df, data_year=year)
                        else:
                            print(f"Falha ao extrair o ano do caminho: {folder_path}")
                    else:
                        print(f"Processamento falhou para o arquivo: {file_correct_path}")

        print(f"Não foram encontrados arquivos .xlsx relevantes em {folder_path}")
        return None

    def __process_df(self, xlsx_file_path: str) -> pd.DataFrame:
        try:
            cols = ['Unnamed: 3','Unnamed: 5','Unnamed: 6','Total']
            df = pd.read_excel(xlsx_file_path, header=6, skiprows=[7, 8],usecols=cols)
            print(f"Colunas disponíveis no arquivo {xlsx_file_path}: {df.columns.tolist()}")

            municipality_col = 'Unnamed: 3'
            total_col = 'Total'
            location_col = 'Unnamed: 5'
            admin_dependency_col = 'Unnamed: 6'

            if municipality_col not in df.columns or total_col not in df.columns or location_col not in df.columns or admin_dependency_col not in df.columns:
                raise ValueError(f"Colunas necessárias ('{municipality_col}', '{total_col}', '{location_col}', '{admin_dependency_col}') não encontradas no arquivo {xlsx_file_path}")

            df_filtered = df[(df[location_col] == 'Total') & (df[admin_dependency_col] == 'Total')]
            filtered_df = df_filtered[[municipality_col, total_col]]
            print(f"Dados filtrados:\n{filtered_df.head()}")  # Exibe uma amostra dos dados filtrados
            filtered_df = filtered_df.rename({
                "Unnamed: 3": "codigo_municipio",
            },axis="columns")

            filtered_df["codigo_municipio"] = filtered_df["codigo_municipio"].astype("int")

            return filtered_df
        except Exception as e:
            print(f"Erro ao processar o DataFrame: {e}")
            return None

    def __extract_year_from_path(self, path: str) -> int:
        print(f"Extraindo ano do caminho: {path}")
        ano_match = re.search(r'\d{4}', path)
        if ano_match:
            year = int(ano_match.group(0))
            print(f"Ano extraído: {year}")
            return year
        else:
            print("Falha ao extrair o ano.")
            return None


if __name__ == "__main__":
    scrapper = SchoolDistortionRatesScrapper()
    year_data_points:list[YearDataPoint] = scrapper.extract_database()

    for data_point in year_data_points:
        data_point.df.to_csv(f"{data_point.data_year}.csv")
        print(f"Ano: {data_point.data_year}, DataFrame Shape: {data_point.df.info()}")
