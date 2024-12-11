import zipfile,time,os,re
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
from datastructures import YearDataPoint
from .AbstractScrapper import AbstractScrapper
from etl_config import get_config


class IdebFinalYearsScrapper(AbstractScrapper):
    
    URL = "https://www.gov.br/inep/pt-br/areas-de-atuacao/pesquisas-estatisticas-e-indicadores/ideb/resultados"
    YEAR_REGEX_PATTERN = r"\d{4}"
    FINAL_DATA_VAL_COL = get_config("DATA_VALUE_COL")
    EXTRACTED_CITY_COL = "Código do Município"

    def __init__(self):
        self.files_folder_path = self._create_downloaded_files_dir()

    def __extract_links(self) -> list[str]:
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")  # modo para não gerar um janela
        options.add_argument("--disable-gpu")  # compatibilidade
        options.add_argument("--no-sandbox") 
        options.add_argument("--disable-dev-shm-usage")  # prevenir problemas de memória
        driver = webdriver.Chrome(options=options)
        driver.maximize_window()
        driver.get(self.URL)
        time.sleep(5)

        try:
            actions = ActionChains(driver)
            actions.move_by_offset(driver.execute_script("return window.innerWidth / 2;"),
                                   driver.execute_script("return window.innerHeight / 2;")).click().perform()
            time.sleep(2)
        except Exception as e:
           pass

        try:
            botao_2021 = driver.find_element(By.LINK_TEXT, '2021')
            botao_2021.click()
            time.sleep(2)
        except Exception as e:
            pass
        
        try:
            botao_anos_anteriores = driver.find_element(By.LINK_TEXT, 'Anos anteriores')
            botao_anos_anteriores.click()
            time.sleep(2)
        except Exception as e:
            pass

        html_content = driver.page_source
        driver.quit()

        regex_pattern = r'https://download.inep.gov.br/ideb/resultados/divulgacao_anos_finais_municipios_\d{4}\.zip'
        links = re.findall(regex_pattern, html_content)

        return links

    def __download_and_extract_zipfiles(self, urls: list[str]) -> None:
        # Caminho atual do diretório onde o script está sendo executado
        current_directory = os.getcwd()

    
        # Juntando o caminho atual com o subdiretório
        download_directory = os.path.join(current_directory, self.DOWNLOADED_FILES_PATH)

        # Criando o diretório se ele não existir
        if not os.path.exists(download_directory):
            os.makedirs(download_directory)

        chrome_options = Options()
        chrome_options.add_argument("--headless")  # modo para não gerar um janela
        chrome_options.add_argument("--disable-gpu")  # compatibilidade
        chrome_options.add_argument("--no-sandbox") 
        chrome_options.add_argument("--disable-dev-shm-usage")  # prevenir problemas de memória
        chrome_options.add_experimental_option("prefs", {
            "download.default_directory": download_directory,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        })

        driver = webdriver.Chrome(options=chrome_options)
        for url in urls:
            driver.get(url)
            time.sleep(15)  # Tempo para garantir que o download comece

            # Verifique o diretório até encontrar o arquivo ZIP
            downloaded_file = None
            max_wait_time = 300  # Máximo de 5 minutos para aguardar o download
            wait_time = 0

            while wait_time < max_wait_time:
                files = os.listdir(self.DOWNLOADED_FILES_PATH)
                for file in files:
                    if file.endswith(".zip"):
                        downloaded_file = file
                        break
                if downloaded_file:
                    break

                time.sleep(10)
                wait_time += 10

            if not downloaded_file:
                print(f"Tempo de espera excedido para o download de: {url}")
            else:
                # Extrair e remover o arquivo ZIP
                file_path = os.path.join(self.DOWNLOADED_FILES_PATH, downloaded_file)
                with zipfile.ZipFile(file_path, "r") as zip_ref:
                    zip_ref.extractall(self.DOWNLOADED_FILES_PATH)
                os.remove(file_path)

        driver.quit()

    def __process_all_files_in_directory(self, folder_path: str) -> YearDataPoint:
      data_point:YearDataPoint|None = None

      # Percorrer todas as pastas extraídas no diretório
      for root, dirs, files in os.walk(folder_path):
            for file in files:
                if file.endswith(".xlsx"):
                    file_correct_path = os.path.join(root, file)
                    year_data_point = self.__data_file_process(file_correct_path)
                    if year_data_point:
                        data_point = year_data_point
                    else:
                        print(f"Processamento falhou em um dos arquivos: {file_correct_path}")

      if data_point is None:
         raise RuntimeError("falha ao processar os arquivos em um yearDataPoint")
      return data_point

    def __data_file_process(self, file_path: str) -> YearDataPoint:
        df = self.__process_df(file_path)
        year = self.__extract_year_from_path(file_path)
        return YearDataPoint(df=df, data_year=year)

    def __process_df(self, xlsx_file_path: str) -> pd.DataFrame:
        # Ler o arquivo XLSX a partir da linha 6 como cabeçalho e a partir da linha 9 para os dados
        df = pd.read_excel(xlsx_file_path, header=6, skiprows=[7, 8])

        # Verifica a presença da coluna "Sigla da UF" e aplica os filtros adequados
        if 'Sigla da UF' in df.columns:
            # Filtra os dados do DF para "Pública"
            df_df = df[(df['Sigla da UF'] == 'DF') & (df['Rede'] == 'Pública')]
            # Filtra os dados dos outros estados para "Municipal"
            df_others = df[(df['Sigla da UF'] != 'DF') & (df['Rede'] == 'Municipal')]
            # Combina os dois dataframes
            df = pd.concat([df_df, df_others])
        else:
            # Se "Sigla da UF" não estiver presente, filtra por "Municipal"
            df = df[df['Rede'] == 'Municipal']

        # Filtra as colunas que contêm a palavra 'IDEB'
        relevant_cols = [col for col in df.columns if 'IDEB' in str(col)]
        
        # Usar a coluna 'Nome do Município' para os nomes dos municípios
        municipality_col = 'Código do Município'
        if municipality_col not in df.columns:
            raise ValueError(f"Coluna do município '{municipality_col}' não encontrada no arquivo {xlsx_file_path}")

        relevant_cols.append(municipality_col)  # Inclui a coluna de município
        filtered_df = df[relevant_cols]
        # Itera sobre as linhas e imprime os municípios e seus valores de IDEB
        for index, row in filtered_df.iterrows():
            municipio = row[municipality_col]
            ideb_values = row[relevant_cols[:-1]]  # Exclui a coluna de município dos valores de IDEB

        cols:list[str]  = filtered_df.columns
        cols = list(map(self.__parse_col_name, cols))
        filtered_df.columns = cols
        return filtered_df

    def __parse_col_name(self,input_str:str)->str:
      """
      pega a string que representa o ano dos dados do nome da coluna 
      """
      match = re.findall(self.YEAR_REGEX_PATTERN,input_str)
      if match is None or len(match) == 0:
          return input_str
      else:
          return match[0]

    def __extract_year_from_path(self, path: str) -> int:
        ano_match = re.search(r'\d{4}', path)
        if ano_match:
            return int(ano_match.group(0))
        else:
            return None

    def __separate_data_by_year(self,data_point:YearDataPoint)->list[YearDataPoint]:
         data_points:list[YearDataPoint] = []
         
         df:pd.DataFrame = data_point.df
         year_cols:list[str] = [ x for x in df.columns if len(re.findall(self.YEAR_REGEX_PATTERN,x)) != 0 ] #add todas as colunas do df que tem um ano

         for year in year_cols:
            new_df = pd.DataFrame()
            new_df[self.EXTRACTED_CITY_COL] = df[self.EXTRACTED_CITY_COL] #copia coluna de nome dos municípios
            new_df[self.FINAL_DATA_VAL_COL] = df[year] #coluna do valor dos dados é a coluna com o ano 
            data_points.append(
                YearDataPoint(df=new_df,data_year=year)
            )
         
         return data_points

    def extract_database(self) -> list[YearDataPoint]:
        links = self.__extract_links()
        self.__download_and_extract_zipfiles(links)
        data_point:YearDataPoint = self.__process_all_files_in_directory(self.DOWNLOADED_FILES_PATH)
        
        self._delete_download_files_dir()
        return self.__separate_data_by_year(data_point)
