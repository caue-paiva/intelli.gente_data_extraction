from datastructures import YearDataPoint
from .AbstractScrapper import AbstractScrapper
import pandas as pd

import os
import re
import time
import zipfile
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options

# Configuração de logging

class TechEquipamentScrapper(AbstractScrapper):
    
    URL = "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-escolar"
    REGEX_PATTERN = r'https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_\d{4}\.zip'
    SCHOOL_TO_CITY_TABLE_FILE = "suplemento_cursos_tecnicos"

    __school_to_city_df:pd.DataFrame|None

    def __init__(self):
        super().__init__()
        self.files_folder_path = self._create_downloaded_files_dir()
        self.__school_to_city_df = None

    def __get_school_to_city_code_table(self,path:str)->pd.DataFrame:
        COLS_TO_READ = [
            "CO_MUNICIPIO",
            "NO_ENTIDADE"
        ]
        return pd.read_csv(
            path,
            sep=";",
            encoding="latin-1",
            usecols=COLS_TO_READ
        )

    def __extract_links(self) -> list[str]:
        """
        extrai os links e retorna uma lista deles
        """
        driver = webdriver.Chrome()
        driver.maximize_window()
        driver.get(self.URL)
        time.sleep(5)

        try:
            actions = ActionChains(driver)
            actions.move_by_offset(driver.execute_script("return window.innerWidth / 2;"),
                                   driver.execute_script("return window.innerHeight / 2;")).click().perform()
            time.sleep(2)
        except Exception as e:
            print(f"Erro ao clicar no centro da tela: {e}")

        html_content = driver.page_source
        driver.quit()

        links = re.findall(self.REGEX_PATTERN, html_content)

        return links

    def __download_zipfiles(self, urls: list[str]) -> None:
        """
        baixa os zipfiles localmente usando o selenium
        """
        chrome_options = Options()
        chrome_options.add_experimental_option("prefs", {
            "download.default_directory": os.path.abspath(self.files_folder_path),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        })

        driver = webdriver.Chrome(options=chrome_options)

        for url in urls:
            driver.get(url)
            time.sleep(10)  # Tempo para garantir que o download comece

            max_wait_time = 300  # 5 minutos
            wait_time = 0
            poll_interval = 5

            while wait_time < max_wait_time:
                files = os.listdir(self.files_folder_path)
                downloading = any(file.endswith(".crdownload") for file in files)
                if not downloading:
                    break

                time.sleep(poll_interval)
                wait_time += poll_interval
        
        driver.quit()

    def __extract_zipfiles(self) -> None:
        """
        Extrai os zipfiles localmente
        """
        for file in os.listdir(self.files_folder_path):
            if file.endswith(".zip"):
                file_path = os.path.join(self.files_folder_path, file)
                with zipfile.ZipFile(file_path, "r") as zip_ref:
                    zip_ref.extractall(self.files_folder_path)
                os.remove(file_path)

    def __process_all_files_in_directory(self, folder_path: str) -> list[YearDataPoint]:
        year_data_points = []

        for root, dirs, files in os.walk(folder_path):
            #caminho pelos subdiretórios até achar no dir que tem os arquivos CSV
            for file in files:
                print(file)
                if file.endswith(".csv") and not file.lower().startswith("~$"):
                    file_correct_path = os.path.join(root, file)
                    if   self.SCHOOL_TO_CITY_TABLE_FILE.lower() in file.lower(): #arquivo é o mapping de nome de escola para o código do município
                        print("achou a ref")
                        self.__school_to_city_df = self.__get_school_to_city_code_table(file_correct_path)
                        continue
                    
                    year_data_point = self.__data_file_process(file_correct_path)
                    if year_data_point:
                        year_data_points.append(year_data_point)
                        print(f"Processamento bem-sucedido para o arquivo: {file_correct_path}")
                    else:
                        print(f"Processamento falhou em um dos arquivos: {file_correct_path}")

        return year_data_points

    def __data_file_process(self, file_path: str) -> YearDataPoint:
        """
        Dado um file path de um csv, extrai o df e o ano dele,retornando um objeto YearDataPoint
        """
        
        df = self.__process_df(file_path)
        year = self.__extract_year_from_path(file_path)
        return YearDataPoint(df=df, data_year=year)
        
    def __process_df(self, csv_file_path: str) -> pd.DataFrame:
        """
        Dado um path para um arquivo CSV, tenta ler ele num DF
        """
        RELEVANT_COLS: list[str] = [
            "IN_LABORATORIO_INFORMATICA", "IN_EQUIP_LOUSA_DIGITAL", "IN_EQUIP_MULTIMIDIA",
            "IN_DESKTOP_ALUNO", "IN_COMP_PORTATIL_ALUNO", "IN_TABLET_ALUNO", 
            "IN_INTERNET_APRENDIZAGEM", "NO_ENTIDADE"
        ]
        print(f"path para extrair DF: {csv_file_path}")

        # Leitura do arquivo CSV, utilizando as colunas relevantes
        df = pd.read_csv(csv_file_path, sep=";", encoding="latin-1", usecols=RELEVANT_COLS)

        # Exibindo informações básicas para verificar se a leitura foi correta

        # Chamando a função __filter_df
        self.__filter_df(df)
        
        return df

    def __filter_df(self, df: pd.DataFrame) -> pd.DataFrame:
        # Retornando o DataFrame sem filtros
        return df

    def __add_city_codes(self, df: pd.DataFrame) -> pd.DataFrame:
        merged = df.merge( #join nos dataframes pela coluna de nome da
            right=self.__school_to_city_df,
            on="NO_ENTIDADE",
            how='inner',
        )

        merged = merged.drop(["NO_ENTIDADE"],axis="columns")
        return merged

    def __extract_year_from_path(self, path: str) -> int:
        ano_match = re.search(r'\d{4}', path)
        if ano_match:
            return int(ano_match.group(0))
        else:
            return None

    def extract_database(self) -> list[YearDataPoint]:
      links = self.__extract_links()[:2]
      self.__download_zipfiles(links)
      self.__extract_zipfiles()
      data_points:list[YearDataPoint] = self.__process_all_files_in_directory(self.files_folder_path)

      if self.__school_to_city_df is None:
            raise RuntimeError("Não foi possível achar arquivo que mapea o nome das escolas para um município")

      self._delete_download_files_dir()

      return [ 
            YearDataPoint(self.__add_city_codes(data.df),data.data_year) for data in data_points    
      ]


        