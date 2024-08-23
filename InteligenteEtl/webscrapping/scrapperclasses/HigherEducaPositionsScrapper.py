import os, time, re ,requests , zipfile
import pandas as pd
from datastructures import YearDataPoint
from .AbstractScrapper import AbstractScrapper
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from functools import reduce

class HigherEducaPositionsScrapper(AbstractScrapper):
    
    URL =  "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-da-educacao-superior"

    def __init__(self):
        self._create_downloaded_files_dir()

    def extract_database(self)->list[YearDataPoint]:
        links:list[str] = self.__get_file_links()[:1]
        print(links)
        self.__download_zipfiles(links)
        print("baixou todos zipfiles")
        time.sleep(5)
        self.__extract_zipfiles()
        
        year_data_points = []

        # Modificação para acessar pastas já extraídas localmente
        inner_folder = os.listdir(self.DOWNLOADED_FILES_PATH)

        for folder in inner_folder:
            folder_correct_path = os.path.join(self.DOWNLOADED_FILES_PATH, folder)
            if not os.path.isdir(folder_correct_path):
                print(f"Não é um diretório: {folder_correct_path}")
                continue

            year_data_point = self.__data_dir_process(folder_correct_path)
            if year_data_point:
                year_data_points.append(year_data_point)
            else:
                print(f"Processamento falhou na pasta {folder}")
        
        self._delete_download_files_dir()
        return year_data_points

    def __get_file_links(self)->list[str]:
        # FUNÇÃO NOVA E BEM MAIS SIMPLES QUE EXTRAI DIRETO DO HTML DA PÁGINA 
        regex_pattern =  r'https://download\.inep\.gov\.br/microdados/microdados_censo_da_educacao_superior_\d{4}\.zip'
        # Faz a requisição HTTP para obter o conteúdo da página
        response = requests.get(self.URL) 
        #response.raise_for_status()  # Garante que a requisição foi bem-sucedida
        
        # Obtém o conteúdo da página como texto
        html = response.text
        
        # Define a expressão regular para encontrar os links desejados
        regex = re.compile(regex_pattern)
        
        # Encontra todos os links que correspondem à expressão regular
        links = regex.findall(html)
        
        return links

    def __download_zipfiles(self,urls:list[str])->None:  
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

    def __extract_zipfiles(self)->None:
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

    def __data_dir_process(self, folder_path: str) -> YearDataPoint:
        dados_folder = self.__find_dados_folder(folder_path)
        
        if not dados_folder:
            print(f"A pasta 'dados' não foi encontrada em {folder_path}")
            return None

        data_files_list = os.listdir(dados_folder)
        
        is_courses_file = lambda x: "CURSOS" in x.upper()
        filtered_list = list(filter(is_courses_file, data_files_list))
        
        if len(filtered_list) != 1:
            print(f"Número inesperado de arquivos 'CURSOS' em {dados_folder}")
            return None
        
        csv_file = filtered_list[0]
        full_csv_file_path = os.path.join(dados_folder, csv_file)        
        df = self.__process_df(full_csv_file_path)
        
        if df is not None:
            year = self.__extract_year_from_path(folder_path)
            if year:
                return YearDataPoint(df=df, data_year=year)
            else:
                print(f"Não foi possível extrair o ano do caminho: {folder_path}")
        else:
            print(f"DataFrame é None após processamento do arquivo: {full_csv_file_path}")
        
        return None

    def __find_dados_folder(self, base_path):
        for root, dirs, files in os.walk(base_path):
            for dir_name in dirs:
                if dir_name.lower() == "dados":
                    return os.path.join(root, dir_name)
        return None

    def __process_df(self, csv_file_path: str) -> pd.DataFrame:
        RELEVANT_COLS = ["TP_GRAU_ACADEMICO", "TP_NIVEL_ACADEMICO", "TP_ORGANIZACAO_ACADEMICA",
                         "TP_CATEGORIA_ADMINISTRATIVA", "QT_VG_TOTAL", "CO_MUNICIPIO", "TP_MODALIDADE_ENSINO"]
        

        try:
            df = pd.read_csv(csv_file_path, sep=";", encoding="latin-1", usecols=RELEVANT_COLS)
            filtered_df = self.__filter_df(df)
            return filtered_df
        except Exception as e:
            print(f"Erro ao processar o DataFrame: {e}")
            return None

    def __filter_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        city_codes_before_filter = set(df["CO_MUNICIPIO"])
        print(len(city_codes_before_filter))
        
        cols_and_filter_vals = {
            "TP_GRAU_ACADEMICO": [1, 2, 3, 4],
            "TP_NIVEL_ACADEMICO": [1, 2],
            "TP_ORGANIZACAO_ACADEMICA": [1, 2, 3, 4, 5], #equivalentes ás modalidades 
            "TP_CATEGORIA_ADMINISTRATIVA": [1, 2, 3, 4, 5, 7], #equivalente à situação
            "TP_MODALIDADE_ENSINO": [1]
        }

        df = df.dropna(axis="index", subset=["TP_GRAU_ACADEMICO", "TP_NIVEL_ACADEMICO", "TP_ORGANIZACAO_ACADEMICA", "TP_CATEGORIA_ADMINISTRATIVA"])

        for col in ["TP_GRAU_ACADEMICO", "TP_NIVEL_ACADEMICO", "TP_ORGANIZACAO_ACADEMICA", "TP_CATEGORIA_ADMINISTRATIVA"]:
            df[col] = df[col].astype("int")

        for key, val in cols_and_filter_vals.items():
            filtered_series = df[key].apply(lambda x: x in val)
            df = df[filtered_series]

        df["CO_MUNICIPIO"] = df["CO_MUNICIPIO"].astype("int")
        city_codes_after_filter = set(df["CO_MUNICIPIO"])
        print(df.info())
        print(len(city_codes_after_filter))
        
        #código abaixo lida com os municípios que estavam originalmente nos dados (~3300) mas que foram retirados após filtragem
        #esses municípios não devem ter valor NULL (por que os dados deles foram coletados) mas sim valores 0, por não terem cursos que
        #entrem na especificação de filtragem
        removed_city_codes:set[int] = city_codes_before_filter.difference(city_codes_after_filter)

        removed_city_lines = [[code,None,None,None,None,None,0] 
                             for code in removed_city_codes] 
        removed_values_df = pd.DataFrame(removed_city_lines,columns=df.columns)

        df = pd.concat([df,removed_values_df])
        df = df[ df["CO_MUNICIPIO"] != 0 ] #código de município 0 deve ser filtrado 
        
        return df

    def __extract_year_from_path(self, path: str) -> int:
        ano_match = re.search(r'\d{4}', path)
        return int(ano_match.group(0)) if ano_match else None


if __name__ == "__main__":    
    scrapper = HigherEducaPositionsScrapper()
    year_data_points = scrapper.extract_database()
    print(f"tam da lista de year data points: {len(year_data_points)}")
    
