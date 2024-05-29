import selenium.webdriver
import re
import pandas as pd
from html_parser import MyHTMLParser
from AbstractScrapper import AbstractScrapper, BaseFileType

"""
TODO
1) Atualmente esse código foi testado apenas na página de PIB de cidades do IBGE, testar em páginas similares
url da página: https://www.ibge.gov.br/estatisticas/economicas/contas-nacionais/9088-produto-interno-bruto-dos-municipios.html?t=downloads&c=1100023


2) Ver se dá pra implementar esse código com o beatifulSoup4

"""

class IbgeBasesScrapper(AbstractScrapper):

   website_url:str
   file_type: BaseFileType

   def __init__(self, website_url: str, file_type: BaseFileType) -> None:
      self.website_url = website_url
      self.file_type = file_type

   def extract_database(self)->pd.DataFrame:
      """
      Extrai um arquivo e retorna ele como um Dataframe da base de dados do IBGE dado um URL para uma página do IBGE, um identificador da tag HTML que o link do arquivo está e o tipo de dado do arquivo

      Args:
         url (str) : URL da página do IBGE com os dados das bases
         html_tag_identifier (str): uma tag HTML onde o link para o arquivo da base está, usada no web-scrapping
         file_type (Enum BaseFileType): Um elemento do Enum que dita qual o tipo de arquivo será baixado

      Return:
         (pd.Dataframe) : Dataframe do Pandas com os dados baixados   
      """
      
      file_link: str = self.__get_file_link()
      return super()._dataframe_from_link(file_link,self.file_type,zipfile=True) #retorna o dataframe do link extraido
   
   def download_database_locally(self)-> str:
      """
      Extrai um arquivo e baixa ele localmente apenas da base de dados do IBGE dado um URL para uma página do IBGE, um identificador da tag HTML que o link do arquivo está e o tipo de dado do arquivo

      Args:
         url (str) : URL da página do IBGE com os dados das bases
         html_tag_identifier (str): uma tag HTML onde o link para o arquivo da base está, usada no web-scrapping
         file_type (Enum BaseFileType): Um elemento do Enum que dita qual o tipo de arquivo será baixado

      Return:
         (str) : caminho para o arquivo baixado
      """
      
      file_link:str = self.__get_file_link()
      return super()._download_and_extract_zipfile(file_link)

   def __get_file_link(self)->str:
      """
      realiza o web-scrapping e retorna o link para o arquivo da base mais atual e com o tipo de arquivo passado como argumento
      
      """
      DATA_BASE_IDENTIFIER_PATTERN: str = r"Base \d{4}-\d{4}" #regex para achar a string que identifica onde está os arquivos da base de dados
      HTML_TAG_IDENTIFIER:str = "li" #essas são constantes internas da lógica de webscrapping de página desse tipo 

      driver = selenium.webdriver.Chrome()
      driver.get(self.website_url) #acessa a página do url
      page_source:str = driver.page_source #pega o HTML da página
      driver.quit() #fecha o webdriver 

      databases_match = re.finditer(DATA_BASE_IDENTIFIER_PATTERN, page_source) #match no HTML com a string que identifica as bases de dados do ibge
   
      most_recent_data = max(databases_match,key= lambda x : x.group()) #acha a data mais recente entre as matches de banco de dados, isso por que 2010 > 2000 na comparação

      html_parser = MyHTMLParser(HTML_TAG_IDENTIFIER) #instancia um objeto de parser de html
      substr:str = page_source[most_recent_data.start(): ] #corta o html da página a partir da str que representa o banco de dados do IBGE
      
      parsed_html:str = html_parser.get_limited_html_block(substr) #extrai apenas o bloco de html com a tag que tem os links dos arquivos do banco
      lista_links:list[str] = html_parser.get_all_links( parsed_html) #pega todos os links do html extraido
      final_link:str = ""

      for link in lista_links: #acha o link certo para o tipo de dado passado como argumento
         if self.file_type.value in link: #link pra um arquivo que tem o tipo de dado correto
               final_link = link
               break
         
      if not final_link:
         raise RuntimeError("não foi possível extrai o link para o arquivo de dados")
      return final_link


if __name__ == "__main__":
   url = "https://www.ibge.gov.br/estatisticas/economicas/contas-nacionais/9088-produto-interno-bruto-dos-municipios.html?t=downloads&c=1100023"
   scrapper = IbgeBasesScrapper(website_url=url,file_type=BaseFileType.EXCEL)
   
   df:pd.DataFrame = scrapper.extract_database()

   print(df.head())