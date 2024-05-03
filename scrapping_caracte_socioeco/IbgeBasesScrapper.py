import selenium.webdriver
import re
import pandas as pd
from html_parser import MyHTMLParser
from AbstractScrapper import AbstractScrapper, BaseFileType

"""
TODO
1) Atualmente esse código foi testado apenas na página de PIB de cidades do IBGE, testar em páginas similares
url da página: https://www.ibge.gov.br/estatisticas/economicas/contas-nacionais/9088-produto-interno-bruto-dos-municipios.html?t=downloads&c=1100023

2) ver se é possível remover o argumento do identificador da tag HTML para deixar a interface das classes que herdam da classe
abstrata mais consistente

"""

class IbgeBasesScrapper(AbstractScrapper):

   DATA_BASE_IDENTIFIER_PATTERN: str = r"Base \d{4}-\d{4}" #regex para achar a string que identifica onde está os arquivos da base de dados

   def extract_database(self, website_url: str, file_type: BaseFileType,html_tag_identifier:str)->pd.DataFrame:
      """
      Extrai um arquivo e retorna ele como um Dataframe da base de dados do IBGE dado um URL para uma página do IBGE, um identificador da tag HTML que o link do arquivo está e o tipo de dado do arquivo

      Args:
         url (str) : URL da página do IBGE com os dados das bases
         html_tag_identifier (str): uma tag HTML onde o link para o arquivo da base está, usada no web-scrapping
         file_type (Enum BaseFileType): Um elemento do Enum que dita qual o tipo de arquivo será baixado

      Return:
         (pd.Dataframe) : Dataframe do Pandas com os dados baixados   
      """
      
      file_link: str = self.__get_file_link(website_url=website_url, file_type=file_type, html_tag_identifier=html_tag_identifier)
      return self.__dataframe_from_link(file_link,file_type) #retorna o dataframe do link extraido
   
   def download_database_locally(self, website_url: str, file_type: BaseFileType,html_tag_identifier:str) -> str:
      """
      Extrai um arquivo e baixa ele localmente apenas da base de dados do IBGE dado um URL para uma página do IBGE, um identificador da tag HTML que o link do arquivo está e o tipo de dado do arquivo

      Args:
         url (str) : URL da página do IBGE com os dados das bases
         html_tag_identifier (str): uma tag HTML onde o link para o arquivo da base está, usada no web-scrapping
         file_type (Enum BaseFileType): Um elemento do Enum que dita qual o tipo de arquivo será baixado

      Return:
         (str) : caminho para o arquivo baixado
      """
      
      file_link:str = self.__get_file_link(website_url=website_url,file_type=file_type,html_tag_identifier=html_tag_identifier)
      return super()._download_and_extract_zipfile(file_link,self.__class__.__name__)

   def __get_file_link(self, website_url: str, file_type: BaseFileType,html_tag_identifier:str)->str:
      """
      realiza o web-scrapping e retorna o link para o arquivo da base mais atual e com o tipo de arquivo passado como argumento
      
      """

      driver = selenium.webdriver.Chrome()
      driver.get(website_url) #acessa a página do url
      page_source:str = driver.page_source #pega o HTML da página
      driver.quit() #fecha o webdriver 

      databases_match = re.finditer(self.DATA_BASE_IDENTIFIER_PATTERN, page_source) #match no HTML com a string que identifica as bases de dados do ibge
   
      most_recent_data = max(databases_match,key= lambda x : x.group()) #acha a data mais recente entre as matches de banco de dados, isso por que 2010 > 2000 na comparação

      html_parser = MyHTMLParser(html_tag_identifier) #instancia um objeto de parser de html
      substr:str = page_source[most_recent_data.start(): ] #corta o html da página a partir da str que representa o banco de dados do IBGE
      
      parsed_html:str = html_parser.get_limited_html_block(substr) #extrai apenas o bloco de html com a tag que tem os links dos arquivos do banco
      lista_links:list[str] = html_parser.get_all_links( parsed_html) #pega todos os links do html extraido
      final_link:str = ""

      for link in lista_links: #acha o link certo para o tipo de dado passado como argumento
         if file_type.value in link: #link pra um arquivo que tem o tipo de dado correto
               final_link = link
               break
         
      if not final_link:
         raise RuntimeError("não foi possível extrai o link para o arquivo de dados")
      return final_link

   def __dataframe_from_link(self, file_url:str , file_type: BaseFileType)->pd.DataFrame:
      file_path: str = super()._download_and_extract_zipfile(file_url, self.__class__.__name__) #chama o método da classe parente

      df:pd.DataFrame
      if file_type == BaseFileType.EXCEL:
         df:pd.DataFrame = pd.read_excel(file_path)

      """
      TODO
      Colocar os casos para os outros tipos de arquivos, caso seja possível extrair dataframes deles
      """

      if df is None:
         raise RuntimeError("não foi possível criar um dataframe a partir do link")
      
      return df


if __name__ == "__main__":
   scrapper = IbgeBasesScrapper()
   url = "https://www.ibge.gov.br/estatisticas/economicas/contas-nacionais/9088-produto-interno-bruto-dos-municipios.html?t=downloads&c=1100023"
   file_path:str = scrapper.download_database_locally(website_url=url, html_tag_identifier="li" , file_type=BaseFileType.EXCEL)

   print(file_path)