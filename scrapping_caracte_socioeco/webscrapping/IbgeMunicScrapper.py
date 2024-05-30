
from pandas.core.api import DataFrame as DataFrame
from AbstractScrapper import BaseFileType
from IbgePibCidadesScrapper import IbgeBasesScrapper


class IbgeBasesMunicScrapper(IbgeBasesScrapper):
   
   #a base MUNI de municípios do IBGE  tem um link diferente para os dados de cada ano, pelo menos isso é oq da pra ver no HTML da página
   URL_FOR_EACH_YEAR = {
      "2019" : "https://www.ibge.gov.br/estatisticas/sociais/educacao/10586-pesquisa-de-informacoes-basicas-municipais.html?edicao=29466&t=downloads",
      "2020" :  "https://www.ibge.gov.br/estatisticas/sociais/educacao/10586-pesquisa-de-informacoes-basicas-municipais.html?edicao=32141&t=downloads",
      "2021" : "https://www.ibge.gov.br/estatisticas/sociais/educacao/10586-pesquisa-de-informacoes-basicas-municipais.html?edicao=35765&t=downloads",
   }

   @classmethod
   def extract_database(cls,file_type: BaseFileType, priority_to_series_len:bool) -> DataFrame:
      file_links_by_year:dict = cls._get_all_files(file_type,priority_to_series_len)
      #fazer auguma lógica para concatenar os dados de todos os anos, mas para isso é preciso saber como manipular esses dados
      print(file_links_by_year)
      #return super().extract_database()
   
   @classmethod
   def _get_all_files(cls,file_type: BaseFileType, priority_to_series_len:bool)->dict[str,str]:
      file_links_by_year:dict = {}
      
      for year,page_url in cls.URL_FOR_EACH_YEAR.items():
          file_link:str = super()._get_file_link(page_url,file_type,priority_to_series_len)
          file_links_by_year[year] = file_link
      
      return file_links_by_year
  

# links:str = IbgeBasesMunicScrapper.extract_database(BaseFileType.EXCEL,True)
# print(links)