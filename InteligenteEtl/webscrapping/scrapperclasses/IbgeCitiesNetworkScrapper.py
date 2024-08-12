import requests,re
from .AbstractScrapper import AbstractScrapper
import pandas as pd
from typing import Iterable
from datastructures import YearDataPoint



class IbgeCitiesNetworkScrapper(AbstractScrapper):

   URL = "https://www.ibge.gov.br/geociencias/cartas-e-mapas/redes-geograficas/15798-regioes-de-influencia-das-cidades.html?=&t=downloads"

   def __get_file_link(self)->Iterable[tuple[str,int]]:
      response = requests.get(self.URL) #request get pro site
      page_html: str = response.content.decode()  #pega conteudo html
      
      file_regex_pattern = r"REGIC\d{4}_Ligacoes_entre_Cidades.xlsx" #regex para achar os arquivos
      matches = list(re.finditer(file_regex_pattern,page_html))
      
      year_regex_pattern = r"(\d{4})"
      get_data_year = lambda x: int(re.findall(year_regex_pattern,x)[0]) #pega o ano do dado a partir do link
      
      for match in matches:
         match_end = match.end()
         sliced_html = page_html[:match_end]
         url_start = sliced_html.rfind('"')

         file_link = page_html[url_start+1:match_end]
         yield file_link , get_data_year(file_link)
  
   def extract_database(self)->list[YearDataPoint]:
      data_points:list[YearDataPoint] = []

      for link,year in self.__get_file_link():
         df = pd.read_excel(link)
         data_points.append(
            YearDataPoint(df,year)
         )
      
      return data_points

