import re

class MyHTMLParser():
    
   limiter_tag: str #tag externa que limita o bloco de HTML, ex: <li> até </li>
   limiter_end_regex: str

   def __init__(self, limiter_tag:str)->None:
        self.limiter_tag = limiter_tag
        self.limiter_end_regex = rf"</{limiter_tag}>"

   def get_limited_html_block(self,raw_html:str)->str:
         """
         Dado a tag HTML delimitadora configurada na instanciação da classe, acha a primeira tag de final </tag> desse tipo e retorna o html do começo 
         Até essa tag. Esse método apenas funciona se o HTMl não estiver "nested" com vários níveis da mesma tag   
         """  
         
         end_delimiter_match = re.search(self.limiter_end_regex, raw_html) #acha a primeia occorencia do delimitador final da tag
         if end_delimiter_match:
            end_delimiter_index = end_delimiter_match.end()
            return raw_html[:end_delimiter_index]
         else:  #caso nenhum delimitador for achado
            raise ValueError("Não foi possível processar esse bloco de html")
   
   def get_all_links(self,parsed_html:str)->list[str]:
      href_pattern = r'href'
      link_pattern = r'"([^"]*)"' #padrão regex para um string de qualquer tamanho entre aspas


      href_matches = re.finditer(href_pattern,parsed_html)
      link_list:list[str] = []

      if href_matches:
         for match_ in href_matches:
            match_start_index: int = match_.start() #começo da match do link
            link_str:str = re.search(link_pattern,parsed_html[match_start_index:]).group()
            link_str:str = link_str.replace('"','') #tira as aspas que estavam no html
            link_list.append(link_str)

         return link_list
      else: 
         print("Não foi encontrado nenhum link")
         return []


if __name__ == "__main__":
   html = """
   Base 2010-2021 - <strong>(<a href="https://ftp.ibge.gov.br/Pib_Municipios/2021/base/base_de_dados_2010_2021_xlsx.zip">xlsx </a>| <a href="https://ftp.ibge.gov.br/Pib_Municipios/2021/base/base_de_dados_2010_2021_ods.zip">ods </a>| <a href="https://ftp.ibge.gov.br/Pib_Municipios/2021/base/base_de_dados_2010_2021_txt.zip">txt</a>)</strong></li>
   </ul>
   <p></p>
   <h4>Mapa interativo</h4>
   <p><a href="https://www.ibge.gov.br/apps/pibmunic" target="_blank" rel="noopener noreferrer">PIB dos Municípios</a></p><hr>
   <p style="font-size: 12px; line-height: 16px; margin-top: -15px;">O IBGE adota uma política d

   """
   parser = MyHTMLParser("li")
   print(parser.get_limited_html_block(html))

   
       

   