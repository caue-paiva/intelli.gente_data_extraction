
import pandas as pd
from .AbstractScrapper import  BaseFileType
from .IbgePibCidadesScrapper import IbgePibCidadesScrapper
from datastructures import YearDataPoint

class IbgeBasesMunicScrapper(IbgePibCidadesScrapper):
   
   #a base MUNI de municípios do IBGE  tem um link diferente para os dados de cada ano, pelo menos isso é oq da pra ver no HTML da página
   URL_FOR_EACH_YEAR:dict[int,str] = {
      2021 : "https://ftp.ibge.gov.br/Perfil_Municipios/2021/Base_de_Dados/Base_MUNIC_2021_20240425.xlsx",
      2020 : "https://ftp.ibge.gov.br/Perfil_Municipios/2020/Base_de_Dados/Base_MUNIC_2020.xlsx",
      2019 : "https://ftp.ibge.gov.br/Perfil_Municipios/2019/Base_de_Dados/Base_MUNIC_2019_20210817.xlsx",
      2018 : "https://ftp.ibge.gov.br/Perfil_Municipios/2018/Base_de_Dados/Base_MUNIC_2018_xlsx_20201103.zip",
      2017 : "https://ftp.ibge.gov.br/Perfil_Municipios/2017/Base_de_Dados/Base_MUNIC_2017_xls.zip",
      2015 : "https://ftp.ibge.gov.br/Perfil_Municipios/2015/Base_de_Dados/Base_MUNIC_2015_xls.zip",
   }

   file_type: BaseFileType
   priority_to_series_len: bool
   url: str #url colocado aqui para compatibilidade com a classe pai

   def __init__(self,file_type: BaseFileType,priority_to_series_len: bool = False) -> None:
      self.file_type = file_type
      self.priority_to_series_len = priority_to_series_len

   def extract_database(self)-> list[YearDataPoint]:

      data_list: list[YearDataPoint] = []
      for year,url in self.URL_FOR_EACH_YEAR.items():

         format = url.split(".")[-1]
         if(format=='zip'):
            path = super()._download_and_extract_zipfile(url)
         else:
            path = url

         excel_file = pd.ExcelFile(path)
         sheets = excel_file.sheet_names

         shared_columns = list(pd.read_excel(excel_file, sheets[-1]).columns)
         merge_key = shared_columns.pop(0)
         shared_columns.append('Pop')

         df = pd.read_excel(excel_file, sheets[-1])
         sheets.remove(sheets[-1])
         sheets.remove('Dicionário')

         for sheet in sheets:
            df_sheet = pd.read_excel(excel_file, sheet)
            df_sheet = df_sheet.drop(columns=shared_columns, errors='ignore')
            df = pd.merge(df, df_sheet, on=merge_key, how='outer')
         
         print(df)
         data_list.append(
             YearDataPoint(df,year)
         )
      
      return data_list
      



