from datastructures import BaseFileType
from .IbgePibCidadesScrapper import IbgePibCidadesScrapper
from datastructures import YearDataPoint

class IbgeMunicScrapper(IbgePibCidadesScrapper):
   
   #a base MUNI de municípios do IBGE  tem um link diferente para os dados de cada ano, pelo menos isso é oq da pra ver no HTML da página
   URL_FOR_EACH_YEAR:dict[int,str] = {
      # 2021 : "https://ftp.ibge.gov.br/Perfil_Municipios/2021/Base_de_Dados/Base_MUNIC_2021_20240425.xlsx",
      2020 : "https://ftp.ibge.gov.br/Perfil_Municipios/2020/Base_de_Dados/Base_MUNIC_2020.xlsx",
      # 2019 : "https://ftp.ibge.gov.br/Perfil_Municipios/2019/Base_de_Dados/Base_MUNIC_2019_20210817.xlsx",
      # 2018: "https://ftp.ibge.gov.br/Perfil_Municipios/2018/Base_de_Dados/Base_MUNIC_2018_xlsx_20201103.zip", 
      # 2017 : "https://ftp.ibge.gov.br/Perfil_Municipios/2017/Base_de_Dados/Base_MUNIC_2017_xls.zip",
      # 2015 : "https://ftp.ibge.gov.br/Perfil_Municipios/2015/Base_de_Dados/Base_MUNIC_2015_xls.zip",
      # 2014 : "https://ftp.ibge.gov.br/Perfil_Municipios/2014/base_MUNIC_xls_2014.zip",
      # 2013 : "https://ftp.ibge.gov.br/Perfil_Municipios/2013/base_MUNIC_xls_2013.zip",
      # 2012 : "https://ftp.ibge.gov.br/Perfil_Municipios/2012/base_MUNIC_xls_2012.zip",
      # 2011 : "https://ftp.ibge.gov.br/Perfil_Municipios/2011/base_MUNIC_xls_2011.zip",
      # 2009 : "https://ftp.ibge.gov.br/Perfil_Municipios/2009/base_MUNIC_2009.zip",
      # 2008 : "https://ftp.ibge.gov.br/Perfil_Municipios/2008/Base2008.zip",
      # 2006 : "https://ftp.ibge.gov.br/Perfil_Municipios/2006/base_MUNIC_2006.zip",
      # 2005 : "https://ftp.ibge.gov.br/Perfil_Municipios/2005/base_MUNIC_2005.zip",
      # 2004: "https://ftp.ibge.gov.br/Perfil_Municipios/2004/base_MUNIC_2004.zip"
   }

   REDUNDANT_COLUMNS:dict[int,list[str]] = {
      2021 : ['UF', 'Cod UF', 'Mun', 'Pop', 'Pop estimada 2021', 'Faixa_pop', 'Regiao'],
      2020 : ['UF', 'Cod UF', 'Mun', 'Faixa_pop', 'Regiao'],
      2019 : ['UF', 'COD UF', 'NOME MUNIC', 'CLASSE POP', 'REGIAO'],
      2018:  ['REGIAO', 'COD UF', 'CLASSE POP',  'Desc mun', 'teste', 'NOME MUNIC', 'CodMun', 'Cod Municipio.1'],
      2017 : ['CodMun.1'],
      2015 : ['Codigouf', 'Codigomunicipio', 'Nome'],
      2014 : [],
      2013 : [],
      2012 : [],
      2011 : [],
      2009 : [],
      2008 : [],
      2006 : [],
      2005 : [],
      2004 : [],
      2002 : [],
      2001 : []
   }

   EXTERNAL_VARIABLES_RENAMES:dict[int,dict[str,str]] = {
      2021 : {"Pop estimada 2021": "Populacao"},
      2020 : {},
      2019 : {"REGIAO": "Regiao", "COD UF" : "Cod UF", "NOME MUNIC" : "Mun", "POP EST" : "Populacao", "CLASSE POP" : "Faixa_pop"},
      2018:  {"REGIAO": "Regiao", "COD UF" : "Cod UF", "NOME MUNIC" : "Mun", "POP EST" : "Populacao", "CLASSE POP" : "Faixa_pop"},
      2017 : {"REGIAO": "Regiao", "COD UF" : "Cod UF", "NOME MUNIC" : "Mun", "POP EST" : "Populacao", "CLASSE POP" : "Faixa_pop"},
      2015 : {"A199": "Regiao", "A200" : "Cod UF", "A201" : "UF", "A202" : "Mun", "A204" : "Populacao", "A203" : "Faixa_pop"},
      2014 : {"A1024": "Regiao", "A1022" : "Cod UF", "A1026" : "UF", "A1027" : "Mun", "A1028" : "Populacao", "A1029" : "Faixa_pop"},
      2013 : {"A393": "Regiao", "A391" : "Cod UF", "A1026" : "UF", "A394" : "Mun", "A395" : "Populacao", "A396" : "Faixa_pop"},
      2012 : {"A534": "Regiao", "A535" : "Cod UF", "A536" : "UF", "A537" : "Mun", "A539" : "Populacao", "A538" : "Faixa_pop"},
      2011 : {"A567": "Regiao", "A568" : "Cod UF", "A569" : "UF", "A570" : "Mun", "A572" : "Populacao", "A571" : "Faixa_pop"},
      2009 : {"A737": "Regiao", "A738" : "Cod UF", "A739" : "UF", "A741" : "Mun", "A740" : "Faixa_pop"},
      2008 : {"A335": "Regiao", "A336" : "Cod UF", "A337" : "UF", "A338" : "Mun", "A339" : "Populacao", "A340" : "Faixa_pop"},
      2006 : {"A355": "Regiao", "A356" : "Cod UF", "A357" : "Mun", "A358" : "Populacao", "A359" : "Faixa_pop"},
      2005 : {"A324": "Regiao", "A325" : "Cod UF", "A326" : "Mun", "A327" : "Populacao", "A328" : "Faixa_pop"},
      2004 : {'A158' : "Regiao", "A159" : "UF", "A164" : "Mun", "A161" : "Populacao", "A162" : "Faixa_pop"}
   }

   EXTERNAL_VARIABLES_DROPS:dict[int,list[str]] = {
      2021 : [],
      2020 : [],
      2019 : [],
      2018:  [],
      2017 : [],
      2015 : [],
      2014 : ['A1023', 'A1025'],
      2013 : ['A392'],
      2012 : [],
      2011 : [],
      2009 : [],
      2008 : [],
      2006 : [],
      2005 : ['A329'],
      2004 : ['A160', 'A163'],
      2002 : [],
      2001 : [],
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
         print(year)

         format = url.split(".")[-1]
         if(format=='zip'):
            path = super()._download_and_extract_zipfile(url)
         else:
            path = url

         excel_file = pd.ExcelFile(path)
         sheets = excel_file.sheet_names

         df = pd.read_excel(excel_file, sheets[-1])
         df = df.rename(columns=self.EXTERNAL_VARIABLES_RENAMES[year])
         df = df.drop(columns=self.EXTERNAL_VARIABLES_DROPS[year])

         sheets_to_ignore = [sheets[-1], sheets[0]]
         if(year==2017):
            sheets_to_ignore.append(sheets[1])
         
         for sheet in sheets_to_ignore:
            sheets.remove(sheet)

         city_code = list(pd.read_excel(excel_file, sheets[-1]).columns).pop(0)

         for sheet in sheets:
            df_sheet = pd.read_excel(excel_file, sheet)
            if((year==2019 and sheet=='Recursos humanos') or (year==2013 and sheet=='Legislação')):
               df_sheet = df_sheet[df_sheet[city_code].notna()]
            df_sheet = df_sheet.drop(columns=self.REDUNDANT_COLUMNS[year], errors='ignore')
            df = pd.merge(df, df_sheet, on=city_code, how='outer')

         if(year==2018):
            df = df.rename(columns={'Cod Municipio' : 'CodMun'})
         if(year<=2015):
            df = df.rename(columns={'A1' : 'CodMun'})

         df.columns = df.columns.str.upper()

         print(df)

         data_list.append(
            YearDataPoint(df,year)
         )

         # df.to_csv(f'csvs/{year}.csv')

      
      return data_list
      



