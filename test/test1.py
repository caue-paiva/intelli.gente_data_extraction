from WebScrapping.ExtractorClasses import DatasusDataExtractor, CategoryDataExtractor
from WebScrapping.ScrapperClasses import DatasusLinkScrapper, DatasusAbreviations,IbgePibCidadesScrapper, IbgeBasesMunicScrapper
from WebScrapping.ExtractorClasses import TableDataPoints
from DataEnums import DataTypes, BaseFileType
import pandas as pd


def run_datasus(url , abreviation:DatasusAbreviations)->pd.DataFrame:
   scrapper = DatasusLinkScrapper(url ,abreviation)
  
   extractor = DatasusDataExtractor()
   processed_data = extractor.extract_processed_collection(scrapper,"educacao","taxa de analfabetismo")

   return processed_data.df

def run_city_gdp(url:str,file_type:BaseFileType,is_zip_file:bool)->pd.DataFrame:
   scrapper = IbgePibCidadesScrapper(url,file_type,is_zip_file,False)
   raw_df = scrapper.extract_database()
   extractor = CategoryDataExtractor()

   pib_percapita = {
      "data_category": "caracterizacao_socio_economica",
      "data_name": "PIB per capita",
      "column_name": """Produto Interno Bruto per capita,  a preços correntes (R$ 1,00)""",
      "data_type": DataTypes.FLOAT,
      "multiply_amount": 1
   }

   pib_agro = {
      "data_category": "caracterizacao_socio_economica",
      "data_name": "PIB Agropecuária",
      "column_name": """Valor adicionado bruto da Agropecuária,  a preços correntes (R$ 1.000)""",
      "data_type": DataTypes.FLOAT,
      "multiply_amount": 1000
   }

   city_gdp_table_data = TableDataPoints("Ano","Código do Município")
   city_gdp_table_data.add_data_points_dicts([pib_agro,pib_percapita])
   processed_df = extractor.extract_data_points(raw_df,city_gdp_table_data)

   return processed_df

def run_MUNIC_base(file_type:BaseFileType)->list[pd.DataFrame]:
   scrapper = IbgeBasesMunicScrapper(file_type,False)
   return scrapper.extract_database()


if __name__ == "__main__":
   url_datasus_literacy_rate = "http://tabnet.datasus.gov.br/cgi/deftohtm.exe?ibge/censo/cnv/alfbr.csv"
   url_datasus_gini_coef = "http://tabnet.datasus.gov.br/cgi/ibge/censo/cnv/ginibr.def"
   url_city_gdp = "https://www.ibge.gov.br/estatisticas/economicas/contas-nacionais/9088-produto-interno-bruto-dos-municipios.html?=&t=downloads"


   df = run_city_gdp(url_city_gdp,BaseFileType.EXCEL,True)
   print(df.head())
