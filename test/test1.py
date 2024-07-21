from WebScrapping.ExtractorClasses import DatasusDataExtractor, CategoryDataExtractor, CityPaymentsExtractor
from WebScrapping.ScrapperClasses import DatasusLinkScrapper, DatasusDataInfo,IbgePibCidadesScrapper, IbgeBasesMunicScrapper, CityPaymentsScrapper
from WebScrapping.ScrapperClasses import FormalJobsScrapper
from WebScrapping.ExtractorClasses import TableDataPoints, FormalJobsExtractor
from ApiExtractors import IbgeAgregatesApi
from DataClasses import DataTypes, BaseFileType, ProcessedDataCollection

import pandas as pd

def run_datasus(abreviation:DatasusDataInfo)->pd.DataFrame:
   scrapper = DatasusLinkScrapper(abreviation)
  
   extractor = DatasusDataExtractor()
   processed_data = extractor.extract_processed_collection(scrapper)

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

def run_api_agregados():
   api = IbgeAgregatesApi("agregados", "ibge")
   data_points = api.extract_data_points()
   api.print_processed_data(data_points)
   api.save_processed_data_in_csv(data_points,1)

def test_fill_non_existing_cities(df:pd.DataFrame):
   data_col1 = ProcessedDataCollection("PIB Agropecuária",DataTypes.FLOAT,"PIB Agropecuária",[1],df)
   final_df =  data_col1.fill_non_existing_cities()
   final_df.to_csv("final_agr.csv")

def run_CAPAG():
   scrapper = CityPaymentsScrapper()
   obj = CityPaymentsExtractor()
   list_ = obj.extract_processed_collection(scrapper)
   for  index, collection in enumerate(list_):
      collection.df.to_csv(f"CAPAG_PROCESSADO{index}.csv")

def run_formal_jobs():
   scrapper = FormalJobsScrapper()
   obj = FormalJobsExtractor()

   collection = obj.extract_processed_collection(scrapper)
   return collection.df

if __name__ == "__main__":
   #obj = CityPaymentsScrapper()
   #obj.download_database_locally()

   """obj = DatasusLinkScrapper(DatasusDataInfo.ILLITERACY_RATE)
   list_ = obj.extract_database()

   for i,x in enumerate(list_):
      print(x.df.info(),x.data_year)

      x.df.to_csv(f"ginicoef{i}.csv")"""
   
   """obj = FormalJobsScrapper()
   df = obj.extract_database()
   print(df.info())
   df.to_csv("trb_forma.csv")"""

   df = run_formal_jobs()
   print(df.info())

   df.to_csv("trb_formais_pro.csv")
   
   
   
   
