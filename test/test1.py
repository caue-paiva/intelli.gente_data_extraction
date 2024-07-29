from webscrapping.extractorclasses import DatasusDataExtractor, CategoryDataExtractor, CityPaymentsExtractor
from webscrapping.scrapperclasses import DatasusLinkScrapper, DatasusDataInfo,IbgePibCidadesScrapper, IbgeBasesMunicScrapper, CityPaymentsScrapper
from webscrapping.scrapperclasses import FormalJobsScrapper
from webscrapping.extractorclasses import  FormalJobsExtractor
from apiextractors import IbgeAgregatesApi
from datastructures import DataTypes, BaseFileType, ProcessedDataCollection
import pandas as pd

def run_datasus(abreviation:DatasusDataInfo)->pd.DataFrame:
   scrapper = DatasusLinkScrapper(abreviation)
  
   extractor = DatasusDataExtractor()
   processed_data = extractor.extract_processed_collection(scrapper)

   return processed_data.df

def run_city_gdp(file_type:BaseFileType,is_zip_file:bool)->pd.DataFrame:
   
   scrapper = IbgePibCidadesScrapper(file_type,is_zip_file)
   # raw_df = scrapper.extract_database()

   # for point in raw_df:
   #    print(point.df.info())
   #    print(point.data_year)
   extractor = CategoryDataExtractor()
   list_ = extractor.extract_processed_collection(scrapper)

   for collec in list_:
      #print(collec)
      collec.df.to_csv(f"{collec.data_name}.csv")


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

   #city_gdp_table_data = TableDataPoints("Ano","Código do Município")
   # city_gdp_table_data.add_data_points_dicts([pib_agro,pib_percapita])
   # processed_df = extractor.extract_data_points(raw_df,city_gdp_table_data)


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

   run_city_gdp(BaseFileType.EXCEL,True)
   
   
   
