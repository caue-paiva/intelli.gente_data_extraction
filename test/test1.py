from webscrapping.extractorclasses import DatasusDataExtractor, CategoryDataExtractor, CityPaymentsExtractor
from webscrapping.scrapperclasses import DatasusLinkScrapper, DatasusDataInfo,IbgePibCidadesScrapper, IbgeBasesMunicScrapper, CityPaymentsScrapper
from webscrapping.scrapperclasses import FormalJobsScrapper, IdhScrapper
from webscrapping.extractorclasses import  FormalJobsExtractor
from apiextractors import IbgeAgregatesApi, IpeaViolenceMapApi
from datastructures import BaseFileType, YearDataPoint
import pandas as pd
from citiesinfo import get_city_code_from_string

def run_datasus(abreviation:DatasusDataInfo)->pd.DataFrame:
   scrapper = DatasusLinkScrapper(abreviation)
  
   extractor = DatasusDataExtractor()
   processed_data = extractor.extract_processed_collection(scrapper)

   return processed_data.df

def run_city_gdp(file_type:BaseFileType)->None:
   scrapper = IbgePibCidadesScrapper(file_type)
   extractor = CategoryDataExtractor()
   list_ = extractor.extract_processed_collection(scrapper)

   for collec in list_:
      #print(collec)
      collec.df.to_csv(f"{collec.data_name}.csv")

def run_MUNIC_base(file_type:BaseFileType)->list[pd.DataFrame]:
   scrapper = IbgeBasesMunicScrapper(file_type,False)
   return scrapper.extract_database()

def run_api_agregados():
   api = IbgeAgregatesApi("agregados", "ibge")
   data_points = api.extract_data_points()
   api.print_processed_data(data_points)
   api.save_processed_data_in_csv(data_points,1)

def run_api_ipea():
   api_extractor = IpeaViolenceMapApi()
   list_data_collect = api_extractor.extract_data_points()
   api_extractor.save_processed_data_in_csv(list_data_collect,0)

def run_CAPAG():
   scrapper = CityPaymentsScrapper()
   obj = CityPaymentsExtractor()
   list_ = obj.extract_processed_collection(scrapper)
   list_.df.to_csv(f"CAPAG_PROCESSADO{1}.csv")

def run_formal_jobs():
   scrapper = FormalJobsScrapper()
   obj = FormalJobsExtractor()

   collection = obj.extract_processed_collection(scrapper)
   return collection.df

def run_IDH():
   scrapper = IdhScrapper()
   list_data_points:list[YearDataPoint] = scrapper.extract_database()
   for point in list_data_points:
      point.df.to_csv("teste.csv")


if __name__ == "__main__":
   run_IDH()