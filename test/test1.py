from webscrapping.extractorclasses import DatasusDataExtractor, CategoryDataExtractor, CityPaymentsExtractor
from webscrapping.scrapperclasses import DatasusLinkScrapper, DatasusDataInfo,IbgePibCidadesScrapper, IbgeBasesMunicScrapper, CityPaymentsScrapper
from webscrapping.scrapperclasses import FormalJobsScrapper, IbgeBasesMunicScrapper,IdhScrapper, SnisScrapper, IbgeCitiesNetworkScrapper,RaisScrapper
from webscrapping.scrapperclasses import RaisDataInfo
from webscrapping.extractorclasses import  FormalJobsExtractor, IdhExtractor, SnisExtractor, IbgeCitiesNetworkExtractor,RaisExtractor
from apiextractors import IbgeAgregatesApi, IpeaViolenceMapApi, AnatelApi
from datastructures import BaseFileType, YearDataPoint
import pandas as pd

def run_datasus(abreviation:DatasusDataInfo)->pd.DataFrame:
   scrapper = DatasusLinkScrapper(abreviation)
  
   extractor = DatasusDataExtractor()
   processed_data = extractor.extract_processed_collection(scrapper)

   return processed_data[0].df

def run_city_gdp(file_type:BaseFileType)->None:
   scrapper = IbgePibCidadesScrapper(file_type)
   extractor = CategoryDataExtractor()
   list_ = extractor.extract_processed_collection(scrapper)

   for collec in list_:
      #print(collec)
      collec.df.to_csv(f"{collec.data_name}.csv")

def run_MUNIC_base(file_type:BaseFileType)->list[YearDataPoint]:
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
   list_[0].df.to_csv(f"CAPAG_PROCESSADO{1}.csv")

def run_formal_jobs():
   scrapper = FormalJobsScrapper()
   obj = FormalJobsExtractor()

   collection_list = obj.extract_processed_collection(scrapper)
   return collection_list[0].df

def run_IDH():
   scrapper = IdhScrapper()
   extractor = IdhExtractor()
   collections = extractor.extract_processed_collection(scrapper)
   for collect in collections:
      print(collect.df.info())
      collect.df.to_csv("idh-m.csv",index=False)

def run_ANATEL():
   obj = AnatelApi()
   list_ = obj.extract_data_points()
   for collect in list_:
      print(collect.df.info())
      collect.df.to_csv(f"{collect.data_name}.csv")

def ibge_cities_network():
   obj = IbgeCitiesNetworkScrapper()
   extractor = IbgeCitiesNetworkExtractor()
   list_ = extractor.extract_processed_collection(obj)
   for collection in list_:
      print(collection.df.info())
      collection.df.to_csv(f"{collection.data_name}.csv")

def run_snis():
   scrapper = SnisScrapper()
   extractor = SnisExtractor()
   list_ = extractor.extract_processed_collection(scrapper)

   for ele in list_:
      ele.df.to_csv(f"{ele.data_name}")

def run_rais():
   scrapper = RaisScrapper(RaisDataInfo.TECH_JOBS)
   #scrapper.extract_database()
   extractor = RaisExtractor()
   extractor.extract_processed_collection(scrapper)


if __name__ == "__main__":
   run_rais()
   