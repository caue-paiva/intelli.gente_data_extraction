from webscrapping.extractorclasses import *
from webscrapping.scrapperclasses import * #um dos poucos casos que fazer isso é uma boa ideia!
from webscrapping.extractorclasses import DatasusDataExtractor, CategoryDataExtractor, CityPaymentsExtractor
from webscrapping.scrapperclasses import DatasusLinkScrapper, DatasusDataInfo,IbgePibCidadesScrapper, IbgeMunicScrapper, CityPaymentsScrapper
from webscrapping.scrapperclasses import FormalJobsScrapper, SnisScrapper, IbgeCitiesNetworkScrapper
from webscrapping.extractorclasses import  FormalJobsExtractor, IdhExtractor, SnisExtractor, IbgeCitiesNetworkExtractor, IbgeMunicExtractor
from apiextractors import IbgeAgregatesApi, IpeaViolenceMapApi, AnatelApi
from datastructures import  YearDataPoint
import pandas as pd

def run_datasus(abreviation:DatasusDataInfo)->pd.DataFrame:
   scrapper = DatasusLinkScrapper(abreviation)
  
   extractor = DatasusDataExtractor()
   processed_data = extractor.extract_processed_collection(scrapper)

   return processed_data[0].df

def run_city_gdp()->None:
   scrapper = IbgePibCidadesScrapper()
   extractor = CategoryDataExtractor()
   list_ = extractor.extract_processed_collection(scrapper)

   for collec in list_:
      collec.df.to_csv(f"{collec.data_name}.csv")

def run_MUNIC_base()->list[YearDataPoint]:
   extractor = IbgeMunicExtractor()
   collections = extractor.extract_processed_collection()
   for collect in collections:
      print(collect.df.info())
      collect.df.to_csv(f"{collect.data_name}.csv")

def run_api_agregados():
   api = IbgeAgregatesApi()
   data_points = api.extract_data_points()
   api.print_processed_data(data_points)
   api.save_processed_data_in_csv(data_points,1)

def run_api_ipea():
   api_extractor = IpeaViolenceMapApi()
   list_data_collect = api_extractor.extract_data_points()
   api_extractor.save_processed_data_in_csv(list_data_collect,0)

def run_CAPAG():
   obj = CityPaymentsExtractor()
   list_ = obj.extract_processed_collection()
   list_[0].df.to_csv(f"CAPAG_PROCESSADO{1}.csv",index=False)

def run_formal_jobs():
   obj = FormalJobsExtractor()
   collection_list = obj.extract_processed_collection()
   return collection_list[0].df

def run_IDH():
   extractor = IdhExtractor()
   collections = extractor.extract_processed_collection()
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
   extractor = IbgeCitiesNetworkExtractor()
   list_ = extractor.extract_processed_collection()
   for collection in list_:
      print(collection.df.info())
      collection.df.to_csv(f"{collection.data_name}.csv",index=False)

def run_snis():
   extractor = SnisExtractor()
   list_ = extractor.extract_processed_collection()

   for ele in list_:
      ele.df.to_csv(f"{ele.data_name}.csv")

def run_rais():
   scrapper = RaisScrapper(RaisDataInfo.TECH_COMPANIES)
   extractor = RaisExtractor()
   extractor.extract_processed_collection(scrapper)

def run_tech_equipament():
    extractor = TechEquipamentExtractor()
    collection = extractor.extract_processed_collection()
    for colec in collection:
      colec.df.to_csv(f"{colec.data_name}.csv")

def run_Idbe():
   extractor = idebFinalYearsExtractor()
   collection = extractor.extract_processed_collection()
   print(len(collection))

   for colec in collection:
      print(colec.data_name)
      colec.df.to_csv(f"{colec.data_name}.csv")

def parse_csv():
   import os
   files:list[str] = os.listdir(os.getcwd())

   for file in files:
      print(file)
      if ".csv" in file:
         df = pd.read_csv(file,index_col=[0])
         df.to_csv(os.path.join(os.getcwd(),file),index=False)
 
if __name__ == "__main__":
   #run_Idbe()
   run_ANATEL()
   #run_MUNIC_base()