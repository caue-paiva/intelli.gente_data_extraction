from WebScrapping.ExtractorClasses import DatasusDataExtractor
from WebScrapping.ScrapperClasses import DatasusLinkScrapper, DatasusAbreviations

if __name__ == "__main__":
   url = "http://tabnet.datasus.gov.br/cgi/deftohtm.exe?ibge/censo/cnv/alfbr.csv"
   scrapper = DatasusLinkScrapper(url ,DatasusAbreviations.ILLITERACY_RATE)
  
   extractor = DatasusDataExtractor()
   processed_data = extractor.extract_processed_collection(scrapper,"educacao","taxa de analfabetismo")

   print(processed_data.df.head(5))