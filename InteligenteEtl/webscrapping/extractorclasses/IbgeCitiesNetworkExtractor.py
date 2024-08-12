from datastructures import ProcessedDataCollection
from .AbstractDataExtractor import AbstractDataExtractor
from webscrapping.scrapperclasses import IbgeCitiesNetworkScrapper
from datastructures import YearDataPoint, DataTypes
import pandas as pd


class IbgeCitiesNetworkExtractor(AbstractDataExtractor):

   DATA_POINTS = {
      "Nível da Hierarquia para as Regiões de Influência das Cidades (VAR09)" : {"column":"nivel_ori","dtype":DataTypes.STRING},
      "Classe Denominação da Hierarquia para as Regiões de Influência das Cidades (VAR10)": {"column":"classe_ori","dtype":DataTypes.STRING}
   }
   EXTRACTED_CITY_CODE_COL = "cod_ori"
   DATA_CATEGORY = "Arranjos Urbanos"

   def __get_processed_collection(self,df:pd.DataFrame, data_name:str,time_series:list[int])->ProcessedDataCollection:
      data_col_name:str = self.DATA_POINTS[data_name]["column"]
      dtype:DataTypes =  self.DATA_POINTS[data_name]["dtype"]

      df = df.loc[:,[self.EXTRACTED_CITY_CODE_COL,data_col_name, self.YEAR_COLUMN]] #cria novo df apenas com as colunas do código da cidade e a coluna do dado atual
      df = df.dropna() #retira os valores nulo

      df = df.rename(
         {
            self.EXTRACTED_CITY_CODE_COL:self.CITY_CODE_COL,
            data_col_name: self.DATA_VALUE_COLUMN 
         }, #renomea a coluna de código do município e dos valores do dado
         axis="columns"
      )
      df[self.DATA_VALUE_COLUMN] = df[self.DATA_VALUE_COLUMN].astype(dtype.value) #muda o tipo de dado da coluna de valores 
      df[self.DTYPE_COLUMN] = dtype.value #add coluna de tipo de dado
      df[self.DATA_IDENTIFIER_COLUMN] = data_name #add coluna do nome do dado

      df = df[~df.duplicated(subset=[self.CITY_CODE_COL],keep="first")] #remove registros duplicados da coluna do codigo de municipio
      df = df.reset_index(drop=True)

      return ProcessedDataCollection(
         category=self.DATA_CATEGORY,
         dtype=dtype,
         data_name=data_name,
         time_series_years=time_series,
         df=df
      )


   def extract_processed_collection(self, scrapper: IbgeCitiesNetworkScrapper) -> list[ProcessedDataCollection]:
      data_points:list[YearDataPoint] = scrapper.extract_database()
      time_series_years:list[int] = YearDataPoint.get_years_from_list(data_points)
      df:pd.DataFrame = self._concat_data_points(data_points)


      data_collections:list[ProcessedDataCollection] = []
      for data_point in self.DATA_POINTS:
          collection = self.__get_processed_collection(df,data_point,time_series_years)
          data_collections.append(collection)

      return data_collections