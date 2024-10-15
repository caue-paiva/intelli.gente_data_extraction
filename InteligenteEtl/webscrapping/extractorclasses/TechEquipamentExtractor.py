from datastructures import ProcessedDataCollection, DataTypes, YearDataPoint
from .AbstractDataExtractor import AbstractDataExtractor
from webscrapping.scrapperclasses import TechEquipamentScrapper
import pandas as pd


class TechEquipamentExtractor(AbstractDataExtractor):
   
   EXTRACTED_CITY_COL = "CO_MUNICIPIO"
   DTYPE = DataTypes.INT #a resposta na planilha é se a escola tem aquilo ou não, porém somamos todas as respostas num municípios
   # e teremos um int que represetam quantas escolas naquele município tem aquele equipamento

   DATA_TOPIC = "Educação"
   DATA_POINTS = [ #nomes dos dados que vamos extrair
      "IN_COMP_PORTATIL_ALUNO",
      "IN_DESKTOP_ALUNO",
      "IN_EQUIP_LOUSA_DIGITAL",
      "IN_EQUIP_MULTIMIDIA",
      "IN_INTERNET_APRENDIZAGEM",
      "IN_INTERNET_APRENDIZAGEM",
      "IN_TABLET_ALUNO",
   ]

   __SCRAPPER_CLASS = TechEquipamentScrapper()

   def __agregate_dfs(self,df:pd.DataFrame)->pd.DataFrame:
      grouped_obj = df.groupby([self.EXTRACTED_CITY_COL,self.YEAR_COLUMN])
      columns_to_sum = df.columns.difference([self.YEAR_COLUMN, self.EXTRACTED_CITY_COL])
      return grouped_obj[columns_to_sum].sum().reset_index()

   def __change_dtypes(self,df:pd.DataFrame)->pd.DataFrame:
      cols:list[str] = df.columns
      for col in cols:
         if col.lower() == self.EXTRACTED_CITY_COL.lower():
            continue #coluna é do município
         df[col] = df[col].astype("int") #transforma a coluna em um int para (0.0 -> 0)
      return df

   def __get_data_collection(self,data_name:str,df:pd.DataFrame,time_series_years:list[int])->ProcessedDataCollection:
      new_df = pd.DataFrame() #cria novo DF

      new_df[self.YEAR_COLUMN] = df[self.YEAR_COLUMN] #coloca colunas de ano e código de munic
      new_df[self.CITY_CODE_COL] = df[self.CITY_CODE_COL] 
      new_df[self.DTYPE_COLUMN] = self.DTYPE.value #coluna do tipo do dado
      new_df[self.DATA_IDENTIFIER_COLUMN] = data_name #coluna do nome do dado
      new_df[self.DATA_VALUE_COLUMN] = df[data_name] #coluna  dos valores, que é uma coluna do DF original

      return ProcessedDataCollection(
         category=self.DATA_TOPIC,
         dtype=self.DTYPE,
         data_name=data_name,
         time_series_years=time_series_years,
         df=new_df
      )

   def extract_processed_collection(self, years_to_extract:int=15)-> list[ProcessedDataCollection]:
      data_points:list[YearDataPoint] = self.__SCRAPPER_CLASS.extract_database(years_to_extract)
      time_series_years:list[int] = YearDataPoint.get_years_from_list(data_points)

      joined_df:pd.DataFrame = self._concat_data_points(data_points)
      joined_df = joined_df.dropna() #tirar valores null
      joined_df = self.__change_dtypes(joined_df) #transforma os valores em inteiro para somar
      aggregated_df:pd.DataFrame = self.__agregate_dfs(joined_df) #groupby nos municipios e anos repetidos e agrega por soma 

      aggregated_df = aggregated_df.rename({ #renomea coluna do codigo do munici
         self.EXTRACTED_CITY_COL:self.CITY_CODE_COL
      },axis="columns")

      return [ #retorna uma lista de objetos ProcesseDataCollection, cada um deles representando um dado que compoẽ o indicador
        self.__get_data_collection(data_point,aggregated_df,time_series_years) for data_point in self.DATA_POINTS
      ]