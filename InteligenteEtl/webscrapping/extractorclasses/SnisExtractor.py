from typing import Iterator
from datastructures import ProcessedDataCollection, DataTypes, YearDataPoint
from .AbstractDataExtractor import AbstractDataExtractor
from webscrapping.scrapperclasses import SnisScrapper
import pandas as pd

class SnisExtractor(AbstractDataExtractor):

   INDICATORS_INFO = { #dicionário com informações dos indicadores
      'IN015_AE': {'name': 'IN015_AE - Índice de coleta de esgoto', 'dtype': DataTypes.FLOAT, 'category': 'Água e Esgoto'},
      'IN015_RS': {'name': 'IN015_RS - Taxa de cobertura do serviço de coleta de resíduo doméstico em relação à população total do município', 'dtype': DataTypes.FLOAT, 'category': 'Resíduos Sólidos'},
      'IN022_AE': {'name': 'IN022_AE - Consumo médio percapita de água', 'dtype': DataTypes.FLOAT, 'category': 'Água e Esgoto'},
      'IN049_AE': {'name': 'IN049_AE - Índice de perdas na distribuição', 'dtype': DataTypes.FLOAT, 'category': 'Água e Esgoto'},
      'IN055_AE': {'name': 'IN055_AE - Índice de atendimento total de água', 'dtype': DataTypes.FLOAT, 'category': 'Água e Esgoto'},
      'CS001': {'name': 'CS001 - Existe coleta seletiva formalizada pela prefeitura no município?', 'dtype': DataTypes.STRING, 'category': 'Resíduos Sólidos'},
      'IN056_AE': {'name': 'IN056_AE - Índice de atendimento total de esgoto referido aos municípios atendidos com água', 'dtype': DataTypes.FLOAT, 'category': 'Água e Esgoto'},
      'IN024_AE': {'name': 'IN024_AE - Índice de atendimento urbano de esgoto referido aos municípios atendidos com água', 'dtype': DataTypes.FLOAT, 'category': 'Água e Esgoto'},
      'IN053_RS': {'name': 'IN053_RS - Taxa de material recolhido pela coleta seletiva (exceto mat. orgânica) em relação à quantidade total coletada de resíduos sólidos domésticos', 'dtype': DataTypes.FLOAT, 'category': 'Resíduos Sólidos'},
      'IN016_AE': {'name': 'IN016_AE - Índice de tratamento de esgoto', 'dtype': DataTypes.FLOAT, 'category': 'Água e Esgoto'},
      'PO007': {'name': 'PO007 - Drenagem urbana e manejo de águas pluviais', 'dtype': DataTypes.BOOL, 'category': 'Gestão de Desastres'},
      'PO028': {'name': 'PO028 - Existência do plano municipal de saneamento básico', 'dtype': DataTypes.BOOL, 'category': 'Água e Esgoto'},
      'AG018': {'name': 'AG018 - Volume de água tratada importado', 'dtype': DataTypes.FLOAT, 'category': 'Água e Esgoto'},
      'AG006': {'name': 'AG006 -Volume de água produzido', 'dtype': DataTypes.FLOAT, 'category': 'Água e Esgoto'},
      'PO048': {'name': 'PO048 -Existência do Plano Municipal de Gestão Integrada de Resíduos Sólidos (PMGIRS)', 'dtype': DataTypes.BOOL, 'category': 'Água e Esgoto'},
   }

   EXTRACTED_YEAR_COL = "Ano de Referência"
   EXTRACTED_CITY_CODE_COL = "Código do Município"

   def __parse_col_dtypes(self,df:pd.DataFrame,column:str)->pd.DataFrame:
      df = df[ df[column] != ''] #remove linhas com valores vazios
      df = df.dropna(axis="index",ignore_index=True,subset=[column]) #remove valores NaN

      indicator_code:str =  column[:column.find("-")-1]
      dtype = self.INDICATORS_INFO[indicator_code]["dtype"]
      df[column] = df[column].astype(dtype.value)
      return df

   def __drop_unecessary_cols(self,df:pd.DataFrame)->pd.DataFrame:
      for col in df.columns:
         #primeira condição são coluna do ano e cod de municicpio, segunda é um char que só tem na coluna de indicadores
         if col in [self.EXTRACTED_CITY_CODE_COL,self.EXTRACTED_YEAR_COL] or '-' in col: 
            continue
         df = df.drop(col,axis="columns")
      
      return df
         
   def __separate_df_by_indicators(self,df:pd.DataFrame)->Iterator[tuple[pd.DataFrame,str]]:
       """
       Separa o Dataframe em outros, cada um com apenas um indicador (uma coluna de dados)

       Return:
         (Iterator[tuple[pd.DataFrame,str]]): Iterador que retorna uma tupla do dataframe e do nome da coluna indicador a que ele se refere por chamada
       """
       for col in df.columns:
          if col in [self.EXTRACTED_CITY_CODE_COL,self.EXTRACTED_YEAR_COL]:
             continue
          yield df.loc[:,[self.EXTRACTED_CITY_CODE_COL,self.EXTRACTED_YEAR_COL,col]],col #itera um dataframe com a coluna de codigos do munic, ano e UM indicador

   def __get_processed_collections(self,df:pd.DataFrame,indicator_col:str,time_series_years:list[int])->ProcessedDataCollection:
      indicator_code:str =  indicator_col[:indicator_col.find("-")-1]
      df = self.__parse_col_dtypes(df,indicator_col) #parsing nas colunas, remove valores vazios e muda tipo de dado
      df = df.rename(
         mapper={
            self.EXTRACTED_CITY_CODE_COL: self.CITY_CODE_COL,
            self.EXTRACTED_YEAR_COL: self.YEAR_COLUMN,
            indicator_col:self.DATA_VALUE_COLUMN #coluna do indicador vira coluna dos valores dos dados
         },
         axis="columns"
      )
      df[self.DTYPE_COLUMN] = self.INDICATORS_INFO[indicator_code]["dtype"].value #coluna de tipo de dado
      df[self.DATA_IDENTIFIER_COLUMN] = self.INDICATORS_INFO[indicator_code]["name"] #coluna de nome do dado

      df = self.update_city_code(df,self.CITY_CODE_COL) #atualiza código de município do IBGE para 7 dígitos
      return ProcessedDataCollection(
            category= self.INDICATORS_INFO[indicator_code]["category"],
            dtype=self.INDICATORS_INFO[indicator_code]["dtype"],
            data_name=self.INDICATORS_INFO[indicator_code]["name"],
            time_series_years=time_series_years,
            df=df
      )

   def extract_processed_collection(self, scrapper: SnisScrapper)-> list[ProcessedDataCollection]:
      data_collection = scrapper.extract_database()
      time_series_years:list[int] = YearDataPoint.get_years_from_list(data_collection)
      df = self._concat_data_points(data_collection,add_year_col=False)
      df = self.__drop_unecessary_cols(df) #remove as colunas desnecessárias
      
      return [self.__get_processed_collections(df=df,indicator_col=indicator_name,time_series_years=time_series_years) for df,indicator_name in self.__separate_df_by_indicators(df)]
