import pandas as pd
from .AbstractDataExtractor import AbstractDataExtractor
from webscrapping.scrapperclasses import IbgePibCidadesScrapper
from datastructures import DataTypes, ProcessedDataCollection, YearDataPoint
from enum import Enum

"""
Existem cidades com nomes duplicados, vai ser usado o código de município para identificar esses municípios, porém bases antigas como o datasus 2010 usam
um código quase identico porém com 6 dígitos e não 7, isso deve ser levado em conta no ETL, Webscrapping

"""

class CitiesGDPDataInfo(Enum):
   """
   TODO: Colocar os 2 dados faltando dessa base

   Enum para representar todas as possibilidades de dados que estão presentes do DF/tabela extraida do WebScrapping
   """
   PERCAPITA_GDP = {
      "data_category": "caracterizacao_socio_economica",
      "data_name": "PIB per capita",
      "column_name": """Produto Interno Bruto per capita, 
       a preços correntes
       (R$ 1,00)""",
      "dtype": DataTypes.FLOAT,
      "multiply_amount": 1
   }
   AGRI_GDP = {
      "data_category": "caracterizacao_socio_economica",
      "data_name": "PIB Agropecuária",
      "column_name": """Valor adicionado bruto da Agropecuária, 
   a preços correntes
   (R$ 1.000)""",
      "dtype": DataTypes.FLOAT,
      "multiply_amount": 1000
   }
   #INDUSTRY_GDP = {

  # }

class CategoryDataExtractor(AbstractDataExtractor):

   EXTRACTED_DATA_YEAR_COL = "Ano"
   EXTRACTED_DATA_CITY_CODE_COL = "Código do Município"

   def extract_processed_collection(self,scrapper: IbgePibCidadesScrapper)->list[ProcessedDataCollection]:
      """ 
      TODO: Completar a função 


      extrai o dataframe bruto da base em um df menor apenas com as colunas de ano, código de cidade e os pontos de dados buscados
      
      Args:
        scrapper (IbgePibCidadesScrapper) : um objeto dessa classe ou de uma classe filha que implemente a interface extract_base       
      Return:
        (list[ProcessedDataCollection]): lista de objetos ProcessedDataCollection, cada um sendo um dado diferente e pronto para entrar no BD
      """

      list_datapoints:list[YearDataPoint] = scrapper.extract_database()
      parsed_datapoints:list[YearDataPoint] = self.__drop_cols(list_datapoints)
      
      concatenated_df:pd.DataFrame = self._concat_data_points(parsed_datapoints,add_year_col=False)
      concatenated_df = self.__change_col_dtypes(concatenated_df)
      
      time_series_years:list[int] = YearDataPoint.get_years_from_list(list_datapoints)

      processed_data_list:list[ProcessedDataCollection] = []
      for data_point in CitiesGDPDataInfo:
         collection = self.__get_processed_collection(concatenated_df,data_point,time_series_years)
         processed_data_list.append(
            collection
      )
      return processed_data_list

   def __change_col_dtypes(self,df:pd.DataFrame)->pd.DataFrame:
      df[self.CITY_CODE_COL] = df[self.CITY_CODE_COL].astype("int")
      df[self.YEAR_COLUMN] = df[self.YEAR_COLUMN].astype("int")

      for point in CitiesGDPDataInfo:
         dtype = point.value["dtype"] #tipo de dado do datapoint
         col_name = self.parse_strings(point.value["column_name"])
         df[col_name] = df[col_name].astype(dtype.value)

      return df

   def __drop_cols(self,list_datapoints:list[YearDataPoint])->list[YearDataPoint]:
      """
      Dado uma lista de YearDataPoints, remove as colunas que não são o código da cidade e o nome dos dados buscados
      """

      columns_to_keep:list[str] = list(item.value["column_name"] for item in CitiesGDPDataInfo) #cria um conjunto com os nomes das colunas de todos os membros do enum      
      columns_to_keep.append(self.EXTRACTED_DATA_CITY_CODE_COL) #add a coluna dos códigos dos municípios
      columns_to_keep.append(self.EXTRACTED_DATA_YEAR_COL) #add a coluna do ano dos dados
      columns_to_keep = list(map(self.parse_strings,columns_to_keep)) #da parsing nas strings dos dados

      parsed_data_points:list[YearDataPoint] = []
      for datapoint in list_datapoints:
         df = datapoint.df
         cols:list[str] = df.columns
         cols:list[str] = list(map(self.parse_strings,cols)) #parsing no nome das colunas e transforma num conjunto
         df.columns = cols #df agora tem as colunas com nomes tratados

         cols_to_drop:list[str] = [x for x in cols if x not in columns_to_keep] #diferença de conjuntos entre todas as colunas e as que vão ficar
         new_df:pd.DataFrame = df.drop(columns=cols_to_drop) 

         new_df = new_df.rename( #renomea os nomes das colunas do df com a função de dar parsing em strings
            mapper = self.parse_strings,
            axis="columns"
         )
         parsed_city_code_col:str = self.parse_strings(self.EXTRACTED_DATA_CITY_CODE_COL) #parsing no nome da coluna de códigos do município original
         parsed_year_col:str = self.parse_strings(self.EXTRACTED_DATA_YEAR_COL)

         new_df = new_df.rename(
            {parsed_city_code_col: self.CITY_CODE_COL, parsed_year_col:self.YEAR_COLUMN},
            axis="columns"
         ) #muda o nome da coluna de código dos municípios e do ano dos dados

         new_df = new_df.dropna(axis="index") #tira as linhas com valores nulos
         parsed_data_points.append(
            YearDataPoint(new_df,datapoint.data_year)
         )   
      
      return parsed_data_points

   def __get_processed_collection(self,df:pd.DataFrame, data_info:CitiesGDPDataInfo,time_series_years:list[int])->ProcessedDataCollection:
      new_df = pd.DataFrame()

      new_df[self.CITY_CODE_COL] = df[self.CITY_CODE_COL] #copia coluna de código do município
      new_df[self.YEAR_COLUMN] = df[self.YEAR_COLUMN] #copia coluna do ano

      data_values_col:str =  self.parse_strings(data_info.value["column_name"]) #nome do dado com parsing
      multiply_amount:int = data_info.value["multiply_amount"] #quantas vezes o dado deve ser multiplicado para atingir o valor não truncado
      new_df[self.DATA_VALUE_COLUMN] = df[data_values_col].apply(lambda x: x*multiply_amount) #cria uma coluna do valor dos dados, multiplicando ele caso seja necessário

      new_df[self.DATA_IDENTIFIER_COLUMN] = data_info.value["data_name"] #coluna do nome final do dado
      new_df[self.DTYPE_COLUMN] = data_info.value["dtype"].value #coluna do tipo de dado

      return ProcessedDataCollection(
         category=data_info.value["data_category"],
         dtype=data_info.value["dtype"],
         data_name=data_info.value["data_name"],
         time_series_years=time_series_years,
         df= new_df
      )

      





