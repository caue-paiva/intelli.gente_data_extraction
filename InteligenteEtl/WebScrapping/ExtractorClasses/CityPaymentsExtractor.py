from typing import Type
from DataClasses import ProcessedDataCollection, YearDataPoint , DataTypes
from .AbstractDataExtractor import AbstractDataExtractor
from WebScrapping.ScrapperClasses import CityPaymentsScrapper
import pandas as pd
import os

class CityPaymentsExtractor(AbstractDataExtractor):

   EXTRACTED_DATA_COLS = ["nota 1","nota 2","nota 3"]
   EXTRACTED_CITY_CODE_COLS = ("código município completo","cod.ibge") #2 possibilidades pra a coluna com os códigos dos municípios
   
   DATA_NAME_FOR_EACH_COL = { #mapeamento de cada coluna pra um nome de dado
      "nota 1":"CAPAG nota indicador endividamento",
      "nota 2":"CAPAG nota indicador poupança corrente",
      "nota 3":"CAPAG nota indicador liquidez"
   }

   NULL_IDENTIFIER = ("n.d.","N.D.")

   DATA_CATEGORY = "Finanças Públicas"
   DTYPE = DataTypes.STRING

   def extract_processed_collection(self, scrapper_class_obj: CityPaymentsScrapper)-> list[ProcessedDataCollection]:
      list_of_datapoints:list[YearDataPoint] = scrapper_class_obj.extract_database()
      
      acess_and_parse_dfs = lambda x: YearDataPoint(self.__parse_columns(x.df),x.data_year) #aplica a função de parsing das colunas no df de cada objeto
      parsed_datapoints:list[YearDataPoint] = list(map(acess_and_parse_dfs,list_of_datapoints)) #da parsing na coluna de todos os dataframes
      
      appended_df:pd.DataFrame = self._concat_data_points(parsed_datapoints) #junta os dataframes dos anos
      separated_dfs:list[pd.DataFrame] = self.__separate_df_by_dataname(appended_df)
      time_series_years:list[int] = [int(x.data_year) for x in list_of_datapoints]

      collection_list:list[ProcessedDataCollection] = []
      for df, data_name in zip(separated_dfs,self.DATA_NAME_FOR_EACH_COL.values()):
         collection_list.append(
            ProcessedDataCollection(
                  category=self.DATA_CATEGORY,
                  dtype=self.DTYPE,
                  data_name=data_name,
                  time_series_years=time_series_years,
                  df=df
            )
      )
      return collection_list
   
   def __parse_columns(self,df:pd.DataFrame)->pd.DataFrame:
      """
      Processa e padroniza nomes de colunas e removes colunas desnecessárias
      """
      df_cols:list[str] = df.columns
      parse_col_names = lambda x: x.lower().replace("_"," ") #coloca os nomes em lowercase e troca underline por espaço para padronizar
      df_cols = list(map(parse_col_names,df_cols))
      df.columns = df_cols #atualiza as colunas do df

      required_cols: list[str] = []
      if self.EXTRACTED_CITY_CODE_COLS[0] in df_cols: #caso do primeiro nome pra coluna de cod de municipio
         df = df.rename({self.EXTRACTED_CITY_CODE_COLS[0]: self.CITY_CODE_COL},axis="columns") #muda nome do codigo do munic para o padrão
         df_cols.remove(self.EXTRACTED_CITY_CODE_COLS[0])
      elif self.EXTRACTED_CITY_CODE_COLS[1] in df_cols:
         df = df.rename({self.EXTRACTED_CITY_CODE_COLS[1]: self.CITY_CODE_COL},axis="columns") #muda nome do codigo do munic para o padrão
         df_cols.remove(self.EXTRACTED_CITY_CODE_COLS[1])
      else:
         raise RuntimeError("Não foi possível achar a coluna do código do município")
      
      required_cols = self.EXTRACTED_DATA_COLS + [self.CITY_CODE_COL]
      cols_to_drop:list[str] = [x for x in df_cols if x not in required_cols]
      df = df.drop(cols_to_drop,axis="columns") #tira as colunas desnecessárias

      df = df.replace({self.NULL_IDENTIFIER[0]:None,self.NULL_IDENTIFIER[1]:None})
      df = df.dropna(axis="index") #tira as linhas com NA em qualquer das colunas

      return self.__change_col_dtypes(df) #muda os tipos de dados das colunas do df

   def __change_col_dtypes(self,df:pd.DataFrame)->pd.DataFrame:
      """
      Muda os tipos de dados das colunas já existentes
      """
      df[self.CITY_CODE_COL] = df[self.CITY_CODE_COL].astype("str")
      for name in self.DATA_NAME_FOR_EACH_COL:
         df[name] = df[name].astype("category") #os dados são notas A,B,C. então elas podem ser otimizadas mudando elas para categorias
      return df
   
   def __separate_df_by_dataname(self,df:pd.DataFrame)->list[pd.DataFrame]:
      """
      Separa cada coluna de dados do dataframe (nota 1,2 e 3) em um dataframe unico cada, retornando a lista deles.
      OBS: a lista está na ordem acima das notas (crescente)
      """
      df_list:list[pd.DataFrame] = []
      for col_name,data_point_name in self.DATA_NAME_FOR_EACH_COL.items(): #loop pelo nome da coluna de cada dado
         
         single_data_point = pd.DataFrame()
         single_data_point[self.YEAR_COLUMN] = df[self.YEAR_COLUMN] #copia coluna de anos
         single_data_point[self.CITY_CODE_COL] = df[self.CITY_CODE_COL] #copia coluna de codigo de município
         single_data_point[self.DATA_VALUE_COLUMN] = df[col_name] #coluna de valor dos dados é o dado específico de uma coluna
         single_data_point[self.DATA_IDENTIFIER_COLUMN] = data_point_name #coluna de nome dos dado é o nome do dado no mapping da classe
         single_data_point[self.DTYPE_COLUMN] = self.DTYPE.value #coluna de tipo de dado
         df_list.append(single_data_point)

      return df_list

   def test(self)-> list[ProcessedDataCollection]:
      """
      Método para testar a classe sem webscrapping, apenas processando arquivos locais.
      Precisa dos arquivos CSV no mesmo diretório de onde é chamada o método para funcionar 
      """
      first_year:int = 2018
      last_year:int = 2024

      list_of_datapoints: list[YearDataPoint] = []

      for i in range(first_year,last_year+1):
         df = pd.read_csv(os.path.join(os.getcwd(),f"CAPAG{i}.csv"))
         list_of_datapoints.append(YearDataPoint(df,str(i)))

      acess_and_parse_dfs = lambda x: YearDataPoint(self.__parse_columns(x.df),x.data_year) #aplica a função de parsing das colunas no df de cada objeto
      parsed_datapoints:list[YearDataPoint] = list(map(acess_and_parse_dfs,list_of_datapoints)) #da parsing na coluna de todos os dataframes
      
      appended_df:pd.DataFrame = self._concat_data_points(parsed_datapoints) #junta os dataframes dos anos
      separated_dfs:list[pd.DataFrame] = self.__separate_df_by_dataname(appended_df)
      time_series_years:list[int] = [int(x.data_year) for x in list_of_datapoints]

      collection_list:list[ProcessedDataCollection] = []
      for df, data_name in zip(separated_dfs,self.DATA_NAME_FOR_EACH_COL.values()):
         print(df.info())
         collection_list.append(
            ProcessedDataCollection(
                  category=self.DATA_CATEGORY,
                  dtype=self.DTYPE,
                  data_name=data_name,
                  time_series_years=time_series_years,
                  df=df
            )
      )
      return collection_list

      
   
