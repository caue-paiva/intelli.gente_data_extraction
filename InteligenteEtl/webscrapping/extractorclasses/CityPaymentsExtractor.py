from datastructures import ProcessedDataCollection, YearDataPoint , DataTypes
from .AbstractDataExtractor import AbstractDataExtractor
from webscrapping.scrapperclasses import CityPaymentsScrapper
import pandas as pd

class CityPaymentsExtractor(AbstractDataExtractor):

   EXTRACTED_DATA_COL = "CAPAG" #nome da coluna dos dados que buscamos
   EXTRACTED_CITY_CODE_COLS = ("código município completo","cod.ibge") #2 possibilidades pra a coluna com os códigos dos municípios
   DATA_NAME = "Capacidade de pagamento dos municípios (CAPAG)"
   NULL_IDENTIFIER = ("n.d.","N.D.","n.e.","N.E.")
   DATA_CATEGORY = "Finanças Públicas"
   DTYPE = DataTypes.STRING

   def extract_processed_collection(self, scrapper_class_obj: CityPaymentsScrapper)->ProcessedDataCollection:
      list_of_datapoints:list[YearDataPoint] = scrapper_class_obj.extract_database()
      
      acess_and_parse_dfs = lambda x: YearDataPoint(self.__parse_columns(x.df),x.data_year) #aplica a função de parsing das colunas no df de cada objeto
      parsed_datapoints:list[YearDataPoint] = list(map(acess_and_parse_dfs,list_of_datapoints)) #da parsing na coluna de todos os dataframes
      appended_df:pd.DataFrame = self._concat_data_points(parsed_datapoints) #junta os dataframes de anos diferentes e add a coluna de ano
      
      final_df:pd.DataFrame = self.__add_missing_cols(appended_df)
      time_series_years:list[int] = YearDataPoint.get_years_from_list(list_of_datapoints)

      return ProcessedDataCollection(
            category=self.DATA_CATEGORY,
            dtype=self.DTYPE,
            data_name=self.DATA_NAME,
            time_series_years=time_series_years,
            df=final_df
      )
      
   def __parse_columns(self,df:pd.DataFrame)->pd.DataFrame:
      """
      Processa e padroniza nomes de colunas e removes colunas desnecessárias
      """
      df_cols:list[str] = df.columns
      parse_col_names = lambda x: x.lower().replace("_"," ") #coloca os nomes em lowercase e troca underline por espaço para padronizar
      df_cols = list(map(parse_col_names,df_cols))
      df.columns = df_cols #atualiza as colunas do df

      if self.EXTRACTED_CITY_CODE_COLS[0] in df_cols: #caso do primeiro nome pra coluna de cod de municipio
         df = df.rename({self.EXTRACTED_CITY_CODE_COLS[0]: self.CITY_CODE_COL},axis="columns") #muda nome do codigo do munic para o padrão
         df_cols.remove(self.EXTRACTED_CITY_CODE_COLS[0])
      elif self.EXTRACTED_CITY_CODE_COLS[1] in df_cols:
         df = df.rename({self.EXTRACTED_CITY_CODE_COLS[1]: self.CITY_CODE_COL},axis="columns") #muda nome do codigo do munic para o padrão
         df_cols.remove(self.EXTRACTED_CITY_CODE_COLS[1])
      else:
         raise RuntimeError("Não foi possível achar a coluna do código do município")
     
      #print(df_cols)
      cols_to_keep:list[str] = [self.CITY_CODE_COL  ,self.__get_capag_col(df_cols)]  #coluna do código da cidade e dos dados, serão mantidas   
      cols_to_drop:list[str] = [x for x in df_cols if x not in cols_to_keep]  #colunas para serem dropadas
      df = df.drop(cols_to_drop,axis="columns") #tira as colunas desnecessárias


      df = df.replace( #troca aas strings de NULL da base pelo valor none
         {
            self.NULL_IDENTIFIER[0]:None,self.NULL_IDENTIFIER[1]:None,
            self.NULL_IDENTIFIER[2]:None,self.NULL_IDENTIFIER[3]:None
         }
      )
      df = df.dropna(axis="index") #tira as linhas com NA em qualquer das colunas

      return self.__change_col_dtypes(df) #muda os tipos de dados das colunas do df

   def __get_capag_col(self,columns:list[str])->list[str]:
      """
      Acha a coluna que contêm os dados CAPAG finais e retorna o nome dela 
      """
      get_capag_cols = lambda x: self.EXTRACTED_DATA_COL.lower() in x.lower() #retorna todas as colunas com a string capag
      capag_cols:list[str] = list(filter(get_capag_cols,columns))

      if len(capag_cols) > 1: #mais de uma coluna com nome CAPAG
         filter_capag_cols = lambda x: len(x) == len(self.EXTRACTED_DATA_COL) or "classificação" in x #filtra as colunas com o nome capag mas que não são as certas
         #apenas colunas 'capag' ou 'capag oficial' ficam depois do filtro
         capag_cols = list(filter(filter_capag_cols,columns)) #filtra de novo as colunas

         if len(capag_cols) > 1 or len(capag_cols) == 0: #falha para achar uma única com nome CAPAG
            raise RuntimeError("falha ao achar coluna do dado CAPAG")
      return capag_cols[0]

   def __change_col_dtypes(self,df:pd.DataFrame)->pd.DataFrame:
      """
      Muda os tipos de dados das colunas já existentes e renomeia a coluna que contem os valores dos dados
      """
      df[self.CITY_CODE_COL] = df[self.CITY_CODE_COL].astype("int")
      
      index:int = next((i for i, s in enumerate(df.columns) if self.EXTRACTED_DATA_COL.lower() in s), -1) #acha o index da coluna de dados
      if index == -1:
         raise RuntimeError("falha ao achar o nome da coluna de dados do dataframe")
      columns:list[str] = list(df.columns)
      columns[index] = self.DATA_VALUE_COLUMN
      df.columns = columns #coloca o novo nome da coluna com os dados
      df[self.DATA_VALUE_COLUMN] = df[self.DATA_VALUE_COLUMN].astype("category")  #os dados são notas A,B,C. então elas podem ser otimizadas mudando elas para categorias
      return df
   
   def __add_missing_cols(self,df:pd.DataFrame)->pd.DataFrame:
      """
      Adiciona as colunas que estão faltando do dataframe (tipo de dado, nome do dado, ano)
      """
      df[self.DATA_IDENTIFIER_COLUMN] = self.EXTRACTED_DATA_COL #coluna de nome do dado
      df[self.DTYPE_COLUMN] = self.DTYPE.value #coluna de tipo de dado

      return df


  

      
   
