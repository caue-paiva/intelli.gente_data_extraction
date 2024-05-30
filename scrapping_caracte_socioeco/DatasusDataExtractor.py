import pandas as pd
import re , os
from DataCollection import ProcessedDataCollection


class DatasusDataExtractor():

   def __init__(self) -> None:
      pass

   def remove_city_name_column(self,df:pd.DataFrame, city_identifier_col:str)->pd.DataFrame:
      remove_non_ints = lambda x : re.sub(r'[^0-9]', '', x)
      df[city_identifier_col]= df[city_identifier_col].apply(remove_non_ints)

      return df

   def transform_df(self,df:pd.DataFrame, data_identifier:str,city_identifier_col:str,data_value_col:str)->pd.DataFrame:
      df["dado_identificador"] = data_identifier #coloca coluna do nome do dado

      df = self.remove_city_name_column(df,city_identifier_col) #deixa so os códigos na coluna do município (tira os nomes)
      df = df.rename({data_value_col:"valor"}, axis="columns") #renomeia a coluna com os valores dos dados
      
      try:
         df["valor"] = df["valor"].astype("float64")
      except ValueError as e:
         if "could not convert string to float" in str(e):
            try:
               df["valor"] = df["valor"].replace(",",".",regex=True)
               df["valor"] = df["valor"].astype("float64")
            except:
               raise ValueError("Não foi possível converter valores do df de str pra float")
      df["tipo_dado"] = "float"

      return df

   def get_data_collection(
      self,
      df:pd.DataFrame,
      data_category:str,
      time_series_years:list[int], #talvez esse argumento possa ser inferido com o .value_counts do pandas
      data_identifier:str,
      city_identifier_col:str,
      data_value_col:str 
   )->ProcessedDataCollection:
      
      df = self.transform_df(df,data_identifier,city_identifier_col,data_value_col)
      return ProcessedDataCollection(data_category,data_identifier,time_series_years,df)

extractor = DatasusDataExtractor()

df = pd.read_csv(os.path.join("webscrapping","tempfiles","datasus_alfbr.csv"))
collection:ProcessedDataCollection = extractor.get_data_collection(df,"educa",[1991,2000,2010],"TAXA ANALFABETISMO","Município","Taxa_de_analfabetismo")

#df = transform_df(df,"TAXA ANALFABETISMO","Município","Taxa_de_analfabetismo")

print(collection.df.head(5))
print(collection.df.info())