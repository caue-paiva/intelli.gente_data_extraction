import pandas as pd
from dataclasses import dataclass

@dataclass
class YearDataPoint():
   """
   Classe para presentar um dataframe com dados extraidos e seu ano de referência.
   Os dados nessa classe ainda estão brutos e devem ser processados para serem transformados numa classe processed data collection
   Essa classe foi criada para representar um conjunto de dados bem comum nas classes de WebScrapping
   
   """

   df:pd.DataFrame
   data_year:int

   #métodos de classe para interagir com a classe como um todo e não com uma instancia do objeto

   @classmethod
   def from_tuple(cls,data:tuple[pd.DataFrame,int] | tuple[int,pd.DataFrame])-> 'YearDataPoint':
      """
      Método estático para criar um objeto a partir de uma tupla de Dataframes e str (ano do dado)
      """
      if isinstance(data[0], pd.DataFrame):
            df, data_year = data
      else:
            data_year, df = data
      return cls(df=df, data_year=data_year)
   

   @classmethod
   def from_lists(cls,df_list:list[pd.DataFrame],years_list:list[int])->list['YearDataPoint']:
      if not isinstance(df_list,list) or not isinstance(years_list,list):
          raise TypeError("Argumento passado não é uma lista")
      
      return [YearDataPoint(x,y) for x,y in zip(df_list,years_list)]
   
   @classmethod
   def get_years_from_list(cls,data_point_list:list['YearDataPoint'])->list[int]:
      """
      Dado uma lista de objetos YearDataPoint, retorna uma lista de strings com os  campos anos desses objetos
      """
      return [x.data_year for x in data_point_list]
   
   @classmethod
   def get_dfs_from_list(cls,data_point_list:list['YearDataPoint'])->list[pd.DataFrame]:
      """
      Dado uma lista de objetos YearDataPoint, retorna uma lista de sdataframes com os campos df desses objetos
      """
      return [x.df for x in data_point_list]