import pandas as pd
from dataclasses import dataclass

@dataclass
class YearDataPoint():
   """
   Classe para presentar um dataframe com dados extraidos e seu ano de referência
   Os dados nessa classe ainda estão brutos e devem ser processados para serem transformados numa classe processed data collection
   Essa classe foi criada para representar um conjunto de dados bem comum nas classes de WebScrapping
   
   """

   df:pd.DataFrame
   data_year:str

   @classmethod
   def from_tuple(cls,data:tuple[pd.DataFrame,str] | tuple[str,pd.DataFrame])-> 'YearDataPoint':
      """
      Método estático para criar um objeto a partir de uma tupla de Dataframes e str (ano do dado)
      """
      if isinstance(data[0], pd.DataFrame):
            df, data_year = data
      else:
            data_year, df = data
      return cls(df=df, data_year=data_year)