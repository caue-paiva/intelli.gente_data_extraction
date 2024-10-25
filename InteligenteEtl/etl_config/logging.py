from dataclasses import dataclass
from datetime import datetime


@dataclass
class ExtractionLog:
   """
   Dataclass que contém informação de uma tentativa de extração de uma classe 
   """
   class_name:str
   indicators:list[str]
   num_of_indicators:int
   years:list[int]
   total_df_lines:int
   date: datetime
   extraction_time: datetime
   