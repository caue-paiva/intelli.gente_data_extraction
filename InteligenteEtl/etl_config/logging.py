from dataclasses import dataclass
from datetime import datetime,timedelta

class ClassExtractionLog:
   """
   Dataclass que contém informação de uma tentativa de extração por webscrapping ou API de uma classe.
   Usada no sistema de logging
   """
   class_name:str
   data_points_logs:list['DataPointExtractionLog']
   start_date:datetime
   finish_date: datetime
   extraction_time: timedelta
   extra_info:str 

   def __init__(self, class_name: str, data_points_logs: list['DataPointExtractionLog'], start_date:datetime ,
                 finish_date: datetime, extraction_time: timedelta, extra_info: str = ""):
        self.class_name = class_name
        self.data_points_logs = data_points_logs
        self.start_date = start_date
        self.finish_date = finish_date
        self.extraction_time = extraction_time
        self.extra_info = extra_info
   
   @classmethod
   def error_log(cls, error_message: str) -> 'ClassExtractionLog':
        return cls(
            class_name="Error",
            data_points_logs=[],  
            start_date=datetime.min, 
            finish_date=datetime.min, 
            extraction_time=timedelta(0),  
            extra_info=error_message
        )
   

   def __str__(self):
      data_points_str = "\n-> ".join([f"{data_point}" for data_point in self.data_points_logs])
      data_points_str = "\n-> "  + data_points_str  + "\n"
      return (
           f"\nClass Name: {self.class_name}\n"
           f"Finish Date: {self.finish_date.strftime('%Y-%m-%d %H:%M:%S')}\n"
           f"Extraction Time: {self.extraction_time}\n"
           f"Stdout: {self.extra_info}\n"
           f"Data Points Logs:\n{data_points_str}"
      )


class DataPointExtractionLog:

   time_series_years:list[int]
   total_df_lines:int
   data_point:str

   def __init__(self, data_point: str, time_series_years: list[int], total_df_lines: int):
        self.data_point = data_point
        self.time_series_years = time_series_years
        self.total_df_lines = total_df_lines

   def __str__(self):
      years_str = ", ".join(map(str, self.time_series_years))
      return (
           f"Data Point: {self.data_point}\n" +
           f"     Time Series Years: {years_str}\n" +
           f"     Total DataFrame Lines: {self.total_df_lines}"
      )
   