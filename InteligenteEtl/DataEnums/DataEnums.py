from enum import Enum

class BaseFileType(Enum): #enum para os tipos de arquivos que são comumente encontrados em bases oficiais do governo
   EXCEL = "xlsx"
   ODS = "ods"
   TXT = "txt"
   CSV = "csv"

class DataTypes(Enum): #Enum para os tipos de dados encontrados nas bases 
   INT = "int"
   FLOAT = "float"
   STRING = "str"
   BOOL = "bool"
   UNKNOWN = "str"
   NULL = "NULL"


