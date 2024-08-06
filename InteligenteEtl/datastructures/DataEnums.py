from enum import Enum

"""
Enums gerais que identificar tipos de dados ou tipos de arquivos
"""



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

   @classmethod
   def from_string(cls,dtype_string:str):
        """
        Retorna um objeto do ENUM a partir de uma string de tipo de dado
        """
        for member in DataTypes:
            if member.value == dtype_string:
                return member
        raise ValueError(f"{dtype_string} não é uma string válida para essa enum")

