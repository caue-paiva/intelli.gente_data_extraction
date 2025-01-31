import json,os,sys
from pathlib import Path
from extractionhandler import ExtractorClassesHandler

def main(argv:list[str])->None:
   """
   Executa o módulo de extração de dados, extraindo e processando dados de acordo com as fontes especficadas em
   sources_to_extract.json, com os dados processados sendo salvos no Data Warehouse do projeto.
   
   Args:
      argv (list[str]): vetor de argumentos da linha de comando, com o primeiro argumento adicional sendo
      o path onde os logs de extração serão armazenados
   
   Return:
      (None)
   """
   if len(argv) >= 2: #primeiro argumento de linha de comando é o própio arquivo
      logs_path = Path(argv[1]).with_suffix(".log")
   else:
      logs_path = Path(f"extraction_logs.log")


   sources: dict 
   with open(os.path.join(os.getcwd(),"sources_to_extract.json"),"rb") as f:
      sources:dict =  json.load(f)

   handler = ExtractorClassesHandler()
   logs = handler.run_requested_extractions(sources)

   try:
      with open(logs_path,"a") as f:
         for log in logs:
            f.write(str(log))
            f.write("\n")    
   except Exception as e:
      print("Falha ao Salvar o arquivo de logs: ",e)

if __name__ == "__main__":
   main(sys.argv)