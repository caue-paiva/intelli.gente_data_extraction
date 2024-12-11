import json,os
from extractors_classes_handler import ExtractorClassesHandler

sources: dict 
with open(os.path.join(os.getcwd(),"sources_to_extract.json"),"rb") as f:
   sources:dict =  json.load(f)

handler = ExtractorClassesHandler()
logs = handler.run_requested_extractions(sources)
print(logs[0])

with open("logs.txt","w") as f:
   f.write(str(logs[0]))
