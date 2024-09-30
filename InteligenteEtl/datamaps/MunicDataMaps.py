import json, os

__SCRIPT_DIR:str = os.path.dirname(os.path.abspath(__file__))

def get_data_informations():
    data_infomations_path:str = os.path.join(__SCRIPT_DIR,"munic_data_information.json")
    try:
        with open(data_infomations_path, 'r') as file:
            data_infomations = json.load(file)
    except:
        raise RuntimeError("Não foi possível abrir o arquivo de informação dos dados MUNIC")

    return data_infomations

def get_data_codes_per_year():
    data_codes_per_year_path:str = os.path.join(__SCRIPT_DIR,"munic_data_codes_per_year.json")
    try:
        with open(data_codes_per_year_path, 'r') as file:
            data_codes_per_year = json.load(file)
    except:
        raise RuntimeError("Não foi possível abrir o arquivo de códigos por ano dos dados MUNIC")
    return data_codes_per_year