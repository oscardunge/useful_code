import json
import pandas as pd

def json_records_to_dataframe(json_records):
    try:
        df = pd.DataFrame(json_records)
        return df.T
    except ValueError as e:
        if "ValueError: If using all scalar values, you must pass an index" in str(e):
            return pd.DataFrame()
        else:
            raise e
    except Exception as e:
        raise e

def read_json_file_and_transpose(file_path):
        with open(file_path, 'r') as f:  
            json_data = json.load(f)  
        if isinstance(json_data, dict) and "value" in json_data: 
            inner_data = json_data["value"]  
            if isinstance(inner_data, list):  
                return json_records_to_dataframe(inner_data)  
            elif isinstance(inner_data, dict):  
                return json_records_to_dataframe([inner_data])  
            else:  
                return pd.DataFrame() 
        else:  
            return pd.DataFrame() 



df_from_file = read_json_file_and_transpose("data_all.json")

if not df_from_file.empty:
    print(df_from_file.T)

df = df_from_file.T
df.to_excel("exceldatafrom.xlsx")