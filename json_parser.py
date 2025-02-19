import pandas as pd
import json

def json_to_dataframe(json_file_path):


    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:  
            json_data = json.load(f)

        if isinstance(json_data, list):  
            df = pd.DataFrame(json_data)
        elif isinstance(json_data, dict):  
            df = pd.DataFrame([json_data])  
        else:
            print("JSON data is neither a list nor a dictionary.")
            return None

        return df

    except FileNotFoundError:
        print(f"Error: File not found at {json_file_path}")
        return None
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in {json_file_path}")
        return None
    except Exception as e:  
        print(f"An error occurred: {e}")
        return None




file_path = "opportunities_all.json"  
df = json_to_dataframe(file_path)



if df is not None:
    print(df.T)
    
    
    
    
else:
    print("Failed to load JSON into DataFrame.")





