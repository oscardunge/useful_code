import json
import pandas as pd
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine
import re

def clean_key(key):
    # Remove parentheses and whitespace
    key = key.lower()
    return re.sub(r'[\s()]', '', key)

def clean_json_keys(data):
    if isinstance(data, dict):
        return {clean_key(k): clean_json_keys(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [clean_json_keys(item) for item in data]
    else:
        return data

class CustomExceptionWithVariable(Exception):
    def __init__(self, message, variable):
        super().__init__(message)
        self.variable = variable
    def __str__(self):
        return f"{super().__str__()} (Variable: {self.variable})"


def flatten_json_to_dataframe(table_name):
    json_file_path = (f"{table_name}.json")
    with open(json_file_path, 'r') as f:
        json_data = json.load(f)
    print(json_data)
    json_data = clean_json_keys(json_data)
    print(json_data)
    
    def flatten_json(json_obj, parent_key='', sep='_'):
        items = []
        for k, v in json_obj.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_json(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                for index, list_item in enumerate(v):
                    if isinstance(list_item, dict):
                        items.extend(flatten_json(list_item, new_key + sep + str(index), sep=sep).items())
                    else: 
                        items.append((new_key + sep + str(index), list_item))
            else:
                items.append((new_key, v))
        return dict(items)
    
    if isinstance(json_data, list): 
        flattened_list = [flatten_json(item) for item in json_data]
        flattened_df = pd.DataFrame(flattened_list)
    elif isinstance(json_data, dict): 
        if 'value' in json_data and isinstance(json_data['value'], list): 
            flattened_df = pd.DataFrame(json_data['value'])
        else: 
            flattened_data = flatten_json(json_data)
            flattened_df = pd.DataFrame([flattened_data]) 
    else:
        return pd.DataFrame() 
    
    return flattened_df



def pandas_dataframe_to_sql(table_name):
    flattened_df = flatten_json_to_dataframe(table_name)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    table_name_with_timestamp = f"{table_name}_{timestamp}"
    
    engine = create_engine('postgresql+psycopg2://oscar:@localhost:5432/postgres')
    
    flattened_df.to_sql(table_name_with_timestamp, engine, if_exists='replace', index=False, method='multi', schema='avega_dwh')
    
    
    return table_name_with_timestamp




def get_column_names(table_name,  cursor):
    
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
    columns = [f'"{row[0]}"' for row in cursor.fetchall()]
    
    return columns





# table_name_with_timestamp = flatten_json_to_dataframe("bronze_won_oppertunities")
# print(table_name_with_timestamp)

def create_with_constraints(table_name, id, conn_details):
    
    local_connection = psycopg2.connect(**conn_details)
    local_cursor = local_connection.cursor()
    
    try:
        table_name_with_timestamp = pandas_dataframe_to_sql(table_name)  
        
        print(table_name_with_timestamp)
        local_connection.commit()
        
        
        create_statement = f"""
            create table if not exists {table_name} as
            select * 
            from {table_name_with_timestamp} 
            limit 0;
            """
        print(create_statement)
            
        local_cursor.execute(create_statement)
        local_connection.commit()
    except Exception as e:
        print(f"Error___: {e}")
    
    try:    
        constraint_statement = f"""         
            alter table {table_name}
            add constraint {id}_{table_name} 
            unique ({id});"""
            
        print(constraint_statement)
        local_cursor = local_connection.cursor()
        local_cursor.execute(constraint_statement)
        local_cursor.connection.commit()
        local_cursor.close()
        return table_name_with_timestamp
    
    except Exception as e:
        print(f"Error___: {e}")
        return table_name_with_timestamp
    finally:
        local_cursor.close()


def main(table_name, id):
    id = clean_key(id)
    error_occurred = False
    
    conn_details = {
        "dbname":'postgres',
        "user":'oscar',
        "password":'',
        "host":'localhost',
        "port":'5432',
        "options":"-c search_path=avega_dwh"
    }
    
    conn = psycopg2.connect(**conn_details)
    cursor = conn.cursor()
    
    table_name_with_timestamp = create_with_constraints(table_name, id, conn_details)
    
    try:
        columns = get_column_names(table_name, cursor)
        columns_str = ', '.join(columns)
        
        insert_statement = f"""
        insert into {table_name} ({columns_str})
        select {columns_str} 
        from {table_name_with_timestamp} 
        on conflict ({id}) do nothing 
        ;
        """
        print(insert_statement)
        
        cursor.execute(insert_statement)
        
            
    except psycopg2.Error as e:
        print(f"Error__: {e} , will not drop {table_name_with_timestamp} or commit")
        error_occurred = True
    finally:
        if not error_occurred:
            cursor.execute(f"DROP TABLE {table_name_with_timestamp}")
            print(f"ATEMT: DROP TABLE {table_name_with_timestamp}")
            conn.commit()
        if cursor:
            cursor.close()
        if conn:
            conn.close()



if __name__ == "__main__":
    main("bronze_won_opportunities", "(Do Not Modify) Opportunity")



# print(clean_key("(Do Not Modify) Opportunity"))