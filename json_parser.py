import json
import pandas as pd
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine
import sqlalchemy


class CustomExceptionWithVariable(Exception):
    def __init__(self, message, variable):
        super().__init__(message)
        self.variable = variable
    def __str__(self):
        return f"{super().__str__()} (Variable: {self.variable})"

# def json_records_to_dataframe(file_path):
#     try:
#         with open(file_path, 'r') as f:
#             data = json.load(f)
        
#         if isinstance(data, list):  # Array of objects
#             df = pd.DataFrame(data)
#         elif isinstance(data, dict):  # Single object
#             df = pd.DataFrame([data])
#         else:
            
#             raise CustomExceptionWithVariable("custom exception", data[:100])
        
#         return df
#     except ValueError as e:
#         if "ValueError: If using all scalar values, you must pass an index" in str(e):
#             return pd.DataFrame()
#     except FileNotFoundError:
#         print(f"File not found: {file_path}")
#         return pd.DataFrame()
#     except json.JSONDecodeError:
#         print(f"Invalid JSON: {file_path}")
#         return pd.DataFrame()


# def dataframe_to_sql_dynamic_dict_handling(table_name, if_exists='replace', index=False):
#     df = json_records_to_dataframe(f"{table_name}.json")
#     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#     table_name_with_timestamp = f"{table_name}_{timestamp}"
#     engine = create_engine('postgresql+psycopg2://oscar:@localhost:5432/postgres')
    
#     print(df.head())
#     print(df.T.head())
    
#     try:
#         dict_columns = []
#         for col in df.columns:
#             if df[col].apply(lambda x: isinstance(x, dict)).any():
#                 dict_columns.append(col)
        
#         if dict_columns:
#             for col in dict_columns:
#                 try:
#                     df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else None)
#                 except TypeError:
#                     print(f"Warning: Could not convert all values in column '{col}' to JSON strings. Proceeding without conversion.")
#                     dict_columns.remove(col) #remove from list so we don't try to add a dtype.
            
#             dtypes = {col: sqlalchemy.types.JSON for col in dict_columns}
#             df.to_sql(table_name_with_timestamp, engine, if_exists=if_exists, index=index, dtype=dtypes, schema='avega_dwh')
#         else:
#             df.to_sql(table_name_with_timestamp, engine, if_exists=if_exists, index=index, schema='avega_dwh')
    
#     except Exception as e:
#         print(f"Error loading DataFrame to SQL: {e}")
    
#     return table_name_with_timestamp



def flatten_json_to_dataframe(table_name):
    json_file_path = (f"{table_name}.json")
    with open(json_file_path, 'r') as f:
        json_data = json.load(f)
    
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
                    else: # Handle cases where list items are not dictionaries (e.g., simple values)
                        items.append((new_key + sep + str(index), list_item))
            else:
                items.append((new_key, v))
        return dict(items)
    
    if isinstance(json_data, list): # Handle case where root is a list of dicts
        flattened_list = [flatten_json(item) for item in json_data]
        flattened_df = pd.DataFrame(flattened_list)
    elif isinstance(json_data, dict): # Handle case where root is a dict
        if 'value' in json_data and isinstance(json_data['value'], list): # Specific handling based on example
            flattened_df = pd.DataFrame(json_data['value'])
        else: # General flattening for dict
            flattened_data = flatten_json(json_data)
            flattened_df = pd.DataFrame([flattened_data]) # List of dict for DataFrame constructor
    else:
        return pd.DataFrame() # Handle cases where json_data is not list or dict
    
    return flattened_df



def pandas_dataframe_to_sql(table_name):
    flattened_df = flatten_json_to_dataframe(table_name)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    table_name_with_timestamp = f"{table_name}_{timestamp}"
    
    engine = create_engine('postgresql+psycopg2://oscar:@localhost:5432/postgres')
    
    flattened_df.to_sql(table_name_with_timestamp, engine, if_exists='replace', index=False, method='multi', schema='avega_dwh')
    # conn.close()
    
    return table_name_with_timestamp



def get_column_names(table_name,  cursor):
    
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
    columns = [f'"{row[0]}"' for row in cursor.fetchall()]
    
    return columns



# def table_exists(conn, table_name, schema='avega_dwh'):
#     try:
#         cursor = conn.cursor()
#         cursor.execute(
#             "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s)",
#             (schema, table_name),
#         )
#         result = cursor.fetchone()[0]
#         cursor.close()
#         return result
#     except psycopg2.Error as e:
#         print(f"Error checking table existence: {e}")
#         return False
    

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
    main("bronze_applications", "application_id")



