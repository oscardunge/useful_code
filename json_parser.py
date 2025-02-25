import json
import pandas as pd
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine



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

def read_json_file_with_json_func(file_path):
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




def pandas_dataframe_to_sql(table_name, conn, cursor):
    df_from_file = read_json_file_with_json_func(f"{table_name}.json")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    table_name_with_timestamp = f"{table_name}_{timestamp}"
    
    engine = create_engine('postgresql+psycopg2://oscar:@localhost:5432/postgres')
    
    df_from_file.T.to_sql(table_name_with_timestamp, engine, if_exists='replace', index=False, method='multi', schema='avega_dwh')
    # conn.close()
    
    return table_name_with_timestamp

# pandas_dataframe_to_sql('bronze_opportunities_all')


def get_column_names(table_name, conn, cursor):
    
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
    columns = [f'"{row[0]}"' for row in cursor.fetchall()]
    
    return columns




def main(table_name, id):
    error_occurred = False
    
    conn = psycopg2.connect(
    dbname='postgres',
    user='oscar',
    password='',
    host='localhost',
    port='5432',
    options="-c search_path=avega_dwh"
    )
    
    cursor = conn.cursor()
    try:
        table_name_with_timestamp = pandas_dataframe_to_sql(table_name,conn, cursor)  
        
        print(table_name_with_timestamp)
        columns = get_column_names(table_name, conn, cursor)
        columns_str = ', '.join(columns)
        
        insert_statement = f"""
        insert into {table_name} ({columns_str})
        select {columns_str} 
        from {table_name_with_timestamp} 
        on conflict ({id}) do nothing 
        ;
        """
        print(insert_statement)
        
        
        try:
            cursor.execute(insert_statement)
        
        except Exception as e:
            print(f"Error: {e}")
            constraint_statement = f"""
            alter table {table_name}
            add constraint {id} unique ({id});"""
            
            cursor.execute(constraint_statement)
            cursor.execute(insert_statement)
        
        finally:
            conn.commit()
            error_occurred = False
            
    except psycopg2.Error as e:
        print(f"Error: {e}")
        error_occurred = True
    finally:
        if not error_occurred:
            cursor.execute(f"DROP TABLE {table_name_with_timestamp}")
            conn.commit()
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    main("bronze_xxxxxies_all", "xxxxxyid")

