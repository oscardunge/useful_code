import json
import pandas as pd
# import sqlite3
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine


#to config?


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


# def pandas_dataframe_to_sql():
#     df_from_file = read_json_file_and_transpose(f"{table_name}.json")
    
#     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
#     table_name_with_timestamp = f"bronze_{table_name}_{timestamp}"
    
    
#     conn = sqlite3.connect('avega_business_process_db.db')
    
#     df_from_file.T.to_sql(f"{table_name_with_timestamp}", conn, if_exists='replace', index=False)
    
#     conn.close()
    
#     return table_name_with_timestamp

def pandas_dataframe_to_sql(table_name):
    df_from_file = read_json_file_and_transpose(f"{table_name}.json")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    table_name_with_timestamp = f"{table_name}_{timestamp}"
    
    conn = psycopg2.connect(
        dbname='postgres',
        user='oscar',
        password='',
        host='localhost',
        port='5432'
    )
    
    engine = create_engine('postgresql+psycopg2://oscar:@localhost:5432/postgres')
    
    df_from_file.T.to_sql(table_name_with_timestamp, engine, if_exists='replace', index=False, method='multi', schema="avega_dwh")
    conn.close()
    
    return table_name_with_timestamp

# pandas_dataframe_to_sql(table_name)


def get_column_names(table_name, conn):
    cursor = conn.cursor()
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
    columns = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return columns


# def main():
#     table_name_with_timestamp = pandas_dataframe_to_sql()
#     target_table_name = f"bronze_{table_name}"  # The table to merge into
    
#     conn = sqlite3.connect('avega_business_process_db.db')
    
#     cursor = conn.cursor()
    
#     # Construct the MERGE statement (adapt to your specific database syntax)
#     merge_statement = f"""
#     MERGE INTO {target_table_name} AS target
#     USING {table_name_with_timestamp} AS source
#     ON target.opportunityid = source.opportunityid;
#     """
    
#     print(merge_statement)
#     cursor.execute(merge_statement)
#     conn.commit()
    
#     conn.close()

# main()


def main(table_name):
    table_name_with_timestamp = pandas_dataframe_to_sql(table_name)  # Replace with your actual table name
    target_table_name = 'bronze_your_table'  # The table to merge into
    
    conn = psycopg2.connect(
        dbname='your_database_name',
        user='your_username',
        password='your_password',
        host='localhost',
        port='5432'
    )
    
    columns = get_column_names(target_table_name, conn)
    columns_str = ', '.join(columns)
    update_str = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns])
    
    # Construct the INSERT ... ON CONFLICT statement
    merge_statement = f"""
    INSERT INTO {target_table_name} ({columns_str})
    SELECT {columns_str}
    FROM {table_name_with_timestamp}
    ON CONFLICT (opportunityid) DO UPDATE
    SET {update_str};
    """
    
    print(merge_statement)
    cursor = conn.cursor()
    cursor.execute(merge_statement)
    conn.commit()
    
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()




