import json
import pandas as pd
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine
import re
import os 
import sys


class CustomExceptionWithVariable(Exception):
    def __init__(self, message, variable):
        super().__init__(message)
        self.variable = variable
    def __str__(self):
        return f"{super().__str__()} (Variable: {self.variable})"


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


def remove_stop_words(text, stop_words):
    if isinstance(text, str):  
        text = text.lower()  
        word = re.findall(r'\b\w+\b', text) 
        filtered = [w for w in word if w not in stop_words]  
        return " ".join(filtered)
    else:
        return ""

def keep_key_words(text, keywords):
    if isinstance(text, str):  
        text = text.lower()  
        word = re.findall(r'\b\w+\b', text) 
        filter_non_white_listed = [w for w in word if w in keywords]  
        return " ".join(filter_non_white_listed)
    else:
        return ""


def flatten_json_to_dataframe(table_name):
    json_file_path = (f"{table_name}.json")
    with open(json_file_path, 'r') as f:
        json_data = json.load(f)
    # print(json_data)
    json_data = clean_json_keys(json_data)
    # print(json_data)
    
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
    
    swedish_stop_words = {'varför', 'än', 'dig', 'våra', 'om', 'nu', 'vad', 'icke', 'ju', 'hennes', 'min', 'vårt', 'bli', 'hade', 'vilket', 'och', 'vart', 'sig', 'ha', 'oss', 'sedan', 'blir', 'sådan', 'dina', 'denna', 'på', 'mellan', 'ett', 'av', 'samma', 'ut', 'dess', 'men', 'han', 'allt', 'för', 'varit', 'din', 'ni', 'jag', 'med', 'i', 'detta', 'att', 'mycket', 'själv', 'varje', 'skulle', 'till', 'de', 'er', 'något', 'den', 'henne', 'över', 'hon', 'vår', 'sitta', 'hans', 'någon', 'vara', 'vem', 'deras', 'era', 'det', 'sådana', 'när', 'sin', 'blivit', 'mot', 'kunde', 'mig', 'så', 'då', 'en', 'var', 'kan', 'vi', 'vilka', 'inom', 'du', 'sådant', 'honom', 'är', 'alla', 'vilkas', 'från', 'dem', 'här', 'utan', 'åt', 'vilken', 'vid', 'upp', 'hur', 'ej', 'ingen', 'blev', 'sina', 'mina', 'under', 'ditt', 'eller', 'där', 'vars', 'dessa', 'efter', 'ert', 'några', 'man', 'har', 'inte', 'som', 'mitt'}
    english_stop_words = {'ma', 'of', 'did', 'itself', 'there', "it's", "he'd", 'with', "needn't", 'aren', 'his', 'himself', 'as', 'more', "i've", 'such', "should've", 'on', 'her', "you'll", 'in', 'here', 'my', 'when', 'again', 'ain', 'you', "we've", 'out', 'by', 'if', 'from', "won't", 'y', 'its', 'them', "mightn't", 'against', 'before', 'for', "it'd", 'over', 'now', 'yourself', 've', 'their', 'having', 'were', 'where', 'that', 'me', 'hadn', 'below', 'hers', 'themselves', 'has', 'then', 'whom', 'be', 'after', 'don', "they'll", 'they', 'are', 'about', 'does', 'to', 'yours', 'our', 'll', 'isn', 's', 'and', 'ourselves', 'into', 'than', 'your', 'he', 'i', 're', "we'd", 'at', 'she', 'do', 'some', "didn't", 'have', 'is', 'but', 'between', 'the', 'wouldn', 'will', "we'll", 'through', "she'd", "hadn't", "they've", 'doesn', "he's", 'nor', 'just', 'mightn', 'hasn', "you've", 'up', "wasn't", 'most', 'not', 'ours', 'until', "shan't", 'yourselves', 'shan', "she'll", "wouldn't", 'other', 'doing', 'down', "shouldn't", 'few', "couldn't", "you'd", 'so', 'couldn', 'can', 'had', 'own', 'wasn', 'once', 'weren', 'd', 'each', "hasn't", 'those', 'above', "i'd", 'theirs', "doesn't", 'mustn', 'these', 'should', "don't", "she's", "you're", 'was', 'who', "aren't", 'being', 'or', 'all', 'while', 'herself', "that'll", 'both', 'any', 'been', 'haven', 't', "i'm", 'needn', "they're", 'this', "mustn't", 'm', "it'll", 'how', 'why', 'o', 'an', 'won', 'a', "we're", 'myself', 'am', 'didn', "i'll", 'under', 'off', 'too', 'it', 'very', 'which', 'only', "haven't", 'same', "he'll", 'no', 'further', 'because', 'shouldn', 'during', 'him', "they'd", 'we', 'what', "weren't", "isn't"}
    custom_stop_words = set(["vissa", "andra", "ord", "som", "inte", "finns", "i", "nltk"])  
    
    svenska_keywords = {"Dataanalys", "Data engineering", "Business", "intelligence","BI", "affär", "logik", "affärslogik", "Maskininlärning", "ML", "ETL", "Extrahera","Transformera","Ladda", "Data", "warehous", "varuhus", "Datavisualisering", "visual", "KPI-dashboards", "Kravanalys", "Tidsserieanalys", "Tidsserie", "Prestandarapportering", "Metadatahantering", "meta", "Logg", "dataanalys", "Lineagerapportering", "Prestanda", "övervakning", "Automatisering", "Automat", "Schemaläggning", "Schema", "orkest", "SQL", "Python", "SAS", "vault", "Data Vault-modellering", "Kimball", "modellering", "modell", "Agil", "Scrum", "CI", "CD", "Continuous", "Integration", "Deployment", "Versions", "kontroll", "Problemlösning", "Kommunikation", "Molnbaserad databehandling (GCP, Vertex AI)", "Moln", "behandling" , "GCP", "Vertex", "AI", "Data Lake", "lakehouse", "Gedigen", "erfarenhet", "Expert", "kunskaper", "SVM", "Vector" , "Machine", "Random", "Forest", "Deep","Neural","Networks", "Keras", "XGBoost", "Tree", "Regressor", "One-hot", "encod", "Dataanalytiker", "analy", "Verksamhet", "Databasutvecklare", "Databas", "utvecklare", "BI", "Dataingenjör", "ETL-utvecklare", "ETL", "ML-engineer", "ML", "engineer", "Systemutvecklare", "System", "System", "Statistiker", "Airflow", "Kubeflow", "Vertex", "AI", "Power BI", "Qlik", "SAS DI", "sas", "integration", "SSIS", "MSSQL", "SQL Server", "sql", "Oracle", "PL/SQL", "BigQuery", "Hadoop", "Visual Studio Code", "vsc", "visual studio", "MinIO", "DBT", "SSRS", "SSAS", "GitHub", "GitLab", "Bitbucket", "TFS Azure DevOps", "Databricks", "Spark SQL"}
    english_keywords = {"analysis", "Data", "engineering", "Business", "intelligence" , "BI", "Machine", "learning", "ML", "ETL", "Extract","Transform","Load", "warehous", "visualization", "KPI", "dashboards", "Requirements ", "Time series", "series", "Performance" , "reporting", "Meta", "management", "Log", "Lineage", "Performance", "monitoring", "Automation", "Scheduling", "SQL", "Python", "SAS", "inmon", "normal", "dimension", "scd", "slowly", "changing", "test", "Vault","modeling", "Kimball", "Agile", "Scrum", "CI", "CD", "Continuous", "Integration", "Deployment", "Version", "Problem", "Communication", "Cloud", "GCP", "Vertex", "AI", "Data Lake", "experienced", "expert", "deep", "SVM","Vector", "neighbor", "random","Forest", "Deep","Neural", "Keras", "XGBoost","Data Engineer", "Data Analyst", "Data Scientist", "Engine", "Analy", "Scien", "analyst", "Developer", "ETL Developer", "ML Engineer", "Statistician", "Business Intelligence Developer", "Business", "Intelligence", "Developer", "Airflow", "Kubeflow", "Vertex AI", "Power BI", "Qlik", "SAS DI", "SSIS", "MSSQL", "SQL Server", "Oracle", "PL/SQL", "sql", "GCP", "BigQuery", "Hadoop", "Visual Studio Code", "visual studio", "vsc", "MinIO", "DBT", "SSRS", "SSAS", "GitHub", "GitLab", "git", "Bitbucket", "TFS Azure DevOps", "tfs", "azure", "devops", "Databricks", "Spark"}
    custom_keywords = set(["montecarlo"])
    
    stop_words = swedish_stop_words.union(english_stop_words, custom_stop_words)
    keywords = svenska_keywords.union(english_keywords, custom_keywords)
    
    print(flattened_df['description'])
    
    try:
        flattened_df['description_filtered_blacklist'] = flattened_df['description'].apply(lambda x: remove_stop_words(x, stop_words))
    except:
        print("No description datafram value")
    
    try:
        flattened_df['description_filtered_whitelist'] = flattened_df['description'].apply(lambda x: keep_key_words(x, keywords))
    except:
        print("No description datafram value")
    
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
        # local_cursor.close()
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
            print(f"attemt: DROP TABLE {table_name_with_timestamp}")
            conn.commit()
        if cursor:
            cursor.close()
        if conn:
            conn.close()



# if __name__ == "__main__":
    # main("bronze_opportunities_2024", "opportunityid")

if __name__ == "__main__":
    import sys
    main(sys.argv[1], sys.argv[2])


# # print(clean_key("(Do Not Modify) Opportunity"))

