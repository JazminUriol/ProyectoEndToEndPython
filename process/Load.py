import pandas as pd 
from utils import utilitarios as u

class Load():
    def __init__(self) -> None:
        self.process = 'Load Process'
        
    def load_to_cloud_storage(self, df, bucketName, fileName):
        try:
            client = u.get_cliente_cloud_storage()             
            bucket = client.get_bucket(bucketName)
            bucket.get_blob(fileName)
            blob = bucket.blob(fileName).upload_from_string(df.to_csv(index=False), 'text/csv')
        except Exception as e:
            print(f"Data Load error: {e}")
    
    def load_to_mysql(self,df,database_name, table_name):
        try:
            engine = u.get_mysql_client(database_name)
            df.to_sql(tbl_name, engine, index=False, if_exists='replace')
        except Exception as e:
            print(f"Data Load error: {e}")
    
