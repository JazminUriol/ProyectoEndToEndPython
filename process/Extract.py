from sqlalchemy import text
import pandas as pd 
from io import StringIO

from utils import utilitarios as u

class Extract():
    
    def read_cloud_storage(self,bucketName, fileName):
        
        client = u.get_cliente_cloud_storage()     
        bucket = client.get_bucket(bucketName)
        blob = bucket.get_blob(fileName)
        downloaded_file = blob.download_as_text(encoding="utf-8")
        df = pd.read_csv(StringIO(downloaded_file))
        return df
    
    def read_mysql(self, database_name, table_name):
    
        engine = u.get_mysql_client(database_name)
        conn = engine.connect()
        df = pd.read_sql_query(text(f'SELECT * FROM {table_name}'), con=conn)
        return df
    