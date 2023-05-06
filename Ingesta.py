from process.Extract import Extract
from process.Transform import Transform
from process.Load import Load
import sys

extract = Extract()
transform = Transform()
load_object = Load()

database_name = sys.argv[1]
file_name = sys.argv[2]
bucketName = sys.argv[3]
folder_name_sink = sys.argv[4]
file_name_sink = sys.argv[5]

def ingest(extract, database_name, file_name):
    df = extract.read_mysql(database_name, file_name)
    return df

def load(load_object, df, bucketName, folder_name_sink, file_name_sink):
    folder = folder_name_sink+ '/' + file_name_sink
    load_object.load_to_cloud_storage(df,bucketName ,folder)
    
df = ingest(extract, database_name, file_name) 

load(load_object, df, bucketName, folder_name_sink, file_name_sink) 