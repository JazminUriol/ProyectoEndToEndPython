from process.Extract import Extract
from process.Transform import Transform
from process.Load import Load
import pandas as pd
import sys

extract_object = Extract()
transform_object = Transform()
load_object = Load()

bucketName = sys.argv[1]
folder_name_sink = sys.argv[2] 

enunciado01 = {"df1": "landing/products", "df2": "landing/order_items", "df3":"landing/orders", "df4": "landing/categories", "df5" : "landing/departments"}
enunciado02 = {"df1": "landing/orders", "df2": "landing/customers", "df3": "landing/order_items"}


def extract(extract_object, bucketName, file_name):
    df = extract_object.read_cloud_storage(bucketName, file_name)
    return df

def transform01(transform_object, bucketName):
    df1 = extract(extract_object, bucketName, enunciado01['df1'])
    df2 = extract(extract_object, bucketName, enunciado01['df2'])
    df3 = extract(extract_object, bucketName, enunciado01['df3'])
    df4 = extract(extract_object, bucketName, enunciado01['df4'])
    df5 = extract(extract_object, bucketName, enunciado01['df5'])
    
    df = transform_object.enunciado1(df1, df2, df3, df4, df5)
    return df


def transform02(transform_object, bucketName):
    df1 = extract(extract_object, bucketName, enunciado02['df1'])
    df2 = extract(extract_object, bucketName, enunciado02['df2'])
    df3 = extract(extract_object, bucketName, enunciado02['df3'])
    df = transform_object.enunciado2(df1,df2,df3)
    return df

def load(load_object, df_enunciado, bucketName, folder_name_sink, file_name_sink):
    folder = folder_name_sink+ '/' + file_name_sink
    load_object.load_to_cloud_storage(df_enunciado, bucketName,folder)

#los 5 productos más vendidos del último año
df_enunciado1 = transform01(transform_object, bucketName)
load(load_object, df_enunciado1,bucketName,  folder_name_sink, "enunciado01")

#reporte de los clientes con pedidos pendientes
df_enunciado2 = transform02(transform_object, bucketName)
load(load_object, df_enunciado2,bucketName,  folder_name_sink, "enunciado02")

