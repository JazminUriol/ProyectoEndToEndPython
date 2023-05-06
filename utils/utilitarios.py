import os
import yaml
from io import StringIO
from google.cloud.storage import Client
import sqlalchemy as db

with open('/user/app/ProyectoEndToEndPython/Proyecto/config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)

def get_cliente_cloud_storage():
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config["cloud_storage"]["credentials"]
    client = Client()
    
    return client


def get_mysql_client(databaseName):
    
    cadena_conexion = (f"mysql://{config['mysql']['user']}:{config['mysql']['pass']}@{config['mysql']['host']}:{config['mysql']['port']}/{databaseName}")
    engine = db.create_engine(cadena_conexion)
    
    return engine
