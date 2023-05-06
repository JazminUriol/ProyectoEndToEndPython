# Flujo Completo de Datos
## _ETL con Python, Google Cloud y Airflow_

### 1. Ingesta
Importaremos nuestra base de datos Retail_DB que se encuentra en Mysql hacia una capa de almacenamiento llamada landing en Cloud Storage. 
### 2. Transformación
Tengo dos enunciados basados en la información cargada en la capa landing:
- Los 5 productos más vendidos en el último año.
- Reporte de los clientes con ordenes en estado pendiente.

Finalmente ambos resultados fueron cargados  a la capa de almacenamiento Gold en Cloud Storage.
### 3. Airflow
Se crearon 5 tareas de ingesta que se ejecutaran en paralelo y luego pasaran a la tarea de Transformación.
