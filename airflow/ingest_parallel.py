from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago 

from datetime import datetime

args = {"owner": "Liz", "start_date":days_ago(1)}
dag = DAG(
    dag_id="ingest_parallel_dag",
    default_args=args,
    schedule_interval='@once', # * * * * * *
)

with dag:
    run_script_ingest_customers = BashOperator(
        task_id='run_script_ingest_customers',
        bash_command='python "/user/app/ProyectoEndToEndPython/Proyecto/Ingesta.py" "retail_db" "customers" "source-projects01" "landing" "customers"'
    )

    run_script_ingest_categories = BashOperator(
        task_id='run_script_ingest_categories',
        bash_command='python "/user/app/ProyectoEndToEndPython/Proyecto/Ingesta.py" "retail_db" "categories" "source-projects01" "landing" "categories"'
    )

    run_script_ingest_departments = BashOperator(
        task_id='run_script_ingest_departments',
        bash_command='python "/user/app/ProyectoEndToEndPython/Proyecto/Ingesta.py" "retail_db" "departments" "source-projects01" "landing" "departments"'
    )

    run_script_ingest_orders = BashOperator(
        task_id='run_script_ingest_orders',
        bash_command='python "/user/app/ProyectoEndToEndPython/Proyecto/Ingesta.py" "retail_db" "orders" "source-projects01" "landing" "orders"'
    )

    run_script_ingest_order_items = BashOperator(
        task_id='run_script_ingest_order_items',
        bash_command='python "/user/app/ProyectoEndToEndPython/Proyecto/Ingesta.py" "retail_db" "order_items" "source-projects01" "landing" "order_items"'
    )

    run_script_ingest_products = BashOperator(
        task_id='run_script_ingest_products',
        bash_command='python "/user/app/ProyectoEndToEndPython/Proyecto/Ingesta.py" "retail_db" "products" "source-projects01" "landing" "products"'
    )

    run_script_transform = BashOperator(
        task_id='run_script_transform',
        bash_command='python "/user/app/ProyectoEndToEndPython/Proyecto/Transformacion.py" "source-projects01" "gold"'
    )

    [run_script_ingest_customers,run_script_ingest_categories,run_script_ingest_departments,run_script_ingest_orders,run_script_ingest_order_items,run_script_ingest_products] >> run_script_transform 