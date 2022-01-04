from airflow.hooks.postgres_hook import PostgresHook
import os
from datetime import datetime 


def create_hook_conn():
    
    hook = PostgresHook(postgre_conn_id='postgres_default',
                        host='host.docker.internal',
                        database='airflow',
                        user='airflow',
                        password='airflow',
                        port=5432)
    conn = hook.get_conn()
    cur = conn.cursor()
    return cur

def filename(csv:str, exact_dir:str):
    directory = os.path.join("dags","temp")
    for root, dirs, files in os.walk(directory):
        for dir in dirs:
            if dir == exact_dir:
                filename = "dags/temp/"+dir+"/"+csv+datetime.now().strftime("%Y-%m-%d")+".csv"
                csv_filename=open(filename, 'w')
                
                return csv_filename

def unload_to_csv():
    cur = create_hook_conn()
    cur.execute("SET search_path TO pizza_runner, public")
    cur.copy_expert("COPY pizza_runner.customer_order_cleans TO STDOUT WITH CSV HEADER", filename("customer_orders","customer_orders"))
    cur.copy_expert("COPY pizza_runner.runners TO STDOUT WITH CSV HEADER", filename("runners","runners"))
    cur.copy_expert("COPY pizza_runner.runner_order_clean TO STDOUT WITH CSV HEADER", filename("runner_order_clean","runner_orders"))
    cur.copy_expert("COPY pizza_runner.pizza_names TO STDOUT WITH CSV HEADER", filename("pizza_names","pizza_names"))
    cur.copy_expert("COPY pizza_runner.pizza_recipes TO STDOUT WITH CSV HEADER", filename("pizza_recipes","pizza_recipes"))
    cur.copy_expert("COPY pizza_runner.pizza_toppings TO STDOUT WITH CSV HEADER", filename("pizza_toppings","pizza_toppings"))
    print("Connection Worked")