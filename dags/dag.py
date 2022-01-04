from re import S
import airflow
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
import sql
import os


default_Args = {
    "owner": "Nelson",
    "depends_on_past": False,
    "wait_for_downstream": False,
    "start_date": datetime(2022, 1, 3),
    "email": ["ogbeide331@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries":1,
    'retry_delay': timedelta(minutes=3)
}


dag = DAG(
    "pizza_runner_pipeline",
    default_args=default_Args,
    schedule_interval="@daily",
)

start_operation = DummyOperator(task_id= "Start_Operation", dag=dag)




clean_runner_order = PostgresOperator(
    dag=dag,
    task_id="clean_runner_order",
    sql=sql.clean_runner_order,
    postgres_conn_id="postgres_default",
    depends_on_past=True,
    wait_for_downstream=True,
)

clean_customer_order = PostgresOperator(
    dag=dag,
    task_id="clean_customer_order",
    sql=sql.clean_customer_order_data,
    postgres_conn_id="postgres_default",
    depends_on_past=True,
    wait_for_downstream=True,
)

runner_dtype = PostgresOperator(
    dag=dag,
    task_id = "runner_dtype",
    sql = sql.alter_run_table,
    postgres_conn_id="postgres_default",
    depends_on_past=True,
    wait_for_downstream=True,
)

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
                filename = "dags/temp/"+dir+"/"+csv+"_"+datetime.now().strftime("%Y-%m-%d")+".csv"
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



extract_to_csv = PythonOperator(
                     task_id="extract_to_csv",
                     python_callable=unload_to_csv,
                     dag=dag,
                     depends_on_past=True,
                     wait_for_downstream=True,
                     )


def upload_to_s3(
    bucket_name, key, exact_dir, remove_local:bool = False):
    s3 = S3Hook("aws_credentials")
    filename = []
    directory = os.path.join("dags","temp", exact_dir)
    for root,dirs,files in os.walk(directory):
        for file in files:
            if file.endswith(".csv"):
                filename.append(file)
    for file in filename:
        file_name = os.path.join(directory, file)
        s3.load_file(filename=file_name, bucket_name=bucket_name, replace=True,
                    key=key)
                    
        if remove_local:                
            if os.path.isfile(file_name):
                os.remove(file_name)
    
                
s3_cutomer_order = PythonOperator(
    dag=dag,
    task_id="s3_cutomer_order",
    python_callable=upload_to_s3,
    op_kwargs={
        "key": "stage_data/customer_order.csv",
        "exact_dir": "customer_orders",
        "bucket_name": "pizzacon",
        "remove_local": True

    },
    depends_on_past=True,
    wait_for_downstream=True,
)

s3_pizza_names = PythonOperator(
    dag=dag,
    task_id="s3_pizza_names",
    python_callable=upload_to_s3,
    op_kwargs={
        "key": "stage_data/pizza_names.csv",
        "exact_dir": "pizza_names",
        "bucket_name": "pizzacon",
        "remove_local": True

    },
    depends_on_past=True,
    wait_for_downstream=True,
)

s3_pizza_recipes = PythonOperator(
    dag=dag,
    task_id="s3_pizza_recipes",
    python_callable=upload_to_s3,
    op_kwargs={
        "key": "stage_data/pizza_recipes.csv",
        "exact_dir": "pizza_recipes",
        "bucket_name": "pizzacon",
        "remove_local": True

    },
    depends_on_past=True,
    wait_for_downstream=True,
)

s3_pizza_toppings = PythonOperator(
    dag=dag,
    task_id="s3_pizza_toppings",
    python_callable=upload_to_s3,
    op_kwargs={
        "key": "stage_data/pizza_toppings.csv",
        "exact_dir": "pizza_toppings",
        "bucket_name": "pizzacon",
        "remove_local": True

    },
    depends_on_past=True,
    wait_for_downstream=True,
)

s3_runner_orders = PythonOperator(
    dag=dag,
    task_id="s3_runner_orders",
    python_callable=upload_to_s3,
    op_kwargs={
        "key": "stage_data/runner_orders.csv",
        "exact_dir": "runner_orders",
        "bucket_name": "pizzacon",
        "remove_local": True

    },
    depends_on_past=True,
    wait_for_downstream=True,
)

s3_runners = PythonOperator(
    dag=dag,
    task_id="s3_runners",
    python_callable=upload_to_s3,
    op_kwargs={
        "key": "stage_data/runners.csv",
        "exact_dir": "runners",
        "bucket_name": "pizzacon",
        "remove_local": True

    },
    depends_on_past=True,
    wait_for_downstream=True,
)

# def task_1(sqll):
#     phook = PostgresHook(postgre_conn_id='postgres_default',
#                         host='host.docker.internal',
#                         database='airflow',
#                         user='airflow',
#                         password='airflow',
#                         port=5432)
#     records = phook.get_records(sqll)
#     return records





start_s3_to_redshift = DummyOperator(
    dag=dag,
    task_id="start_s3_to_redshift",
    depends_on_past=True,
    wait_for_downstream=True
)

redshift_runner = S3ToRedshiftTransfer(
        dag=dag,
        task_id = 'redshift_runner',
        aws_conn_id='aws_credentials',
        redshift_conn_id='redshift',
        schema='public',
        table='runners',
        s3_bucket='pizzacon',
        s3_key='stage_data',
        copy_options=['csv','ignoreheader 1'],
)

redshift_pizza_names = S3ToRedshiftTransfer(
        dag=dag,
        task_id = 'redshift_pizza_names',
        aws_conn_id='aws_credentials',
        redshift_conn_id='redshift',
        schema='public',
        table='pizza_names',
        s3_bucket='pizzacon',
        s3_key='stage_data',
        copy_options=['csv','ignoreheader 1'],
)

redshift_runner_orders = S3ToRedshiftTransfer(
        dag=dag,
        task_id = 'redshift_runner_orders',
        aws_conn_id='aws_credentials',
        redshift_conn_id='redshift',
        schema='public',
        table='runner_orders',
        s3_bucket='pizzacon',
        s3_key='stage_data',
        copy_options=['csv','ignoreheader 1'],
)

redshift_customer_orders = S3ToRedshiftTransfer(
        dag=dag,
        task_id = 'redshift_customer_orders',
        aws_conn_id='aws_credentials',
        redshift_conn_id='redshift',
        schema='public',
        table='customer_order',
        s3_bucket='pizzacon',
        s3_key='stage_data',
        copy_options=['csv','ignoreheader 1'],
)

redshift_pizza_recipe = S3ToRedshiftTransfer(
        dag=dag,
        task_id = 'redshift_pizza_recipe',
        aws_conn_id='aws_credentials',
        redshift_conn_id='redshift',
        schema='public',
        table='pizza_recipes',
        s3_bucket='pizzacon',
        s3_key='stage_data',
        copy_options=['csv','ignoreheader 1'],
)

redshift_pizza_toppings = S3ToRedshiftTransfer(
        dag=dag,
        task_id = 'redshift_pizza_toppings',
        aws_conn_id='aws_credentials',
        redshift_conn_id='redshift',
        schema='public',
        table='pizza_toppings',
        s3_bucket='pizzacon',
        s3_key='stage_data',
        copy_options=['csv','ignoreheader 1'],
)





start_operation >> [clean_runner_order , clean_customer_order] >>  runner_dtype >> extract_to_csv >> [s3_cutomer_order, s3_pizza_names,
s3_pizza_recipes, s3_pizza_toppings, s3_runner_orders, s3_runners] >> start_s3_to_redshift >> [redshift_customer_orders, redshift_pizza_names, redshift_pizza_recipe, redshift_pizza_toppings, redshift_runner_orders, redshift_runner]




# start_operation >> s3_to_redshift