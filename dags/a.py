import os

# filename = []
# directory = os.path.join("dags","temp")
# for root,dirs,files in os.walk(directory):
#     for file in files:
#        if file.endswith(".csv"):
#            filename.append(file)

# for file in filename:
#     file_name = os.path.join(directory, file)
#     # s3.load_file(filename=file_name, bucket_name=bucket_name, replace=True,
#     #             key=key)
#     print(file_name)


# def filename(csv:str, exact_dir:str):
    
#     directory = os.path.join("dags","temp")
#     for root, dirs, files in os.walk(directory):
#         for dir in dirs:
#             if dir == exact_dir:
#                 filename = "dags/temp/"+dir+"/"+csv+datetime.now().strftime("%Y-%m-%d")+".csv"
#                 csv_filename=open(filename, 'w')
                
#                 return csv_filename

# # filename("Test", "customer_orders")
# # filename("Test2", "pizza")
# # filename("Test3", "pizza_names")
# # filename("Test4", "pizza_toppings")
# # filename("Test5", "runner_orders")
# # filename("Test6", "runners")


# def unload_to_csv():
#     conn = psycopg2.connect("host=localhost port=5432 dbname=airflow user=airflow password=airflow")
#     cur = conn.cursor()
#     cur.execute("SET search_path TO pizza_runner, public")
#     cur.copy_expert("COPY pizza_runner.customer_orders TO STDOUT WITH CSV HEADER", filename("Test","customer_orders"))
#     cur.copy_expert("COPY pizza_runner.runners TO STDOUT WITH CSV HEADER", filename("Test2","runners"))
#     cur.copy_expert("COPY pizza_runner.runner_orders TO STDOUT WITH CSV HEADER", filename("Test3","runner_orders"))
#     cur.copy_expert("COPY pizza_runner.pizza_names TO STDOUT WITH CSV HEADER", filename("Test4","pizza_names"))
#     cur.copy_expert("COPY pizza_runner.pizza_recipes TO STDOUT WITH CSV HEADER", filename("Test5","pizza_recipes"))
#     cur.copy_expert("COPY pizza_runner.pizza_toppings TO STDOUT WITH CSV HEADER", filename("Test6","pizza_toppings"))
#     conn.commit()
#     conn.close()

#     print("Connection completed")

# unload_to_csv()

directory = os.path.join("dags","temp", "customer_orders")
filename = []
for root,dirs,files in os.walk(directory):
        for file in files:
            if file.endswith(".csv"):
                filename.append(file)
                bucket=file
        # for file in filename:
        #     file_name = os.path.join(directory, file)
        #     print(file_name)

print(file)