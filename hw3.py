import psycopg2

# Параметры соединения
conn_string= "host='localhost' port=54320:5432 dbname='my_database' user='root' password='postgres'"
conn_string2= "host='localhost' port=5433:5432 dbname='my_database2' user='root' password='postgres'"

#Список таблиц
databases=['customer','lineitem','nation','orders','part','partsupp','region','supplier']

for db in databases:
    with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
        q = f"COPY {db} TO STDOUT WITH DELIMITER ',' CSV HEADER;"
        with open('resultsfile.csv', 'w') as f:
            cursor.copy_expert(q, f)


    with psycopg2.connect(conn_string2) as conn, conn.cursor() as cursor:
        q = f"COPY {db} from STDIN WITH DELIMITER ',' CSV HEADER;"
        with open('resultsfile.csv', 'r') as f:
            cursor.copy_expert(q, f)

