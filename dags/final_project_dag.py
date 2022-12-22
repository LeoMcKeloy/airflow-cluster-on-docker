from datetime import datetime

import psycopg2

from airflow.models import Variable
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
# from airflow.contrib.operators.ssh_operator import SSHOperator


def get_conn_credentials(conn_id) -> BaseHook.get_connection:
    conn = BaseHook.get_connection(conn_id)
    return conn


def connect_to_psql(**kwargs):
    ti = kwargs['ti']

    conn_id = Variable.get("conn_id")

    conn_to_airflow = get_conn_credentials(conn_id)

    pg_hostname, pg_port, pg_username, pg_pass, pg_db = \
        conn_to_airflow.host, conn_to_airflow.port, conn_to_airflow.login, \
        conn_to_airflow.password, conn_to_airflow.schema

    ti.xcom_push(value=[pg_hostname, pg_port, pg_username, pg_pass, pg_db], key='conn_to_airflow')

    conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)
    cursor = conn.cursor()

    cursor.execute("CREATE TABLE IF NOT EXISTS vedomosti " +
                   "(id serial PRIMARY KEY, title varchar, link varchar, guid varchar, pdalink varchar, " +
                   "author varchar, category varchar, enclosure varchar, pubDate varchar);")

    conn.commit()

    cursor.close()
    conn.close()


def read_from_psql(**kwargs):
    ti = kwargs['ti']

    pg_hostname, pg_port, pg_username, pg_pass, pg_db = ti.xcom_pull(key='conn_to_airflow', task_ids="conn_to_psql")

    conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM vedomosti LIMIT 10")
    print(cursor.fetchone())

    cursor.close()
    conn.close()


with DAG(dag_id="final_project_dag", start_date=datetime(2022, 1, 1), schedule="0 0 * * *") as dag:
    bash_task = BashOperator(task_id="hello", bash_command="echo hello")
    conn_to_psql_task = PythonOperator(task_id="conn_to_psql", python_callable=connect_to_psql)
    read_from_psql_task = PythonOperator(task_id="read_from_psql", python_callable=read_from_psql)

    # spark_bash = """
    # ssh root@spark-master /opt/workspace/run_spark_jar.sh
    # """
    spark_task = BashOperator(task_id="spark_bash", bash_command="ssh root@spark-master /opt/workspace/run_spark_jar.sh")

    bash_task >> conn_to_psql_task >> spark_task >> read_from_psql_task
