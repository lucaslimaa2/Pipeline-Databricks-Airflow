from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

def run_databricks_notebook(**kwargs):
    execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')
    databricks_task = DatabricksRunNowOperator(
        task_id='Extraindo-conversoes',
        databricks_conn_id='databricks_default',
        job_id='***',
        notebook_params={'data_execucao': execution_date}
    )
    databricks_task.execute(context=kwargs)

with DAG(
    'Executando-notebook-etl',
    default_args=default_args,
    description='Executando notebook ETL no Databricks',
    start_date=datetime(2024, 7, 1),  # Data inicial para o histórico
    schedule_interval='40 14 * * *',  # Executa diariamente
    catchup=True,  # Habilita a execução de datas passadas
) as dag_executando_notebook_extracao:
    
    run_notebook = PythonOperator(
        task_id='run_notebook',
        python_callable=run_databricks_notebook,
        provide_context=True
    )

    transformando_dados = DatabricksRunNowOperator(
        task_id='transformando-dados',
        databricks_conn_id='databricks_default',
        job_id='***'
    )

    run_notebook >> transformando_dados