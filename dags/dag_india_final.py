"""Extend the second-delivery ETL with training, reporting and finite Kafka replay."""
from datetime import timedelta
import os
from pathlib import Path
import sys
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

ROOT=Path(os.getenv('INDIA_PROJECT_ROOT','/opt/project'))
sys.path.insert(0,str(ROOT/'src'))
from pipeline_india import (prepare_run,extract_air,extract_cities,extract_weather,eda_sources,
    transform_merge,validate_enriched,load_warehouse,visualize_warehouse)

def prepare_final(run_id):
    from final_setup import prepare
    prepare();prepare_run(run_id)

def train_final(run_id):
    from final_ml import train
    train(os.environ['INDIA_DATABASE_URL'],ROOT/'artifacts')

def report_final(run_id):
    from final_report import generate_report
    generate_report(os.environ['INDIA_DATABASE_URL'],ROOT/'reports/final',ROOT/'artifacts')

def publish_final(run_id):
    from final_stream import create_topic,produce,TOPIC
    bootstrap=os.getenv('KAFKA_BOOTSTRAP_SERVERS','kafka:9092')
    topic=os.getenv('KAFKA_TOPIC',TOPIC)
    create_topic(bootstrap,topic)
    produce(os.environ['INDIA_DATABASE_URL'],bootstrap,topic,'2023-01-01','2024-12-31',.1,100,run_id)

with DAG('etl_india_final',description='Validated warehouse, AQI model, static report and Kafka replay',
    start_date=pendulum.datetime(2026,11,22,tz='Asia/Kolkata'),schedule_interval=timedelta(days=1),
    catchup=False,max_active_runs=1,default_args={'owner':'Samuel Flores y Santiago Becerra','retries':0},
    tags=['india','final-delivery']) as dag:
    def task(name,fn,minutes=20):
        return PythonOperator(task_id=name,python_callable=fn,op_kwargs={'run_id':'{{ run_id }}'},
            execution_timeout=timedelta(minutes=minutes))
    prepare=task('preparar_ejecucion',prepare_final)
    air=task('extraer_primera_entrega_sqlite',extract_air)
    cities=task('extraer_dataset_ciudades_bd',extract_cities)
    weather=task('extraer_api_clima',extract_weather,45)
    eda=task('eda_tres_fuentes',eda_sources)
    merge=task('transformar_integrar',transform_merge)
    quality=task('validar_great_expectations',validate_enriched)
    load=task('cargar_data_warehouse',load_warehouse)
    previous_charts=task('visualizar_desde_warehouse',visualize_warehouse)
    model=task('entrenar_modelo',train_final)
    report=task('reporte_estatico',report_final)
    publish=task('publicar_kafka',publish_final)
    prepare >> [air,cities]
    cities >> weather
    [air,cities,weather] >> eda >> merge >> quality >> load
    load >> previous_charts
    load >> model >> [report,publish]
