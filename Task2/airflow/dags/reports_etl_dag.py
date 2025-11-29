from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator

ETL_JAR_PATH = "/opt/etl/etl-java.jar"
ETL_IMAGE = "task2-etl-java:latest"

default_args = {
    'owner': 'bionicpro',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bionicpro_reports_etl',
    default_args=default_args,
    description='ETL pipeline для витрины отчётов BionicPRO',
    schedule_interval='0 */1 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['bionicpro', 'etl', 'reports'],
)

extract_crm = DockerOperator(
    task_id='extract_crm_java',
    image=ETL_IMAGE,
    api_version='auto',
    auto_remove=True,
    force_pull=False,
    command='java -jar /app/etl-java.jar --job=extractCrmJob --date={{ ds }}',
    docker_url='unix://var/run/docker.sock',
    network_mode='task2_bionicpro-network',
    mount_tmp_dir=False,
    environment={
        'SPRING_DATASOURCE_CLICKHOUSE_URL': 'jdbc:clickhouse://bionicpro-clickhouse:8123/default',
        'SPRING_DATASOURCE_CLICKHOUSE_USERNAME': 'default',
        'SPRING_DATASOURCE_CLICKHOUSE_PASSWORD': '',
        'SPRING_BATCH_DATASOURCE_URL': 'jdbc:postgresql://postgres-core:5432/bionicpro_core',
        'SPRING_BATCH_DATASOURCE_USERNAME': 'bionicpro',
        'SPRING_BATCH_DATASOURCE_PASSWORD': 'bionicpro123',
        'CRM_API_URL': 'http://postgres-core:5432',
    },
    dag=dag,
)

extract_telemetry = DockerOperator(
    task_id='extract_telemetry_java',
    image=ETL_IMAGE,
    api_version='auto',
    auto_remove=True,
    force_pull=False,
    command='java -jar /app/etl-java.jar --job=extractTelemetryJob --date={{ ds }}',
    docker_url='unix://var/run/docker.sock',
    network_mode='task2_bionicpro-network',
    mount_tmp_dir=False,
    environment={
        'SPRING_DATASOURCE_CLICKHOUSE_URL': 'jdbc:clickhouse://bionicpro-clickhouse:8123/default',
        'SPRING_DATASOURCE_CLICKHOUSE_USERNAME': 'default',
        'SPRING_DATASOURCE_CLICKHOUSE_PASSWORD': '',
        'SPRING_DATASOURCE_COREDB_URL': 'jdbc:postgresql://postgres-core:5432/bionicpro_core',
        'SPRING_DATASOURCE_COREDB_USERNAME': 'bionicpro',
        'SPRING_DATASOURCE_COREDB_PASSWORD': 'bionicpro123',
        'SPRING_BATCH_DATASOURCE_URL': 'jdbc:postgresql://postgres-core:5432/bionicpro_core',
        'SPRING_BATCH_DATASOURCE_USERNAME': 'bionicpro',
        'SPRING_BATCH_DATASOURCE_PASSWORD': 'bionicpro123',
    },
    dag=dag,
)

build_mart = DockerOperator(
    task_id='build_mart_java',
    image=ETL_IMAGE,
    api_version='auto',
    auto_remove=True,
    force_pull=False,
    command='java -jar /app/etl-java.jar --job=buildMartJob --date={{ ds }}',
    docker_url='unix://var/run/docker.sock',
    network_mode='task2_bionicpro-network',
    mount_tmp_dir=False,
    environment={
        'SPRING_DATASOURCE_CLICKHOUSE_URL': 'jdbc:clickhouse://bionicpro-clickhouse:8123/default',
        'SPRING_DATASOURCE_CLICKHOUSE_USERNAME': 'default',
        'SPRING_DATASOURCE_CLICKHOUSE_PASSWORD': '',
        'SPRING_BATCH_DATASOURCE_URL': 'jdbc:postgresql://postgres-core:5432/bionicpro_core',
        'SPRING_BATCH_DATASOURCE_USERNAME': 'bionicpro',
        'SPRING_BATCH_DATASOURCE_PASSWORD': 'bionicpro123',
    },
    dag=dag,
)

optimize_clickhouse = BashOperator(
    task_id='optimize_clickhouse',
    bash_command="""
    curl -X POST 'http://bionicpro-clickhouse:8123/' \
         --data-binary 'OPTIMIZE TABLE mart_report_user_daily FINAL' \
         || echo "Optimize command executed"
    """,
    dag=dag,
)

extract_crm >> build_mart
extract_telemetry >> build_mart
build_mart >> optimize_clickhouse



