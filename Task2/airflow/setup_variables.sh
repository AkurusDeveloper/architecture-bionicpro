#!/bin/bash

echo "=== Настройка переменных Airflow ==="

docker exec bionicpro-airflow-scheduler airflow variables set crm_api_url "http://core-db:5432"
docker exec bionicpro-airflow-scheduler airflow variables set clickhouse_host "bionicpro-clickhouse"
docker exec bionicpro-airflow-scheduler airflow variables set clickhouse_port "8123"
docker exec bionicpro-airflow-scheduler airflow variables set core_db_url "jdbc:postgresql://core-db:5432/bionicpro_core"
docker exec bionicpro-airflow-scheduler airflow variables set core_db_user "bionicpro"
docker exec bionicpro-airflow-scheduler airflow variables set core_db_password "bionicpro123"

echo "✅ Переменные Airflow установлены!"
echo ""
echo "Проверка переменных:"
docker exec bionicpro-airflow-scheduler airflow variables list

