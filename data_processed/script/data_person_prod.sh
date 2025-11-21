#!/bin/bash
echo "Ativando Ambiente Virtual"

source /home/ubuntu/services_etl_prod/.venv/bin/activate

sleep 2

echo "executando scritp"

python /home/ubuntu/services_etl_prod/data_processed/extract_data_person.py

echo "Desativando Ambiente Virtual"
sleep 2
deactivate
