#!/bin/bash
echo "Ativando Ambiente Virtual"
source /home/ubuntu/services_etl_prod/.venv/bin/activate
sleep 3
echo "executando scritp"
python /home/ubuntu/services_etl_prod/data_processed/extract_data_invoice.py
echo "Desativando Ambiente Virtual"
deactivate
