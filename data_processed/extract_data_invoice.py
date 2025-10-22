from dados_finan import DadosFinanceiro
import time
import logging
#logging.basicConfig(filename='time_person.log',level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# count time start
instante_inicial = time.perf_counter()
logging.info(f'start count time invoice {instante_inicial}')

dados_finan = DadosFinanceiro('4936')
#data_financial_person = dados_finan.create_df_finan()
insert_dados = dados_finan.insert_data_finan()

# count time finish
instante_final = time.perf_counter()
logging.info(f'end count time invoice {instante_final}')


duracao = instante_final - instante_inicial
tempo = duracao / 60
logging.info(f'Duração da execução: {tempo:.2f} segundos')