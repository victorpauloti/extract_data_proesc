from dados_person import DadosPerson
import time
import logging
# logging.basicConfig(filename='time_person.log',level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# count time start
instante_inicial = time.perf_counter()
logging.info(f'start count time {instante_inicial}')

dados_pessoais = DadosPerson(4936)
#percorre_lista = dados_pessoais.percorre_pages_person()
#data_financial_person = dados_pessoais.create_df_person()
insert_dados = dados_pessoais.insert_data_person()

# count time finish
instante_final = time.perf_counter()
logging.info(f'end count time {instante_final}')

duracao = instante_final - instante_inicial
tempo = duracao / 60
logging.info(f'Duração da execução: {tempo:.2f} segundos')