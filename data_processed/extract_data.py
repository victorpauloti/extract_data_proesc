from dados_person import DadosPerson
from dados_finan import DadosFinanceiro
import time

instante_inicial = time.perf_counter()
dados_pessoais = DadosPerson('4936')
data_financial_person = dados_pessoais.create_df_person()

#print(data_financial_person)

# dados_financeiros = DadosFinanceiro('4936')
# data_financial_invoices =  dados_financeiros.create_df_finan()
#print(data_financial_invoices)

# salvando os dados
data_financial_person.to_csv('data_processed/dados_person.csv', index=False)

instante_final = time.perf_counter()

duracao = instante_final - instante_inicial
print(f'Duração da execução: {duracao:.2f} segundos')
#data_financial_invoices.to_csv('data_processed/dados_finan.csv', index=False)

# XXXXXXXXXXXXXXXX INSERINDO DADOS NA TABELA USUARIO XXXXXXXXXXXXXXXXXX
#data_financial_person.insert_data()