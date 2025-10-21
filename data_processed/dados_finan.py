import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging
logging.basicConfig(filename='etl_finan.log',level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


load_dotenv()

class DadosFinanceiro:
    '''
    Classe para interagir com a API do PROESC FINANCEIRO e obter dados de ALUNOS.
    '''
    def __init__(self, unit_id):

        self.unit_id = unit_id
        self.api_base_url = "https://api.proesc.com/api/v2/invoices"
        self.headers = {"x-proesc-waf": os.getenv("PROESC_WAF"), "Authorization": f"Bearer {os.getenv('PROESC_TOKEN')}"}

    def percorre_pages_finan(self):
        '''
        Obtém a lista de invoices da API do PROESC FINANCEIRO.
        '''
        invoice_list = [] # armazenar paginas em lista
        years   = [2024,2025,2026]

        for ano in years:
            page_num = 0
            data_pag_ultima = 0
            while page_num <= data_pag_ultima:
                page_num = page_num + 1
                params = { "expiration_year": ano,
                            "unit_id": self.unit_id,
                            "page": page_num
                        }
                try:
                    response = requests.get(self.api_base_url, headers=self.headers,params=params)
                    if  response.status_code == 200:
                        data = response.json()
                        data_pag_ultima = data["last_page"]
                        print(f"Ano {ano} | Página {page_num}  DE {data_pag_ultima}")
                        invoice_list.append(response.json())
                except:
                    invoice_list.append(None)

        return invoice_list
    
    def data_financial(self, invoice_list):
        turma_id = [323297,323301,323298,323293,340179,383505,368209,370031]
        invoice_id, person_id, matricula_turma, matricula_ativa, description, order, status_api, description = [], [], [], [], [], [], [], []
        due_date, payment_date, barcode_line, pix_text, value, value_pay, external_gateway_url = [], [], [], [], [], [], []


        for invoice_per_page in invoice_list:
            for financial_data in invoice_per_page['data']:
                # dados matricula
                matricula_data_api = financial_data.get('matricula') or {} # se nao tiver matricula retorna um dict vazio
                if matricula_data_api.get('turma_id') in turma_id:
                    if matricula_data_api.get('ativa') == True:
                        matricula_turma_api = matricula_data_api.get('turma_id')
                        matricula_ativa_api = matricula_data_api.get('ativa')
                        # dados invoices(fatura)
                        invoice_id_api = financial_data['invoice_id']
                        person_id_api = financial_data['person_id']
                        # dados financeiros
                        order_id_api = financial_data['order']
                        status_api_data = financial_data['status']
                        description_api = financial_data['description']
                        due_date_api = financial_data['due_date']
                        payment_date_api = financial_data['payment_date']
                        original_invoice_amount_api = financial_data['original_invoice_amount']
                        paid_invoice_amount_api = financial_data['paid_invoice_amount']
                        #dados do boleto bancario e pix
                        bank_data_api = financial_data.get('bank_slip') or {} # se nao tiver bank_slip retorna um dict vazio
                        barcode_line_api = bank_data_api.get('barcode_line')
                        pix_data_api = bank_data_api.get('pix_text')
                        external_gateway_url_api = bank_data_api.get('external_gateway_url')

                        # adicionar os dados a variavel
                        invoice_id.append(invoice_id_api)
                        person_id.append(person_id_api)
                        matricula_turma.append(matricula_turma_api)
                        matricula_ativa.append(matricula_ativa_api)
                        order.append(order_id_api)
                        status_api.append(status_api_data)
                        description.append(description_api.strip())
                        due_date.append(due_date_api)
                        payment_date.append(payment_date_api)
                        barcode_line.append(barcode_line_api)
                        pix_text.append(pix_data_api)
                        external_gateway_url.append(external_gateway_url_api)
                        value.append(original_invoice_amount_api)
                        value_pay.append(paid_invoice_amount_api)

        return invoice_id, person_id, matricula_turma, matricula_ativa, order, status_api, description,due_date, payment_date, barcode_line, pix_text, value, value_pay, external_gateway_url

    def create_df_finan(self):
        data_financial = self.percorre_pages_finan()
        invoice_id, person_id, matricula_turma, matricula_ativa, order, status_api, description,due_date, payment_date, barcode_line, pix_text, value, value_pay, external_gateway_url = self.data_financial(data_financial)    

        data = pd.DataFrame()
        data['invoice_id'] = invoice_id
        data['person_id'] = person_id
        data['matricula_turma'] = matricula_turma
        data['matricula_ativa'] = matricula_ativa
        data['order'] = order
        data['status_api'] = status_api
        data['description'] = description
        data['due_date'] = due_date
        data['payment_date'] = payment_date
        data['barcode_line'] = barcode_line
        data['pix_text'] = pix_text
        data['value'] = value
        data['value_pay'] = value_pay
        data['external_gateway_url'] = external_gateway_url

        return data

    def insert_data_finan(self):
        data_frame_finan = self.create_df_finan()
        engine = create_engine(os.getenv('DEV_DATABASE_URI'))
        # Inserir os dados na tabela 'user'
        data_frame_finan.to_sql(name='invoices', con=engine, if_exists='replace', index=False)
        return print(f"Dados inseridos com sucesso na tabela 'user', {len(data_frame_finan)} registros.")