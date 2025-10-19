
import os
import requests
import pandas as pd
from dotenv import load_dotenv

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
        for page_num in range(1, 2):
            params = { "expiration_year": 2025,
                    "expiration_month": 10,
                    "unit_id": self.unit_id,
                    "page": page_num
                }
            try:
                #print('percorrendo a pagina -> ',page_num)
                response = requests.get(self.api_base_url, headers=self.headers, params=params)
                invoice_list.append(response.json())
            except:
                invoice_list.append(None)

        return invoice_list
    
    def data_financial(self, invoice_list):
        invoice_id = []
        person_id = []
        matricula_turma = []
        matricula_ativa = []
        description = []
        order = []
        status_api = []
        description = []
        due_date = []
        payment_date = []
        barcode_line = []
        pix_text = []


        for invoice_per_page in invoice_list:
            #print(invoice_per_page_1)
            for financial_data in invoice_per_page['data']:
                try:
                    invoice_id_api = financial_data['invoice_id']
                    person_id_api = financial_data['person_id']
                    # dados matricula
                    matricula_data_api = financial_data.get('matricula') or {} # se nao tiver matricula retorna um dict vazio
                    matricula_turma_api = matricula_data_api.get('turma_id')
                    matricula_ativa_api = matricula_data_api.get('ativa')
                    
                    # dados financeiros
                    order_id_api = financial_data['order']
                    status_api_data = financial_data['status']
                    description_api = financial_data['description']
                    due_date_api = financial_data['due_date']
                    payment_date_api = financial_data['payment_date']
                    #dados do boleto bancario e pix
                    bank_data_api = financial_data.get('bank_slip') or {} # se nao tiver bank_slip retorna um dict vazio
                    barcode_line_api = bank_data_api.get('barcode_line')
                    pix_data_api = bank_data_api.get('pix_text')
                except:
                    pass

                # adicionar os dados a variavel
                invoice_id.append(invoice_id_api)
                person_id.append(person_id_api)
                matricula_turma.append(matricula_turma_api)
                matricula_ativa.append(matricula_ativa_api)
                order.append(order_id_api)
                status_api.append(status_api_data)
                description.append(description_api)
                due_date.append(due_date_api)
                payment_date.append(payment_date_api)
                barcode_line.append(barcode_line_api)
                pix_text.append(pix_data_api)

        return invoice_id, person_id, matricula_turma, matricula_ativa, order, status_api, description,due_date, payment_date, barcode_line, pix_text

    def create_df_finan(self):
        data_financial = self.percorre_pages_finan()
        invoice_id, person_id, matricula_turma, matricula_ativa, order, status_api, description,due_date, payment_date, barcode_line, pix_text = self.data_financial(data_financial)    

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

        return data