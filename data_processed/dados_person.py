
import os
import requests
import pandas as pd
from datetime import date
from flask_bcrypt import generate_password_hash
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()


class DadosPerson:
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
        years   = [2025]          # ou [2024, 2025, 2026] …
        months = range(10,11) # ou [9,10,11] lista
        #total_pages_num =  ultima_pagina

        for ano in years:
            for mes in months:
                for page_num in range(1, 42):
                    params = { "expiration_year": ano,
                            "expiration_month": f"{mes:02}",
                            "unit_id": 4936,
                            "page": page_num
                        }
                    try:
                        response = requests.get(self.api_base_url, headers=self.headers, params=params)
                        if  response.status_code == 200:
                            data_json = response.json()
                            invoice_list.append(data_json)
                    except:
                        invoice_list.append(None)
        # invoice_list = [] # armazenar paginas em lista
        # month = 10
        # for page_num in range(1, 2):
        #         params = { "expiration_year": 2025,
        #                 "expiration_month": month,
        #                 "unit_id": self.unit_id,
        #                 "page": page_num
        #             }
        #         try:
        #             # print('percorrendo a pagina -> ',page_num)
        #             # print('percorrendo a mes -> ',month)
        #             response = requests.get(self.api_base_url, headers=self.headers, params=params)
        #             if response.status_code == 200:
        #                 invoice_list.append(response.json())
        #         except:
        #             invoice_list.append(None)

        return invoice_list
    
    def data(self, invoice_list):
        '''
        percorre a lista de invoices para pegar dados pessoais
        os loop for percorre os indices de cada pagina
        e dentro de cada pagina percorre os dados pessoais
        e imprime os dados pessoais
        '''
        person_id = []
        cpf = []
        name = []
        email = []
        password = []
        #acessa cada pagina da lista
        for data_page in invoice_list:
            for person_item in data_page['data']:
                try:
                    person_data_api = person_item['pessoa'] or {}
                    person_id_api = person_data_api['id']
                    cpf_api = person_data_api['cadastro_nacional']
                    name_api = person_data_api['nome']
                    email_api = person_data_api['email_comunicacao']
                    password_api = generate_password_hash(person_data_api['cadastro_nacional']).decode('utf-8')
                except Exception as e:
                    print(f'Erro ao processar dados pessoais: {e}')
                    continue                
                # XXXX carregando dados nas listas XXXXXXXXXXXXXXXXXX
                person_id.append(person_id_api)
                cpf.append(cpf_api.strip())
                name.append(name_api.strip().upper()) 
                email.append(email_api)
                password.append(password_api)

        return person_id, cpf , name, email, password

    def create_df_person(self):
        data_financial_person = self.percorre_pages_finan()
        person_id, cpf, name, email, password = self.data(data_financial_person)
        # Definir o DataFrame vazio
        data = pd.DataFrame()
        # Adicionar as colunas ao DataFrame
        data['person_id'] = person_id
        data['cpf'] = cpf
        data['name'] = name
        data['email'] = email
        data['password'] = password
        data['date_created'] = date.today().strftime('%Y-%m-%d')
        data['active'] = 1
        data
        #data.info()
        # remove duplicados com base na coluna 'person_id'
        data_unique = data.drop_duplicates(subset=['person_id'])
        #data_unique.describe()
        data_unique
        return data_unique

    def insert_data(self, data):
        engine = create_engine(os.getenv('DEV_DATABASE_URI'))
        # Inserir os dados na tabela 'user'
        data.to_sql(name='user', con=engine, if_exists='replace', index=False)

        return 


