
import os
import requests
import pandas as pd
from datetime import date
from flask_bcrypt import generate_password_hash
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging
logging.basicConfig(filename='etl_person.log',level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()


class DadosPerson:
    '''
    Classe para interagir com a API do PROESC FINANCEIRO e obter dados de ALUNOS.
    '''
    def __init__(self, unit_id):

        self.unit_id = unit_id
        self.api_base_url = "https://api.proesc.com/api/v2/invoices"
        self.headers = {"x-proesc-waf": os.getenv("PROESC_WAF"), "Authorization": f"Bearer {os.getenv('PROESC_TOKEN')}"}

    def percorre_pages_person(self):
        '''
        Obtém a lista de invoices da API do PROESC FINANCEIRO.
        '''
        invoice_list = [] # armazenar paginas em lista
        years   = [2024,2025,2026]

        for ano in years:
            #for page_num in range(1, data_pag_ultima + 1):  # de 1 até ultima pagian
            page_num = 0
            data_pag_ultima = 0
            while page_num <= data_pag_ultima:
                page_num = page_num + 1
                params = { "expiration_year": ano,
                            #"expiration_month": f"{mes:02}",
                            "unit_id": self.unit_id,
                            "page": page_num
                        }
                try:
                    response = requests.get(self.api_base_url, headers=self.headers,params=params)
                    if  response.status_code == 200:
                        data = response.json()
                        data_pag_ultima = data["last_page"]
                        #print(f"Ano {ano} | Mês {mes:02} | Página {page_num} de {data_pag_ultima}")
                        logging.info(f"Ano {ano} | Página {page_num}  DE {data_pag_ultima}")
                        invoice_list.append(response.json())
                except:
                    logging.error(invoice_list.append(None))

        return invoice_list
    
    def data(self, invoice_list):
        '''
        percorre a lista de invoices para pegar dados pessoais
        os loop for percorre os indices de cada pagina
        e dentro de cada pagina percorre os dados pessoais
        e imprime os dados pessoais
        '''
        # listas de saída
        person_id, cpf, name, email, password = [], [], [], [], []
        # 1) cache para evitar gerar o mesmo hash várias vezes
        pwd_cache: dict[str, str] = {}
        # 2) evita processar a mesma pessoa duas vezes
        processed_ids: set[int] = set()
        for page in invoice_list:
            if not page:
                continue
            # usa .get para não estourar KeyError
            for item in page.get("data", []):
                    matricula = item.get("matricula")
                    if not matricula:
                        continue
                    matricula_ativa = (matricula.get("ativa") or "")
                    if matricula_ativa == True:
                        pessoa = item.get("pessoa")          # pode vir None
                        if not pessoa:
                            continue
                        pid = pessoa.get("id")
                        if pid in processed_ids:
                            continue                         # já tratamos este id
                        processed_ids.add(pid)
                        
                        # --- campos básicos ---
                        cpf_raw   = (pessoa.get("cadastro_nacional") or "").strip()
                        nome_raw  = (pessoa.get("nome") or "").strip().upper()
                        email_raw = pessoa.get("email_comunicacao")
                        #password_raw   = (pessoa.get("cadastro_nacional") or "").strip()
                        
                        # --- hash com cache ---
                        # if cpf_raw in pwd_cache:
                        #     pwd_hashed = pwd_cache[cpf_raw]
                        # else:
                        #     #print(f'Gerando hash para CPF {cpf_raw}')
                        #     pwd_hashed = generate_password_hash(cpf_raw)
                        #     #print(f'Hash gerado: {pwd_hashed}')
                        #     pwd_cache[cpf_raw] = pwd_hashed
                       
                        # --- agrega nas listas ---
                        person_id.append(pid)
                        cpf.append(cpf_raw)
                        name.append(nome_raw)
                        email.append(email_raw)
                        password.append(cpf_raw)

        return person_id, cpf , name, email, password

    def create_df_person(self):
        data_financial_person = self.percorre_pages_person()
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
        # remove duplicados com base na coluna 'person_id'
        data_unique = data.drop_duplicates(subset=['person_id'])
        data_unique
        return data_unique

    def insert_data_person(self):
        data_frame_person = self.create_df_person()
        engine = create_engine(os.getenv('DEV_DATABASE_URI'))
        data_frame_person.to_sql(name='user', con=engine, if_exists='replace', index=False)
        
        return logging.info(f"Dados inseridos com sucesso na tabela 'user', {len(data_frame_person)} registros pessoais.")

