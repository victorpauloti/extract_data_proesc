import requests
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

load_dotenv() 

def get_api_data():
    """Busca os dados da API Proesc."""
    url = "https://api.proesc.com/api/v2/invoices?expiration_year=2025&unit_id=4936&expiration_month=09"
    headers = {
      'x-proesc-waf': os.getenv("PROESC_WAF"),
      'Authorization':  f"Bearer {os.getenv('PROESC_TOKEN')}",
      #'Cookie': 'api_proesc_session=fKM6PD8bzJELU8YEyk8sIyPTk28WcguRLmD23fXw'
    }
    PARAMS = {
    "expiration_year": 2025,
    "expiration_month": 9,
    "unit_id": 4936,
    "page": 1
}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Lança exceção para respostas com erro (4xx ou 5xx)
        return response.json().get("data", [])
    except requests.exceptions.RequestException as e:
        print(f"Erro ao chamar a API: {e}")
        return None

def format_decimal(value_str):
    """Converte string monetária '1.234,56' para um formato numérico 1234.56"""
    if not value_str:
        return None
    return float(value_str.replace('.', '').replace(',', '.'))

def insert_invoices_to_db(invoices_data):
    """Conecta ao MySQL e insere os dados das faturas."""
    if not invoices_data:
        print("Nenhum dado de fatura para inserir.")
        return

    try:
        # Recomenda-se usar variáveis de ambiente para as credenciais
        conn = mysql.connector.connect(
            host=os.getenv("DB_HOST", "localhost"),
            user=os.getenv("DB_USER", "admin"),
            password=os.getenv("DB_PASSWORD", "12345"),
            database="db_finan_iepsis"
        )
        cursor = conn.cursor()

        sql_insert_query = """
            INSERT INTO invoices (
                invoice_id, person_id, person_name, person_cpf, person_email,
                address_cep, address_street, address_number, address_neighborhood,
                address_city, address_state, enrollment_id, description, status,
                due_date, payment_date, original_invoice_amount, paid_invoice_amount,
                barcode_line, pix_text
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                person_name=VALUES(person_name), status=VALUES(status),
                payment_date=VALUES(payment_date), paid_invoice_amount=VALUES(paid_invoice_amount),
                last_updated=CURRENT_TIMESTAMP;
        """

        records_to_insert = []
        for item in invoices_data:
            pessoa = item.get("pessoa", {})
            endereco = pessoa.get("endereco", {})
            matricula = item.get("matricula", {})
            bank_slip = item.get("bank_slip", {})
            pix = bank_slip.get("pix", {})

            record = (
                item.get("invoice_id"),
                item.get("person_id"),
                pessoa.get("nome"),
                pessoa.get("cadastro_nacional"),
                pessoa.get("email_comunicacao"),
                endereco.get("cep"),
                endereco.get("nome"),
                endereco.get("numero"),
                endereco.get("bairro"),
                endereco.get("cidade"),
                endereco.get("estado"),
                matricula.get("id"),
                item.get("description"),
                item.get("status"),
                item.get("due_date"),
                item.get("payment_date"),
                format_decimal(item.get("original_invoice_amount")),
                format_decimal(item.get("paid_invoice_amount")),
                bank_slip.get("barcode_line"),
                pix.get("pix_text")
            )
            records_to_insert.append(record)

        cursor.executemany(sql_insert_query, records_to_insert)
        conn.commit()
        print(f"{cursor.rowcount} registros inseridos/atualizados com sucesso.")

    except Error as e:
        print(f"Erro ao conectar ou inserir no MySQL: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print("Conexão com o MySQL foi fechada.")

if __name__ == "__main__":
    invoices = get_api_data()
    if invoices:
        insert_invoices_to_db(invoices)
