import os
import requests
import time
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

load_dotenv()                     # Carrega variáveis do .env

BASE_URL = "https://api.proesc.com/api/v2/invoices?expiration_year=2025&unit_id=4936&expiration_month=09&page=1"

HEADERS = {
    "x-proesc-waf": os.getenv("PROESC_WAF"),
    "Authorization": f"Bearer {os.getenv('PROESC_TOKEN')}"
}

PARAMS = {
    "expiration_year": 2025,
    "expiration_month": 9,
    "unit_id": 4936,
    "page": 1
}

def format_decimal(value_str: str):
    """Converte '1.234,56' → 1234.56 (float)."""
    if not value_str:
        return None
    return float(value_str.replace('.', '').replace(',', '.'))

def open_db_connection():
    """Abre conexão única com o MySQL."""
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )

def insert_invoices(cursor, records):
    """Insere/atualiza lote de faturas."""
    sql = """
        INSERT INTO invoices (
            invoice_id, 
            person_id,
            person_name,
            person_cpf,
            person_email,
            address_cep, address_street, address_number, address_neighborhood,
            address_city, address_state,
            enrollment_id, description, status,
            due_date, payment_date, original_invoice_amount, paid_invoice_amount,
            barcode_line, pix_text
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            person_name           = VALUES(person_name),
            status                = VALUES(status),
            payment_date          = VALUES(payment_date),
            paid_invoice_amount   = VALUES(paid_invoice_amount),
            last_updated          = CURRENT_TIMESTAMP
    """
    cursor.executemany(sql, records)

def build_record(item):
    """Transforma um item JSON em tupla compatível com a tabela."""
    pessoa    = item.get("pessoa", {})
    endereco  = pessoa.get("endereco", {})
    matricula = item.get("matricula", {})
    bank_slip = item.get("bank_slip", {})
    pix       = bank_slip.get("pix", {})

    return (
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

def fetch_and_store_all_pages():
    """Percorre todas as páginas e grava cada lote no banco."""
    current_url = BASE_URL
    params      = PARAMS.copy()

    try:
        conn   = open_db_connection()
        cursor = conn.cursor()

        while current_url:
            time.sleep(1)
            #response = requests.get(current_url, headers=HEADERS, params=params, timeout=15)
            response = requests.get(current_url, headers=HEADERS)
            response.raise_for_status()
            payload = response.json()

            data = payload.get("data", [])
            if not data:
                break

            records = [build_record(item) for item in data]
            insert_invoices(cursor, records)
            conn.commit()
            print(f"Página {payload.get('current_page')} → {len(records)} registros processados.")

            # Próxima página
            current_url = payload.get("next_page_url")
            params = None   # URLs seguintes já contêm todos os parâmetros

    except (requests.RequestException, Error) as exc:
        print(f"Erro: {exc}")
        conn.rollback()
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()
            print("Conexão encerrada.")

if __name__ == "__main__":
    fetch_and_store_all_pages()