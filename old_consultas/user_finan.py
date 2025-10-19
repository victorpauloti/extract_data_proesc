import os
import time
import requests
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from datetime import datetime
from typing import Any, Dict, List, Tuple
from requests.exceptions import ReadTimeout, ConnectionError


load_dotenv()

BASE_URL = "https://api.proesc.com/api/v2/invoices"
HEADERS = {
    "x-proesc-waf": os.getenv("PROESC_WAF"),
    "Authorization": f"Bearer {os.getenv('PROESC_TOKEN')}"
}

# ---------- timeout ----------------------------------------------------------
MAX_RETRIES = 3
TIMEOUT     = 30        # segundos

def get_json_with_retry(url: str, *, params=None, headers=None) -> dict:
    """
    Faz GET com até MAX_RETRIES tentativas em caso de ReadTimeout/ConnectionError.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"→ GET {url}  (tentativa {attempt}/{MAX_RETRIES})")
            resp = requests.get(url, params=params, headers=headers, timeout=TIMEOUT)
            # print(f"Status code: {resp.status_code}")
            # if resp.headers.get("content-type", "").startswith("application/json"): # incluso
            #   print("Resposta bruta:", resp.text[:300])   # 1ªs 300 chars para debug incluso

            resp.raise_for_status()

            return resp.json()
        
        except (ReadTimeout, ConnectionError) as exc:
            print(f"   ⚠️  Timeout/ConnError: {exc}")
            if attempt == MAX_RETRIES:
                raise                    # esgota tentativas → propaga exceção
            sleep_for = 2 * attempt      # back-off linear (2s, 4s…)
            print(f"   ↻ aguardando {sleep_for}s e tentando de novo…")
            time.sleep(sleep_for)

# ---------- helpers ----------------------------------------------------------
def money_to_decimal(value: str) -> float | None:
    if not value:
        return None
    return float(value.replace('.', '').replace(',', '.'))

def safe_date(date_str: str) -> str | None:
    """
    Converte ISO ou 'YYYY-MM-DD' -> 'YYYY-MM-DD'. 
    Retorna None se string vazia.
    """
    if not date_str:
        return None
    return datetime.fromisoformat(date_str[:10]).date()

def db_connect():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )

# ---------- mapeamento JSON -> tupla ----------------------------------------
def build_record(item: Dict[str, Any]) -> Tuple:
    pessoa    = item.get("pessoa", {})
    # endereco  = pessoa.get("endereco", {}) or {}
    # matricula = item.get("matricula", {}) or {}
    # bank_slip = item.get("bank_slip", {})
    # pix       = bank_slip.get("pix", {})
    # late_fees = item.get("late_payment_fees", {})

    return (
        # item.get("invoice_id"),
        # item.get("entidade_id"),
        item.get("person_id"),
        pessoa.get("nome"),
        pessoa.get("cadastro_nacional"),
        pessoa.get("email_comunicacao"),

        # endereco.get("cep"),
        # endereco.get("nome"),
        # endereco.get("numero"),
        # endereco.get("bairro"),
        # endereco.get("cidade"),
        # endereco.get("estado"),
        # matricula.get("id"),
        # item.get("description"),
        # item.get("invoice_group_id"),
        # item.get("invoice_group_type_id"),
        # item.get("invoice_group_type_name"),
        # item.get("invoice_group_total"),

        # item.get("status"),
        # safe_date(item.get("due_date")),
        # safe_date(item.get("payment_date")),

        # money_to_decimal(item.get("original_invoice_amount")),
        # money_to_decimal(item.get("updated_invoice_amount")),
        # money_to_decimal(item.get("paid_invoice_amount")),

        # money_to_decimal(late_fees.get("fine")),
        # money_to_decimal(late_fees.get("interest")),

        # bank_slip.get("barcode_line"),
        # pix.get("pix_text")
    )

# ---------- insert -----------------------------------------------------------
INSERT_SQL = """
INSERT INTO user (
    person_cpf, person_id, person_email, person_name,
    password, data_created, active, 
) VALUES (
    %s,%s,%s,%s,%s,%s,%s
)
ON DUPLICATE KEY UPDATE
    data_created          = CURRENT_TIMESTAMP;
"""

def insert_batch(cursor, batch: List[Tuple]):
    cursor.executemany(INSERT_SQL, batch)

# ---------- paginação --------------------------------------------------------
def fetch_and_store():
    """
    Percorre todas as páginas, corrigindo a URL para HTTPS e gravando no banco.
    """
    params = {
    "expiration_year": 2025,
    "expiration_month": "09",
    "unit_id": 4936,
    "per_page": 30,
    "page": 1 
    }

    conn   = cursor = None
    try:
        conn   = db_connect()
        cursor = conn.cursor()

        while True:
            url    = BASE_URL
            # busca na funao q fz o insert  get_json_with_retry
            payload = get_json_with_retry(url, params=params, headers=HEADERS)
            
            data = payload.get("data", []) # incluso
            last_page  = payload.get("last_page", params["page"])
            current_pg = payload.get("current_page", params["page"])

            print(f"Pág {current_pg}/{last_page} → {len(data)} registros")
            
            # incluso
            if not data:
                print("Nenhum dado. Interrompendo loop.")
                break

            batch = [build_record(obj) for obj in data]
            #if batch:
            insert_batch(cursor, batch)
            conn.commit()
            if current_pg >= last_page:
                break          # chegamos ao fim

            params["page"] += 1   # próxima página preservando os filtros
            time.sleep(1)
    except (Error, requests.RequestException) as exc:
        if conn:
            conn.rollback()
        print(f"\nERRO detectado: {exc}")
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
            print("Conexão encerrada.")

# ---------- main -------------------------------------------------------------
if __name__ == "__main__":
    fetch_and_store()
