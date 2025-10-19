#### Primeira ação: definir a estrutura da tabela que receberá os dados
Paginas da API :  https://proesc.readme.io/reference/parcelas

para ler

https://hub.asimov.academy/tutorial/mapeando-uma-base-de-dados-com-sqlalchemy/

https://www.alura.com.br/artigos/django-diferenca-entre-null-e-blank?srsltid=AfmBOoq6TZLRHsoHL0mymXhHechxtPe8xkInJ2hbxR3vK4LLwZfD09nz

https://medium.com/@habbema/sqlalchemy-7f55f5619481

https://www.rocketseat.com.br/blog/artigos/post/python-manipulando-um-banco-de-dados-com-SQLAlchemy

https://huogerac.hashnode.dev/estrutura-e-organizacao-de-pastas-em-projetos-flask

https://weslleyf.medium.com/flask-orm-e-mysql-conex%C3%A3o-7c47ffa19d74



Inspecione a resposta da API
• Troque o print simples por print(response.json()) para ver o JSON e entender quais campos precisam ser salvos.

Desenhe o esquema MySQL de acordo com esses campos
• Escolha tipos adequados (INT, DECIMAL, DATE, VARCHAR).
• Defina chave primária (normalmente o id da invoice).
• Crie a tabela com um script SQL (CREATE TABLE …).

Com o esquema claro, os próximos passos serão:
• Instalar um driver (ex.: mysql-connector-python).

• Criar funções de conexão/insert parametrizadas.

• Transformar o JSON em tuplas compatíveis com a tabela.

• Usar transações e tratamento de erros.

-- XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXxx
Como transformar cada registro do JSON em linha de banco.
import json

print(json.dumps(response.json(), indent=2, ensure_ascii=False))

xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
uso collate:
Se a sua aplicação precisa de suporte completo para a maioria das línguas e caracteres especiais (como emojis), a opção utf8mb4_0900_ai_ci é a recomendada.
Se a sua aplicação foca-se em um conjunto específico de caracteres, tem recursos de hardware limitados, ou precisa de um desempenho extra, a utf8mb4_general_ci pode ser uma alternativa viável. 

Estrutura Mysql:
CREATE DATABASE IF NOT EXISTS db_finan_iepsis
              DEFAULT CHARACTER SET utf8mb4
              DEFAULT COLLATE utf8mb4_unicode_ci;

;

CREATE TABLE invoices (
    invoice_id INT PRIMARY KEY,
    person_id INT,
    person_name VARCHAR(255),
    person_cpf VARCHAR(20),
    person_email VARCHAR(255),
    address_cep VARCHAR(10),
    address_street VARCHAR(255),
    address_number VARCHAR(50),
    address_neighborhood VARCHAR(100),
    address_city VARCHAR(100),
    address_state VARCHAR(2),
    enrollment_id INT,
    description TEXT,
    status VARCHAR(50),
    due_date DATE,
    payment_date DATE,
    original_invoice_amount DECIMAL(10, 2),
    paid_invoice_amount DECIMAL(10, 2),
    barcode_line VARCHAR(255),
    pix_text TEXT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
pip install requests mysql-connector-python

XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

'''


    invoice_id, entidade_id, person_id, person_name, person_cpf, person_email,
    address_cep, address_street, address_number, address_neighborhood,
    address_city, address_state, enrollment_id, description,
    invoice_group_id, invoice_group_type_id, invoice_group_type_name,
    invoice_group_total, status, due_date, payment_date,
    original_invoice_amount, updated_invoice_amount, paid_invoice_amount,
    fine_percentage, interest_percentage, barcode_line, pix_text



            # --- CORREÇÃO PRINCIPAL AQUI ---
            # Obtém a próxima URL e garante que ela use HTTPS
            # url    = payload.get("next_page_url")  # None encerra o loop
            # if url and url.startswith("http://"):
            #     print(f"   🔧 Corrigindo URL: http -> https")
            #     url = url.replace("http://", "https://", 1)
            # -------------------------------


