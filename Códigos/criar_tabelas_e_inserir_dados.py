import mysql.connector
import pandas as pd

# Conexão com o banco de dados MySQL
conn = mysql.connector.connect(
    host='localhost', 
    port = 3306,
    database='olist_tcc',
    user='usuário_mysql',
    password='senha_mysql' 
)

cursor = conn.cursor()

# Deletar tabelas se existirem
cursor.execute('DROP TABLE IF EXISTS PEDIDO_ITENS')
cursor.execute('DROP TABLE IF EXISTS PEDIDO_PAGAMENTOS')
cursor.execute('DROP TABLE IF EXISTS PEDIDO_COMENTARIOS')
cursor.execute('DROP TABLE IF EXISTS PEDIDOS')
cursor.execute('DROP TABLE IF EXISTS CLIENTES')
cursor.execute('DROP TABLE IF EXISTS VENDEDORES')
cursor.execute('DROP TABLE IF EXISTS PRODUTOS')
cursor.execute('DROP TABLE IF EXISTS GEOLOCALIZACAO')

# Criação da tabela produtos no MySQL
cursor.execute('''
    CREATE TABLE PRODUTOS (
        cod_produto CHAR(32) NOT NULL,
        categoria VARCHAR(50),
        tamanho_nome INT,
        tamanho_descricao INT,
        qnt_fotos INT,
        massa INT,
        comprimento INT,
        largura INT,
        altura INT,
        PRIMARY KEY (cod_produto)
    );
''')

# Carregar dados do CSV e inserir na tabela Produtos
produtos = pd.read_csv('D:/TCC/Datasets_tratados/produtos.csv')

for row in produtos.itertuples(index=False):
    cursor.execute('''
        INSERT INTO PRODUTOS (
            cod_produto, categoria, tamanho_nome, 
            tamanho_descricao, qnt_fotos, massa, 
            comprimento, largura, altura
        ) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ''', (
        row.product_id, 
        row.product_category_name,
        row.product_name_lenght,
        row.product_description_lenght,
        row.product_photos_qty,
        row.product_weight_g,
        row.product_length_cm,
        row.product_width_cm,
        row.product_height_cm
    ))

conn.commit()

# Criação da tabela pedido_comentarios
cursor.execute('''
    CREATE TABLE PEDIDO_COMENTARIOS(
        cod_avaliacao CHAR(32) NOT NULL,
        cod_pedido CHAR(32),
        pontuacao INT,
        titulo_comentario VARCHAR(100),
        mensagem_comentario VARCHAR(500),
        data_criacao DATETIME,
        data_resposta DATETIME,
        PRIMARY KEY (cod_avaliacao)
    );
''')

# Carregar dados do CSV e inserir na tabela pedido_comentarios
pedido_comentarios = pd.read_csv('D:/TCC/Datasets_tratados/pedido_comentarios.csv')

for row in pedido_comentarios.itertuples(index=False):
    cursor.execute('''
        INSERT IGNORE INTO PEDIDO_COMENTARIOS (
            cod_avaliacao, cod_pedido, pontuacao, titulo_comentario,
            mensagem_comentario, data_criacao, data_resposta
        )   
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    ''', (
        row.review_id, 
        row.order_id,
        row.review_score,
        row.review_comment_title,
        row.review_comment_message,
        row.review_creation_date,
        row.review_answer_timestamp
    ))

conn.commit()

# Criação da tabela Clientes
cursor.execute('''
    CREATE TABLE CLIENTES(
        cod_cliente_pedido CHAR(32) NOT NULL,
        cod_cliente CHAR(32),
        prefix_cep CHAR(5),
        cidade VARCHAR(50),
        estado VARCHAR(50),
        PRIMARY KEY (cod_cliente_pedido)
    );
''')

# Carregar dados do CSV e inserir na tabela Clientes
clientes = pd.read_csv('D:/TCC/Datasets_tratados/clientes.csv')

for row in clientes.itertuples(index=False):
    cursor.execute('''
        INSERT INTO CLIENTES (
            cod_cliente_pedido, cod_cliente, prefix_cep,
            cidade, estado
        )   
        VALUES (%s, %s, %s, %s, %s)
    ''', (
        row.customer_id, 
        row.customer_unique_id,
        row.customer_zip_code_prefix,
        row.customer_city,
        row.customer_state
    ))

conn.commit()

# Criação da tabela Geolocalização
cursor.execute('''
    CREATE TABLE GEOLOCALIZACAO(
        prefix_cep CHAR(5) NOT NULL,
        latitude FLOAT,
        longitude FLOAT,
        cidade VARCHAR(50),
        estado VARCHAR(50),
        PRIMARY KEY (prefix_cep)
    );
''')

# Carregar dados do CSV e inserir na tabela Geolocalização
geolocalizacao = pd.read_csv('D:/TCC/Datasets_tratados/geolocalizacao.csv')

for row in geolocalizacao.itertuples(index=False):
    cursor.execute('''
        INSERT IGNORE INTO GEOLOCALIZACAO (
            prefix_cep, latitude, longitude,
            cidade, estado
        )   
        VALUES (%s, %s, %s, %s, %s)
    ''', (
        row.geolocation_zip_code_prefix, 
        row.geolocation_lat,
        row.geolocation_lng,
        row.geolocation_city,
        row.geolocation_state
    ))

# Inserção registros que faltavam de cep dos vendedores
cursor.execute('''
    INSERT IGNORE INTO GEOLOCALIZACAO (
        prefix_cep, longitude, latitude, cidade, estado
    ) 
    VALUES 
        ('02285', -46.5678753, -23.431341, 'sao paulo', 'sp'),
        ('07412', -46.341797, -23.3994586, 'aruja', 'sp'),
        ('21941', -43.2438268, -22.8143993, 'rio de janeiro', 'rj'),
        ('37708', -46.4992939, -21.8124775, 'pocos de caldas', 'mg'),
        ('71551', -47.8679859, -15.7045682, 'brasilia', 'df'),
        ('72580', -47.9893173, -15.9924741, 'brasilia', 'df'),
        ('82040', -49.2056041, -25.3981577, 'curitiba', 'pr'),
        ('91901', -51.2589679, -30.1060653, 'porto alegre', 'rs');
''')

conn.commit()

# Criação da tabela Vendedores
cursor.execute('''
    CREATE TABLE VENDEDORES(
        cod_vendedor CHAR(32) NOT NULL,
        prefix_cep CHAR(5),
        cidade VARCHAR(50),
        estado CHAR(2),
        PRIMARY KEY (cod_vendedor)
    );
''')

# Carregar dados do CSV e inserir na tabela Vendedores
vendedores = pd.read_csv('D:/TCC/Datasets_tratados/vendedores.csv')

for row in vendedores.itertuples(index=False):
    cursor.execute('''
        INSERT INTO VENDEDORES (
            cod_vendedor, prefix_cep, cidade, estado
        )   
        VALUES (%s, %s, %s, %s)
    ''', (
        row.seller_id, 
        row.seller_zip_code_prefix,
        row.seller_city,
        row.seller_state
    ))

conn.commit()

# Criação da tabela pedido_pagamentos
cursor.execute('''
    CREATE TABLE PEDIDO_PAGAMENTOS(
        cod_pedido CHAR(32) NOT NULL,
        sequencial_pagamento INT NOT NULL,
        tipo VARCHAR(50),
        parcelas INT,
        valor FLOAT,
        PRIMARY KEY (cod_pedido, sequencial_pagamento)
    );
''')

# Carregar dados do CSV e inserir na tabela Pagamentos
pedido_pagamentos = pd.read_csv('D:/TCC/Datasets_tratados/pedido_pagamentos.csv')

for row in pedido_pagamentos.itertuples(index=False):
    cursor.execute('''
        INSERT INTO PEDIDO_PAGAMENTOS (
            cod_pedido, sequencial_pagamento, tipo, parcelas, valor
        )   
        VALUES (%s, %s, %s, %s, %s)
    ''', (
        row.order_id, 
        row.payment_sequential,
        row.payment_type,
        row.payment_installments,
        row.payment_value
    ))

# Inserção um registro faltante
cursor.execute('''
    INSERT INTO PEDIDO_PAGAMENTOS (
        cod_pedido, sequencial_pagamento, valor
    ) 
    VALUES ('bfbd0f9bdef84302105ad712db648a6c', 1, 134.97);
''')

conn.commit()

# Criação da tabela Pedidos
cursor.execute('''
    CREATE TABLE PEDIDOS(
        cod_pedido CHAR(32) NOT NULL,
        cod_cliente CHAR(32),
        status VARCHAR(20),
        data DATETIME,
        data_aprovacao DATETIME,
        data_entrega_transportadora DATETIME,
        data_entrega_cliente DATETIME,
        data_estimada_entrega DATETIME,
        PRIMARY KEY (cod_pedido)
    );
''')

# Carregar dados do CSV e inserir na tabela Pedidos
pedidos = pd.read_csv('D:/TCC/Datasets_tratados/pedidos.csv')

for row in pedidos.itertuples(index=False):
    cursor.execute('''
        INSERT INTO PEDIDOS (
            cod_pedido, cod_cliente, status, data, data_aprovacao, 
            data_entrega_transportadora, data_entrega_cliente, 
            data_estimada_entrega
        )   
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ''', (
        row.order_id, 
        row.customer_id,
        row.order_status,
        row.order_purchase_timestamp,
        row.order_approved_at,
        row.order_delivered_carrier_date,
        row.order_delivered_customer_date,
        row.order_estimated_delivery_date
    ))

# Criação da tabela de pedido_itens
cursor.execute('''
    CREATE TABLE IF NOT EXISTS pedido_itens (
        cod_pedido CHAR(32) NOT NULL,
        item_pedido INT NOT NULL,
        cod_produto CHAR(32) NOT NULL,
        cod_vendedor CHAR(32) NOT NULL,
        data_limite_entrega DATETIME NULL,
        preco FLOAT NULL,
        frete FLOAT NULL,
        PRIMARY KEY (cod_pedido, item_pedido)
    );
''')

# Carregar dados do CSV e inserir na tabela Pedidos
pedido_itens = pd.read_csv('D:/TCC/Datasets_tratados/pedido_itens.csv')

# Inserção dos dados na tabela pedido_itens
for row in pedido_itens.itertuples(index=False):
    cursor.execute('''
        INSERT INTO pedido_itens (cod_pedido, item_pedido, cod_produto, cod_vendedor, data_limite_entrega, preco, frete)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    ''', (
        row.order_id, 
        row.order_item_id,
        row.product_id,
        row.seller_id,
        row.shipping_limit_date,
        row.price,
        row.freight_value
    ))


conn.commit()

# Fechar a conexão
cursor.close()
conn.close()

print('Final do procedimento')