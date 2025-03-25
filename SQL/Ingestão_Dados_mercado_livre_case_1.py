#!/usr/bin/env python
# coding: utf-8

# In[4]:


import boto3
import pandas as pd
from io import BytesIO

# Configuração das credenciais de acesso e conexão com o S3
aws_access_key = 'AKIAQEFWA3JMYGOWVJHC'
aws_secret_key = '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'
bucket_name = 'projetomercadolivre'
file_key = 'DashboardAnalitico/Bronze/API_CSC_RES_BRO_INTE_en_excel.xlsx'

s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name='us-east-2' 
)

try:
    # Obtendo o arquivo de dentro do S3
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    
    # Ler o conteúdo do arquivo como um dataframe do Pandas
    file_content = response['Body'].read()
    df_1 = pd.read_excel(BytesIO(file_content))  # Usar BytesIO para tratar o fluxo de bytes
    
    # Agora, imprimir as primeiras linhas do DataFrame corretamente
    print(df_1.head())  # Alterado para df_1

except Exception as e:
    print(f"Erro ao acessar o arquivo no S3: {e}")


# In[6]:


# Fazendo a ingestão do Segundo arquivo

file_key = 'DashboardAnalitico/Bronze/API_CSC_RES_DIG_ENA_SERV_en_excel.xlsx'

# Obter o arquivo do S3 e criar o df_2
try:
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    file_content = response['Body'].read()
    df_2 = pd.read_excel(BytesIO(file_content))  # Corrigido o erro de sintaxe, fechando o parêntese
    print("Segundo DataFrame criado!")
    print(df_2.head())

except Exception as e:
    print(f"Erro ao acessar o arquivo no S3: {e}")



# In[7]:


# Fazendo a ingestão do Terceiro arquivo

file_key = 'DashboardAnalitico/Bronze/API_IT_GOV_EGOV_XQ_en_excel.xlsx'

# Obter o arquivo do S3 e criar o df_3
try:
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    file_content = response['Body'].read()
    df_3 = pd.read_excel(BytesIO(file_content))
    print("Terceiro DataFrame criado!")
    print(df_3.head())

except Exception as e:
    print(f"Erro ao acessar o arquivo no S3: {e}")


# In[8]:


# Fazendo a ingestão do Quarto arquivo

file_key = 'DashboardAnalitico/Bronze/API_IT_NET_USER_ZS_en_excel.xlsx'

try:
    # Obter o arquivo do S3 e criar o df_4
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    file_content = response['Body'].read()
    df_4 = pd.read_excel(BytesIO(file_content))  
    print("Quarto DataFrame criado!")
    print(df_4.head())

except Exception as e:
    print(f"Erro ao acessar o arquivo no S3: {e}")



# In[21]:


import sqlite3

# Enviando os 4 arquivos para o meu destino do Sqlite.
# Caminho para o arquivo local do banco SQLite
db_path = r'C:\Users\gilbe\OneDrive\Desktop\Mercado Livre\Case avanço internet\MercadoLivreAnalitico.sqlite'

# Conectar ao banco de dados SQLite
conn = sqlite3.connect(db_path)

try:
    # Inserir o DataFrame internet_banda_larga
    df_1.to_sql('internet_banda_larga', conn, if_exists='replace', index=False)
    print("Dados do DataFrame 'internet_banda_larga' inseridos com sucesso!")

    # Inserir o DataFrame serviços_digitais
    df_2.to_sql('serviços_digitais', conn, if_exists='replace', index=False)
    print("Dados do DataFrame 'serviços_digitais' inseridos com sucesso!")

    # Inserir o DataFrame e_government
    df_3.to_sql('e_government', conn, if_exists='replace', index=False)
    print("Dados do DataFrame 'e_government' inseridos com sucesso!")

    # Inserir o DataFrame internet_população
    df_4.to_sql('internet_população', conn, if_exists='replace', index=False)
    print("Dados do DataFrame 'internet_população' inseridos com sucesso!")

except Exception as e:
    print(f"Erro ao inserir os dados: {e}")
finally:
    # Fechar a conexão com o banco
    conn.close()
    print("Conexão com o banco SQLite fechada!")


# In[ ]:




