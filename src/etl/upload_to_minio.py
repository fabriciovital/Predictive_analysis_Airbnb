import boto3
import os
from dotenv import load_dotenv

# Carregar variáveis do arquivo .env
load_dotenv()

# Obter credenciais e endpoint do MinIO do .env
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
minio_endpoint = os.getenv('MINIO_ENDPOINT')

# Configurar o cliente MinIO
client = boto3.client(
    's3',
    endpoint_url=minio_endpoint,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

# Diretório de origem dos arquivos CSV
source_dir = 'data/raw/'

# Nome do bucket no MinIO
bucket_name = 'airbnb-datalake'

# Iterar sobre todos os arquivos CSV no diretório de origem e fazer upload
for filename in os.listdir(source_dir):
    if filename.endswith('.csv'):
        local_path = os.path.join(source_dir, filename)
        s3_key = filename  # Nome do arquivo no MinIO
        print(f'Fazendo upload de {local_path} para o bucket {bucket_name} como {s3_key}')
        client.upload_file(local_path, bucket_name, s3_key)