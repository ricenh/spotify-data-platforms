import psycopg2
import os

def get_conn():
    return psycopg2.connect(
        host=os.getenv('RDS_HOST'),
        port=int(os.getenv('RDS_PORT', 5432)),
        dbname=os.getenv('RDS_DATABASE', 'spotify'),
        user=os.getenv('RDS_USER', 'spotify_admin'),
        password=os.getenv('RDS_PASSWORD')
    )