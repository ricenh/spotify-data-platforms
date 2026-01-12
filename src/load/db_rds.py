import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_rds_conn():
    """Connect to AWS RDS Postgres"""
    return psycopg2.connect(
        host=os.getenv("RDS_HOST"),
        port=int(os.getenv("RDS_PORT", 5432)),
        dbname=os.getenv("RDS_DATABASE"),
        user=os.getenv("RDS_USER"),
        password=os.getenv("RDS_PASSWORD"),
        connect_timeout=10
    )

if __name__ == "__main__":
    # Test connection
    try:
        conn = get_rds_conn()
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()
        print(f"✅ Connected to RDS: {version[0]}")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Connection failed: {e}")