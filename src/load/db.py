import psycopg2


def get_conn():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="spotify",
        user="spotify_user",
        password="spotify_pass",
    )
