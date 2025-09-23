import psycopg2  # or psycopg
from template import Templates
from config import get_db_connection

def finalize_sql(sql: str, params: dict, conn) -> str:
    with conn.cursor() as cur:
        return cur.mogrify(sql, params).decode("utf-8")

conn = get_db_connection()
tpl = Templates()

sql, params = tpl.patients_drugs_time("RxNorm","RxNorm","1154343","1191",60)
final_sql = finalize_sql(sql, params, conn)
print(final_sql)

with conn, conn.cursor() as cur:
    cur.execute(final_sql)           
    row = cur.fetchone()
    print(row)
