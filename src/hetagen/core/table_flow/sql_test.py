import os
import sqlite3

sql_query = """
SELECT 实体, "市值（美元）" FROM "指定公司市值排名表" ORDER BY "市值（美元）" DESC LIMIT 10

"""


current_dir = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(current_dir, "data", "指定公司市值排名表.db")
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
cur.execute(sql_query)
result = cur.fetchall()
print(result)
