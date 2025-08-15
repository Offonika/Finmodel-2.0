import sqlite3

source_db = "finmodel.db"
output_file = "schema.sql"

conn = sqlite3.connect(source_db)
cursor = conn.cursor()

# Получаем все инструкции CREATE
cursor.execute("""
    SELECT sql FROM sqlite_master
    WHERE type IN ('table', 'index', 'trigger')
    AND name NOT LIKE 'sqlite_%'
""")
schema = [row[0] for row in cursor.fetchall() if row[0]]

# Сохраняем в файл
with open(output_file, "w", encoding="utf-8") as f:
    for stmt in schema:
        f.write(stmt.strip() + ";\n\n")

conn.close()
print(f"✅ Схема базы сохранена в {output_file}")
