import psycopg2

# Функция для печати всех баз данных
def print_all_databases(user, password):
    conn = psycopg2.connect(
        dbname='postgres',
        user=user,
        password=password,
        host='localhost',
        port=5434
    )
    cursor = conn.cursor()

    cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
    databases = cursor.fetchall()
    print("Databases:")
    for database in databases:
        print(database[0])

    conn.close()

# Функция для получения данных из таблицы user_online
def fetch_user_online_data(db_name, user, password):
    conn = psycopg2.connect(
        dbname=db_name,
        user=user,
        password=password,
        host='localhost',
        port=5434
    )
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, status, updated_at FROM public.user_online")
    rows = cursor.fetchall()
    
    conn.close()
    return rows

# Функция для обновления таблицы user_online в базе данных
def update_user_online_table(db_name, data, user, password):
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=user,
            password=password,
            host='localhost',
            port=5434
        )
        cursor = conn.cursor()
        
        for id, status, updated_at in data:
            cursor.execute("""
                INSERT INTO public.user_online (id, status, updated_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET status = EXCLUDED.status, updated_at = EXCLUDED.updated_at;
            """, (id, status, updated_at))
        
        conn.commit()
    except Exception as e:
        print(f"Error updating database '{db_name}': {e}")
    finally:
        if conn:
            conn.close()

# Основная функция
def main():
    primary_db = 'mbss_master'
    excluded_dbs = [
        'postgres',
        'mbss_master',
        'flagman_mbss_master',
        'rox_mbss_master'
    ]
    
    secondary_dbs = [
        'legzo_mbss_master',
        'jet_mbss_master',
        'fresh_mbss_master',
        'sol_mbss_master',
        'izzi_mbss_master',
        'starda_mbss_master',
        'drip_mbss_master',
        '1go_mbss_master',
        'lex_mbss_master',
        'irwin_mbss_master',
        'monro_mbss_master',
        'gizbo_mbss_master'
    ]

    user = 'robo_sudo'
    password = '%dFgH8!zX4&kLmT2'

    # Печать всех баз данных
    print_all_databases(user, password)

    # Получение данных из основной базы данных
    data = fetch_user_online_data(primary_db, user, password)
    
    # Обновление вторичных баз данных
    for db in secondary_dbs:
        if db not in excluded_dbs:
            update_user_online_table(db, data, user, password)

if __name__ == "__main__":
    main()
