from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import sqlite3
import os
import csv
from datetime import datetime
import numpy as np
import openpyxl


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

def check_file_exists(file_path):
    """
    Проверка наличия файла.

    Функция принимает путь к файлу и проверяет, существует ли файл по указанному пути.
    Если файл не найден, выбрасывается исключение FileNotFoundError.

    Аргументы:
        file_path (str): Путь к проверяемому файлу.

    Исключения:
        FileNotFoundError: Выбрасывается, если файл отсутствует по указанному пути.
    """
    if not os.path.exists(file_path):
        # Если файл не найден, выбрасывается исключение с указанием пути
        raise FileNotFoundError(f"Файл {file_path} не найден.")
    

def validate_columns_events(file_path):
    """
    Проверка наличия обязательных столбцов в файле.

    Функция загружает CSV-файл, проверяет наличие необходимых столбцов
    и выбрасывает исключение, если каких-либо столбцов не хватает.

    Аргументы:
        file_path (str): Путь к файлу, который необходимо проверить.

    Исключения:
        ValueError: Выбрасывается, если отсутствуют обязательные столбцы.

    Логика работы:
    1. Читаем файл с помощью pandas.
    2. Определяем список обязательных столбцов.
    3. Сравниваем обязательные столбцы с реальными столбцами из файла.
    4. Если каких-то столбцов не хватает, выбрасываем исключение с их перечнем.
    """
    # Чтение CSV-файла
    df = pd.read_csv(file_path)
    
    # Список обязательных столбцов
    required_columns = [
        'datetime', 'user_id', 'event_name', 'page',
        'product_id', 'order_id'
    ]

    
    # Поиск отсутствующих столбцов
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    # Если отсутствуют обязательные столбцы, выбрасываем исключение
    if missing_columns:
        raise ValueError(f"Не хватает столбцов: {', '.join(missing_columns)}")
    

def validate_columns_orders(file_path):
    """
    Проверка наличия обязательных столбцов в файле.

    Функция загружает CSV-файл, проверяет наличие необходимых столбцов
    и выбрасывает исключение, если каких-либо столбцов не хватает.

    Аргументы:
        file_path (str): Путь к файлу, который необходимо проверить.

    Исключения:
        ValueError: Выбрасывается, если отсутствуют обязательные столбцы.

    Логика работы:
    1. Читаем файл с помощью pandas.
    2. Определяем список обязательных столбцов.
    3. Сравниваем обязательные столбцы с реальными столбцами из файла.
    4. Если каких-то столбцов не хватает, выбрасываем исключение с их перечнем.
    """
    # Чтение CSV-файла
    df = pd.read_csv(file_path)
    
    # Список обязательных столбцов
    required_columns = [
        'datetime', 'user_id',
        'product_id', 'order_id', 'revenue'
    ]
    
    # Поиск отсутствующих столбцов
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    # Если отсутствуют обязательные столбцы, выбрасываем исключение
    if missing_columns:
        raise ValueError(f"Не хватает столбцов: {', '.join(missing_columns)}")


def validate_events_file(events_file_path):
    
    # Проверяет структуру файла events.csv Валидность формата дат.
        
    try:
        # Чтение файла с явным указанием возможных разделителей и кодировки
        df = pd.read_csv(events_file_path)

        # Попытка безопасного приведения типов (не меняем исходный df для проверки)
        df_types = df.copy()

        # Проверка валидности дат (без вывода предупреждения)
        datetime_parsed = pd.to_datetime(df_types['datetime'], errors='coerce')
        
        # Проверка на наличие пустых значений после преобразования
        if datetime_parsed.isna().any():
            raise ValueError("В столбце 'datetime' найдены некорректные или пустые значения.")
            
        if df_types['user_id'].isna().any():
            raise ValueError("В столбце 'user_id' найдены некорректные или пустые значения.")
            
        if df_types['event_name'].isna().any():
            raise ValueError("В столбце 'event_name' найдены некорректные или пустые значения.")
            
        if df_types['page'].isna().any():
            raise ValueError("В столбце 'page' найдены некорректные или пустые значения.")
        
        print(f"Файл {events_file_path} успешно прошел проверку.")
        
    except Exception as e:
        # Оборачиваем все остальные ошибки в понятное сообщение
        raise ValueError(f"Ошибка при проверке файла: {e}")

def validate_orders_file(orders_file_path):
    # Проверяет структуру файла orders.csv Валидность формата дат.
    
    try:
        # Чтение файла с явным указанием возможных разделителей и кодировки
        df = pd.read_csv(orders_file_path)
        
        df_types = df.copy()

        # Проверка валидности дат (без вывода предупреждения)
        datetime_parsed = pd.to_datetime(df_types['datetime'], errors='coerce')
        
        # Проверка на наличие пустых значений после преобразования
        if datetime_parsed.isna().any():
            raise ValueError("В столбце 'datetime' найдены некорректные или пустые значения.")
                        
        if df_types['revenue'].isna().any():
            raise ValueError("В столбце 'revenue' найдены некорректные или пустые значения.")
        
        print(f"Файл {orders_file_path} успешно прошел проверку.")
        
    except Exception as e:
        # Оборачиваем все остальные ошибки в понятное сообщение
        raise ValueError(f"Ошибка при проверке файла: {e}")


def validate_data_structures(events_path, orders_path):
    """
    Проверяет структуру и бизнес-правила для файлов events.csv и orders.csv.
    """
    try:
        # Загрузка данных
        events_df = pd.read_csv(events_path)
        orders_df = pd.read_csv(orders_path)

        errors = []
        print("🔍 Начало проверки данных...")

        # --- ПРОВЕРКА ФАЙЛА EVENTS ---
        print("\n--- Проверка events.csv ---")

        # 1. Проверка: Все ли обязательные столбцы есть в наличии?
        required_events_cols = {'datetime', 'user_id', 'event_name', 'page', 'product_id', 'order_id'}
        if not required_events_cols.issubset(events_df.columns):
            missing = required_events_cols - set(events_df.columns)
            errors.append(f"❌ В events.csv отсутствуют столбцы: {', '.join(missing)}")

        # 2. Проверка: Формат datetime (YYYY-MM-DD HH:MM:SS)
        try:
            pd.to_datetime(events_df['datetime'], format='%Y-%m-%d %H:%M:%S', errors='raise')
        except ValueError as e:
            errors.append("❌ Ошибка в формате datetime в events.csv. Ожидалось: YYYY-MM-DD HH:MM:SS")

        # 3. Проверка: user_id в диапазоне 1-10000
        invalid_user_ids = events_df[
            (events_df['user_id'].notna()) & 
            ((events_df['user_id'] < 1) | (events_df['user_id'] > 10000))
        ]
        if not invalid_user_ids.empty:
            errors.append(f"❌ Некорректные user_id в events.csv (вне диапазона 1-10000). Найдено строк: {len(invalid_user_ids)}")

        # 4. Проверка: event_name только из списка допустимых
        allowed_events = {'view', 'click', 'add', 'purchase'}
        invalid_events = events_df[~events_df['event_name'].isin(allowed_events)]
        if not invalid_events.empty:
            errors.append(f"❌ Недопустимые значения в event_name. Найдено строк: {len(invalid_events)}")

        # 5. Проверка: page только из списка допустимых
        allowed_pages = {'main_page', 'search', 'catalog', 'recommendations', 'pdp', 'cart'}
        invalid_pages = events_df[~events_df['page'].isin(allowed_pages)]
        if not invalid_pages.empty:
            errors.append(f"❌ Недопустимые значения в page. Найдено строк: {len(invalid_pages)}")

        # 6. Проверка: product_id должен быть NULL или в диапазоне 1-500
        invalid_product_ids = events_df[
            (events_df['product_id'].notna()) & 
            ((events_df['product_id'] < 1) | (events_df['product_id'] > 500))
        ]
        if not invalid_product_ids.empty:
            errors.append(f"❌ Некорректные product_id в events.csv (вне диапазона 1-500). Найдено строк: {len(invalid_product_ids)}")

        # 7. Проверка: order_id должен быть NULL, если event_name != 'purchase'
        invalid_order_ids = events_df[
            (events_df['event_name'] != 'purchase') & 
            (events_df['order_id'].notna())
        ]
        if not invalid_order_ids.empty:
            errors.append(f"❌ Логическая ошибка: order_id не NULL при event_name != 'purchase'. Найдено строк: {len(invalid_order_ids)}")


        # --- ПРОВЕРКА ФАЙЛА ORDERS ---
        print("\n--- Проверка orders.csv ---")

        # 1. Проверка: Все ли обязательные столбцы есть в наличии?
        required_orders_cols = {'datetime', 'user_id', 'product_id', 'order_id', 'revenue'}
        if not required_orders_cols.issubset(orders_df.columns):
            missing = required_orders_cols - set(orders_df.columns)
            errors.append(f"❌ В orders.csv отсутствуют столбцы: {', '.join(missing)}")

        # 2. Проверка: Формат datetime
        try:
            pd.to_datetime(orders_df['datetime'], errors='coerce')
            if orders_df['datetime'].isna().any():
                raise ValueError("Найдены некорректные даты")
        except ValueError:
            errors.append("❌ Ошибка в формате datetime в orders.csv.")

         # 3. Проверка: revenue в диапазоне 10-1000
         # Используем .astype(float) для обработки возможных строковых значений
        invalid_revenue = orders_df[
            (orders_df['revenue'].astype(float) < 10) | 
            (orders_df['revenue'].astype(float) > 1000)
         ]
        if not invalid_revenue.empty:
            errors.append(f"❌ Некорректная выручка (revenue) в orders.csv (вне диапазона 10-1000). Найдено строк: {len(invalid_revenue)}")


         # --- КРОСС-ТАБЛИЧНЫЕ ПРОВЕРКИ ---
        print("\n--- Кросс-проверки между файлами ---")

         # 4. Проверка: Все ли order_id из orders.csv есть в events.csv?
        orders_ids = set(orders_df['order_id'].dropna().astype(str))
        events_purchase_ids = set(
            events_df[events_df['event_name'] == 'purchase']['order_id'].dropna().astype(str)
        )
         
        missing_in_events = orders_ids - events_purchase_ids
        if missing_in_events:
            errors.append(f"⚠️ Предупреждение: В orders.csv найдены order_id, которых нет в событиях purchase: {missing_in_events}")

         # 5. Проверка: Все ли order_id из events.csv (purchase) есть в orders.csv?
        missing_in_orders = events_purchase_ids - orders_ids
        if missing_in_orders:
            errors.append(f"⚠️ Предупреждение: В событиях purchase найдены order_id, которых нет в orders.csv: {missing_in_orders}")


    except FileNotFoundError as e:
        print(f"❌ Файл не найден: {e.filename}")
        return
    except Exception as e:
        print(f"❌ Критическая ошибка при чтении файлов: {e}")
        return

    # --- ВЫВОД РЕЗУЛЬТАТОВ ---
    if errors:
        print("\n🚨 Итог проверки: ОБНАРУЖЕНЫ ОШИБКИ!")
        for err in errors:
            print(err)
    else:
        print("\n✅ Итог проверки: ДАННЫЕ СООТВЕТСТВУЮТ ВСЕМ ПРАВИЛАМ!")


def preprocess_data_events(input_file, output_file):
    """
    Обработка данных.

    Функция выполняет очистку и преобразование данных:
    1. Преобразует столбец с датами в формат datetime.
    2. Добавляет новые метрики:
        - месяц визита.
        - день недели.

    Аргументы:
        input_file (str): Путь к входному CSV-файлу с необработанными данными.
        output_file (str): Путь к выходному CSV-файлу для сохранения обработанных данных.

    Логика работы:
    1. Чтение данных из файла.
    2. Очистка и преобразование данных.
    3. Добавление новых метрик для дальнейшего анализа.
    4. Сохранение данных в новый файл.
    """
    # Чтение данных из входного файла
    df = pd.read_csv(input_file)

    
    # Используем 'Int64' для поддержки NaN значений
    df['product_id'] = pd.to_numeric(df['product_id'], errors='coerce')
    df['order_id']   = pd.to_numeric(df['order_id'], errors='coerce')
    df['user_id']    = pd.to_numeric(df['user_id'], errors='coerce')
   

    # Преобразование столбца с датами в формат datetime
    df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d %H:%M:%S')


    # Добавление новых метрик
    df['month'] = df['datetime'].dt.month  # Месяц визита
    df['weekday'] = df['datetime'].dt.day_name()  # День недели визита



    # Словарь с соответствием старых и новых названий столбцов
    column_mapping = {
        'datetime': 'datetime',
        'user_id': 'user_id',
        'event_name': 'event_name',
        'page': 'page',
        'product_id': 'product_id',
        'order_id': 'order_id',
        'month': 'month',
        'weekday': 'weekday'
    }

    # Переименование столбцов
    df = df.rename(columns=column_mapping)

    # Удаление существующего csv-файла
    if os.path.exists(output_file):
            os.remove(output_file)
            print(f"Существующий файл {output_file} удален.")

    # Сохранение обработанных данных в выходной файл
    df.to_csv(output_file, index=False)


def preprocess_data_orders(input_file, output_file):
    """
    Обработка данных.

    Функция выполняет очистку и преобразование данных:
    1. Преобразует столбец с датами в формат datetime.
    2. Добавляет новые метрики:
        - месяц визита.
        - день недели.

    Аргументы:
        input_file (str): Путь к входному CSV-файлу с необработанными данными.
        output_file (str): Путь к выходному CSV-файлу для сохранения обработанных данных.

    Логика работы:
    1. Чтение данных из файла.
    2. Очистка и преобразование данных.
    3. Добавление новых метрик для дальнейшего анализа.
    4. Сохранение данных в новый файл.
    """
    # Чтение данных из входного файла
    df = pd.read_csv(input_file)

    
    # Используем 'Int64', 'Float' для поддержки NaN значений
    df['product_id'] = pd.to_numeric(df['product_id'], errors='coerce')
    df['order_id']   = pd.to_numeric(df['order_id'], errors='coerce')
    df['user_id']    = pd.to_numeric(df['user_id'], errors='coerce')
    df['revenue']    = pd.to_numeric(df['revenue'], errors='coerce')

    # Преобразование столбца с датами в формат datetime
    df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d %H:%M:%S')


    # Добавление новых метрик
    df['month'] = df['datetime'].dt.month  # Месяц визита
    df['weekday'] = df['datetime'].dt.day_name()  # День недели визита
    
    #среднее количество визитов по месяцам
    df['average_revenue_for_month'] = df.groupby('month')['revenue'].transform('mean')


    # Словарь с соответствием старых и новых названий столбцов
    column_mapping = {
        'datetime': 'datetime',
        'user_id': 'user_id',
        'revenue': 'revenue',
        'product_id': 'product_id',
        'order_id': 'order_id',
        'month': 'month',
        'weekday': 'weekday',
        'average_revenue_for_month': 'average_revenue_for_month'
    }

    # Переименование столбцов
    df = df.rename(columns=column_mapping)

    # Удаление существующего csv-файла
    if os.path.exists(output_file):
            os.remove(output_file)
            print(f"Существующий файл {output_file} удален.")

    # Сохранение обработанных данных в выходной файл
    df.to_csv(output_file, index=False)


def check_processed_events(output_file):
    """
    Проверка преобразованного файла.

    Функция выполняет несколько проверок преобразованного файла:
    1. Проверяет, существует ли файл по указанному пути.
    2. Проверяет, что файл не пустой.
    3. Проверяет наличие обязательных столбцов, которые должны быть добавлены в ходе обработки данных.

    Аргументы:
        output_file (str): Путь к преобразованному CSV-файлу.

    Исключения:
        - FileNotFoundError, если файл не найден.
        - ValueError, если файл пуст или не содержит обязательных столбцов.
    """
    
    # Проверка существования файла
    if not os.path.exists(output_file):
        raise FileNotFoundError(f"Преобразованный файл {output_file} не найден.")
    
    # Чтение данных из преобразованного файла
    df = pd.read_csv(output_file)

    # Проверка, что файл не пуст
    if df.empty:
        raise ValueError("Преобразованный файл пустой.")
    
    # Проверка наличия обязательных столбцов
    if ('month' not in df.columns or 'weekday' not in df.columns):
        raise ValueError("В преобразованном файле не хватает обязательных столбцов 'month', 'weekday'.")


def check_processed_orders(output_file):
    """
    Проверка преобразованного файла.

    Функция выполняет несколько проверок преобразованного файла:
    1. Проверяет, существует ли файл по указанному пути.
    2. Проверяет, что файл не пустой.
    3. Проверяет наличие обязательных столбцов, которые должны быть добавлены в ходе обработки данных.

    Аргументы:
        output_file (str): Путь к преобразованному CSV-файлу.

    Исключения:
        - FileNotFoundError, если файл не найден.
        - ValueError, если файл пуст или не содержит обязательных столбцов.
    """
    
    # Проверка существования файла
    if not os.path.exists(output_file):
        raise FileNotFoundError(f"Преобразованный файл {output_file} не найден.")
    
    # Чтение данных из преобразованного файла
    df = pd.read_csv(output_file)

    # Проверка, что файл не пуст
    if df.empty:
        raise ValueError("Преобразованный файл пустой.")
    
    # Проверка наличия обязательных столбцов
    if ('month' not in df.columns or 'weekday' not in df.columns):
        raise ValueError("В преобразованном файле не хватает обязательных столбцов 'month',	'weekday',	'average_revenue_for_month'.")


def init_db(db_path):
    """
    Инициализация базы данных SQLite.

    Функция выполняет следующие действия:
    1. Создает соединение с базой данных по указанному пути.
    2. Создает таблицу в базе данных, если она не существует.
    3. Выполняет команду для создания таблицы с определенной схемой.
    4. Закрывает соединение с базой данных после выполнения операции.

    Аргументы:
        db_path (str): Путь к базе данных SQLite, куда будет создана таблица.
    """
    try:
        # Устанавливаем соединение с базой данных SQLite по указанному пути
        conn = sqlite3.connect(db_path)

        # Создаем курсор для выполнения SQL-запросов
        cursor = conn.cursor()

        cursor.execute('''
            DROP TABLE IF EXISTS events;
        ''')

        cursor.execute('''
            DROP TABLE IF EXISTS orders;
        ''')

        # SQL-запрос для создания таблицы, если она не существует
        cursor.execute('''
            CREATE TABLE events (
                datetime DATE NOT NULL,  -- Интервал дат визита
                user_id INTEGER NOT NULL,  --id пользователя
                event_name TEXT NOT NULL, --наименование событий
                page TEXT NOT NULL, --наименование страцицы
                product_id INTEGER,  --id заказа
                order_id INTEGER,  --id заказа
                month INTEGER NOT NULL,  --номер мусяца
                weekday TEXT NOT NULL --день недели
            );
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS orders (
                datetime DATE NOT NULL,  -- Интервал дат визита
                user_id INTEGER NOT NULL,  --id пользователя
                product_id NTEGER NOT NULL,  --id продукта
                order_id INTEGER NOT NULL,  --id заказа
                month INTEGER NOT NULL,  --номер мусяца
                weekday TEXT NOT NULL, --день недели
                revenue  FLOAT  NOT NULL, --выручка  
                average_revenue_for_month FLOAT  NOT NULL --выручка
            );
            ''')

    # Чтение и загрузка данных из events.csv
        df_events = pd.read_csv(OUTPUT_NEW_EVENTS)
        df_events.to_sql('events', conn, if_exists='append', index=False)
        print("Данные из events.csv успешно загружены в таблицу events.")
            
    # Чтение и загрузка данных из orders.csv
        df_orders = pd.read_csv(OUTPUT_NEW_ORDERS)
        df_orders.to_sql('orders', conn, if_exists='append', index=False)
        print("Данные из new_orders.csv успешно загружены в таблицу orders.")
            
    # Сохранение изменений и закрытие соединения
        conn.commit()
        conn.close()
    except Exception as e:
        raise ValueError(f"Ошибка при загрузке данных в SQLite: {e}")


def validate_loaded_data():
    """
    Проверяет качество данных после загрузки в SQLite.
    Включает проверки на количество строк, дубликаты, пропущенные значения и диапазоны значений.
    """
    try:
        # Подключение к базе данных
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 1. Проверка количества строк
        def check_row_count(table_name, csv_file):
            # Число строк в таблице SQLite
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            db_row_count = cursor.fetchone()[0]

            # Число строк в CSV-файле
            df_csv = pd.read_csv(csv_file)
            csv_row_count = len(df_csv)

            if db_row_count != csv_row_count:
                raise ValueError(f"Несоответствие количества строк в {table_name}. В CSV: {csv_row_count}, в DB: {db_row_count}")

        check_row_count('events', OUTPUT_NEW_EVENTS)
        check_row_count('orders', OUTPUT_NEW_ORDERS)

        print("Проверка количества строк успешно пройдена.")

        # 2. Проверка на наличие дубликатов
        def check_duplicates(table_name, primary_key):
            cursor.execute(f"SELECT {primary_key}, COUNT(*) FROM {table_name} GROUP BY {primary_key} HAVING COUNT(*) > 1")
            duplicates = cursor.fetchall()
            if duplicates:
                raise ValueError(f"Обнаружены дубликаты в {table_name}: {duplicates}")

        #check_duplicates('events', 'user_id')
        check_duplicates('orders', 'order_id')

        print("Проверка на дубликаты успешно пройдена.")

        # 3. Проверка на пропущенные значения
        def check_missing_values(table_name, columns):
            for column in columns:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {column} IS NULL")
                missing_count = cursor.fetchone()[0]
                if missing_count > 0:
                    raise ValueError(f"Обнаружены пропущенные значения в столбце {column} таблицы {table_name}")

        check_missing_values('events', ['datetime','user_id','event_name','page','month','weekday'])
        check_missing_values('orders', ['datetime','user_id','product_id','order_id','revenue','month','weekday','average_revenue_for_month'])

        print("Проверка на пропущенные значения успешно пройдена.")


        print("Все проверки качества данных успешно завершены.")
    except Exception as e:
        raise ValueError(f"Ошибка при проверке качества данных: {e}")
    finally:
        conn.close()


def check_table_events(db_path):
    """Функция для выполнения SELECT запроса и проверки таблицы."""
    
    # Подключаемся к базе данных SQLite по указанному пути
    conn = sqlite3.connect(db_path)
    
    # Создаем курсор для выполнения SQL-запросов
    cursor = conn.cursor()

    # Выполнение SELECT запроса, чтобы получить количество строк в таблице 'events'
    cursor.execute("SELECT COUNT(*) FROM events;")
    
    # Получаем результат запроса, который возвращает кортеж с числом строк в таблице
    row_count = cursor.fetchone()[0]  # Получаем количество строк в таблице

    # Проверка, если таблица пуста (нет данных)
    if row_count == 0:
        # Если таблица пуста, выбрасываем исключение с соответствующим сообщением
        raise ValueError("Таблица 'events' пуста.")

    # Выполняем дополнительный запрос для получения информации о столбцах таблицы
    cursor.execute("PRAGMA table_info(events);")
    
    # Извлекаем имена всех столбцов из результатов запроса
    columns = [column[1] for column in cursor.fetchall()]  # Получаем имена столбцов

    # Список обязательных столбцов, которые должны присутствовать в таблице
    required_columns = ['datetime','user_id','event_name','page','product_id','order_id','month','weekday']  

    # Проверка на отсутствие обязательных столбцов
    missing_columns = [col for col in required_columns if col not in columns]

    # Если какие-либо обязательные столбцы отсутствуют, выбрасываем исключение с их списком
    if missing_columns:
        raise ValueError(f"Отсутствуют обязательные столбцы: {', '.join(missing_columns)}")

    # Закрываем соединение с базой данных
    conn.close()


def check_table_orders(db_path):
    """Функция для выполнения SELECT запроса и проверки таблицы."""
    
    # Подключаемся к базе данных SQLite по указанному пути
    conn = sqlite3.connect(db_path)
    
    # Создаем курсор для выполнения SQL-запросов
    cursor = conn.cursor()

    # Выполнение SELECT запроса, чтобы получить количество строк в таблице 'events'
    cursor.execute("SELECT COUNT(*) FROM orders;")
    
    # Получаем результат запроса, который возвращает кортеж с числом строк в таблице
    row_count = cursor.fetchone()[0]  # Получаем количество строк в таблице

    # Проверка, если таблица пуста (нет данных)
    if row_count == 0:
        # Если таблица пуста, выбрасываем исключение с соответствующим сообщением
        raise ValueError("Таблица 'orders' пуста.")

    # Выполняем дополнительный запрос для получения информации о столбцах таблицы
    cursor.execute("PRAGMA table_info(orders);")
    
    # Извлекаем имена всех столбцов из результатов запроса
    columns = [column[1] for column in cursor.fetchall()]  # Получаем имена столбцов

    # Список обязательных столбцов, которые должны присутствовать в таблице
    required_columns = ['datetime','user_id','product_id','order_id','revenue','month','weekday','average_revenue_for_month']

      

    # Проверка на отсутствие обязательных столбцов
    missing_columns = [col for col in required_columns if col not in columns]

    # Если какие-либо обязательные столбцы отсутствуют, выбрасываем исключение с их списком
    if missing_columns:
        raise ValueError(f"Отсутствуют обязательные столбцы: {', '.join(missing_columns)}")

    # Закрываем соединение с базой данных
    conn.close()


def aggregated_data(db_path):
    """
    Функция объединяет данные из таблиц events и orders
    """
    # Подключение к базе данных SQLite
    conn = sqlite3.connect(db_path)

    # SQL-запрос для объединения данных из таблиц events и orders

    query = '''
        SELECT 
            e.user_id AS ID_пользователя,                       -- ID_пользователя
            e.datetime AS Дата_посещения_сайта,                 -- Дата посещения сайта
            e.weekday AS День_недели,                           -- День недели
            e.month AS Месяц,                                   -- Номер месяца
            e.event_name AS Наименование_событий,               -- Наименование событий
            e.page AS Наименование_страницы,                    -- Наименование страницы
            o.revenue AS Выручка,                               -- Выручка
            o.average_revenue_for_month AS Средняя_выручка     -- Средняя выручка
        FROM events AS e
        LEFT JOIN orders AS o
        ON e.order_id = o.order_id;
        
    '''

    # Выполнение запроса и загрузка результатов в DataFrame
    results = pd.read_sql(query, conn)

    # Закрытие соединения с базой данных
    conn.close()

    # Возвращаем результаты анализа
    return results

def save_to_csv(output, db_path):
    """
    Удаление и сохранение результатов в файл CSV.
    """
    # Шаг 1: Удаление существующего csv-файла
    if os.path.exists(output):
            os.remove(output)
            print(f"Существующий файл {output} удален.")

    # Шаг 2: Выполнение всех аналитических функций
    aggregated_data_new = aggregated_data(db_path)  # Объединение двух таблиц: events и orders

    # Шаг 3: Создание файла CSV
    aggregated_data_new.to_csv(output, index=False)

    print("Данные успешно сохранены в final_aggregated.csv")


def conversion_total(db_path):
    """
    Общая конверсия
    """
    # Подключение к базе данных SQLite
    conn = sqlite3.connect(db_path)
    
    funnel_query = '''
            -- 1. Подсчет уникальных пользователей на каждом этапе воронки
            WITH funnel_steps AS (
                SELECT
                    COUNT(DISTINCT CASE WHEN event_name = 'view' THEN user_id END) AS unique_views,
                    COUNT(DISTINCT CASE WHEN event_name = 'click' THEN user_id END) AS unique_clicks,
                    COUNT(DISTINCT CASE WHEN event_name = 'add' THEN user_id END) AS unique_adds,
                    COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN user_id END) AS unique_purchases
                FROM events
            )

            -- 2. Расчет итоговых показателей и конверсии на основе данных из CTE
            SELECT
                unique_views AS Количество_уникальных_просмотров,
                unique_clicks AS Количество_уникальных_кликов,
                unique_adds AS Количество_уникальных_добавлений,
                unique_purchases AS Количество_уникальных_покупок,

                -- Конверсия из просмотров в клики (CTR)
                ROUND(
                    (unique_clicks * 100.0) / NULLIF(unique_views, 0),
                    2
                ) AS Конверсия_кликов_к_просмотрам,

                -- Конверсия из кликов в добавления в корзину
                ROUND(
                    (unique_adds * 100.0) / NULLIF(unique_clicks, 0),
                    2
                ) AS Конверсия_добавления_в_корзину_к_кликам,

                -- Конверсия из добавлений в корзину в покупки
                ROUND(
                    (unique_purchases * 100.0) / NULLIF(unique_adds, 0),
                    2
                ) AS Конверсия_покупок_к_добавлению_в_корзину,

                -- Общая конверсия: Покупки / Просмотры
                ROUND(
                    (unique_purchases * 100.0) / NULLIF(unique_views, 0),
                    2
                ) AS Конверсия_покупок_к_просмотрам

            FROM funnel_steps;
                    
        '''
   

    # Выполнение запроса и загрузка результатов в DataFrame
    results = pd.read_sql(funnel_query, conn)

    # Закрытие соединения с базой данных
    conn.close()

    # Возвращаем результаты анализа
    return results


def conversion_user_id(db_path):
    """
    Воронка конверсии (Funnel) по дням
    """
    # Подключение к базе данных SQLite
    conn = sqlite3.connect(db_path)
    
    funnel_query = '''
            WITH user_events AS (
            -- Подсчитываем количество каждого типа события для каждого пользователя
            SELECT
                user_id,
                COUNT(CASE WHEN event_name = 'view' THEN 1 END) AS views,
                COUNT(CASE WHEN event_name = 'click' THEN 1 END) AS clicks,
                COUNT(CASE WHEN event_name = 'add' THEN 1 END) AS adds,
                COUNT(CASE WHEN event_name = 'purchase' THEN 1 END) AS purchases
            FROM events
            GROUP BY user_id
        )
        SELECT
            user_id AS ID_пользователя,
            views AS Количество_просмотров,
            clicks AS Колчество_кликов,
            adds AS Колчество_добавлений_в_корзину,
            purchases AS Количество_покупок,

            -- Конверсия: Клики / Просмотры
            ROUND(
                CASE 
                    WHEN views > 0 THEN (clicks * 1.0 / views)
                    ELSE NULL 
                END * 100, 
                2
            ) AS Конверсия_кликов_к_просмотрам,

            -- Конверсия: Добавления в корзину / Клики
            ROUND(
                CASE 
                    WHEN clicks > 0 THEN (adds * 1.0 / clicks)
                    ELSE NULL 
                END * 100, 
                2
            ) AS Конверсия_добавлений_в_корзину_к_кликам,

            -- Конверсия: Покупки / Добавления в корзину
            ROUND(
                CASE 
                    WHEN adds > 0 THEN (purchases * 1.0 / adds)
                    ELSE NULL 
                END * 100, 
                2
            ) AS Конверсия_покупок_к_добавлению_в_корзину,

            -- Общая конверсия в покупку: Покупки / Просмотры
            ROUND(
                CASE 
                    WHEN views > 0 THEN (purchases * 1.0 / views)
                    ELSE NULL 
                END * 100, 
                2
            ) AS Конверсия_покупок_к_просмотрам

        FROM user_events
        ORDER BY Конверсия_покупок_к_просмотрам DESC NULLS LAST;

'''

  # Выполнение запроса и загрузка результатов в DataFrame
    results = pd.read_sql(funnel_query, conn)

    # Закрытие соединения с базой данных
    conn.close()

    # Возвращаем результаты анализа
    return results


def revenue_week(db_path):
    """
    Рассчитывает еженедельную выручку накопительным итогом для базы данных SQLite.
    """
    # Подключение к базе данных SQLite
    conn = sqlite3.connect(db_path)
    
    query = '''
        WITH weekly_revenue AS (
            -- Шаг 1: Группируем данные по году и номеру недели
            -- Используем функции SQLite: strftime('%Y', date) и strftime('%W', date)
            -- '%W' возвращает номер недели (00-53), где понедельник — первый день недели.
            SELECT 
                strftime('%Y', e.datetime) AS year_number,
                strftime('%m', e.datetime) AS month_number,
                strftime('%W', e.datetime) AS week_number,
                SUM(o.revenue) AS total_weekly_revenue
            FROM events AS e
            -- Используем INNER JOIN, так как ищем только покупки с заказами
            LEFT JOIN orders AS o ON e.order_id = o.order_id          
            WHERE e.event_name = 'purchase'
              AND o.revenue IS NOT NULL
            GROUP BY year_number, week_number
            -- ORDER BY внутри CTE не обязателен для финального результата
        )
        -- Шаг 2: Расчёт накопительного итога
        SELECT
            year_number AS Год,
            month_number AS Месяц,
            week_number AS Неделя,
            total_weekly_revenue AS Еженедельная_выручка,
            SUM(total_weekly_revenue) OVER (
                ORDER BY year_number, week_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS Еженедельная_накопленная_выручка
        FROM weekly_revenue;
    '''

    # Выполнение запроса и загрузка результатов в DataFrame
    results = pd.read_sql_query(query, conn) # Используем read_sql_query для явности

    # Закрытие соединения с базой данных
    conn.close()

    # Возвращаем результаты анализа
    return results

def user_behavior(db_path):
    """
    Анализирует популярные страницы.
    """
    # Подключение к базе данных SQLite
    conn = sqlite3.connect(db_path)
    
    # ИСПРАВЛЕННЫЙ ЗАПРОС ДЛЯ SQLITE
    query = '''

            SELECT e.page AS Наименование_страницы, COUNT(*) AS Количество_просмотров_страницы_сайта
            FROM events AS e
            LEFT JOIN orders AS o
            ON e.order_id = o.order_id
            WHERE e.event_name = 'view'
            GROUP BY e.page
            ORDER BY Количество_просмотров_страницы_сайта DESC;

'''
    # Выполнение запроса и загрузка результатов в DataFrame
    results = pd.read_sql_query(query, conn) # Используем read_sql_query для явности

    # Закрытие соединения с базой данных
    conn.close()

    # Возвращаем результаты анализа
    return results


def save_all_analysis_to_excel(output):
    """
    Сохраняет результаты всех аналитических функций в один файл Excel на разные листы.
    """
    # Шаг 1: Удаление существующего xlsx-файла
    if os.path.exists(output):
            os.remove(output)
            print(f"Существующий файл {output} удален.")

    # Шаг 2: Выполнение всех аналитических функций
    conversion = conversion_total(db_path) # Конверсия всех пользователей
    conversion_user = conversion_user_id(db_path) # Конверсия каждого пользователя
    total_revenue_week = revenue_week(db_path) # Еженедельная выручка
    behavior = user_behavior(db_path) # Анализ поведения пользователей

    # Шаг 3: Создание файла Excel
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Запись каждого DataFrame на отдельный лист
        conversion.to_excel(writer, sheet_name='Конверсия по событиям', index=False)
        conversion_user.to_excel(writer, sheet_name='Конверсия по посетителям', index=False)
        total_revenue_week.to_excel(writer, sheet_name='Еженедельная выручка', index=False)
        behavior.to_excel(writer, sheet_name='Поведение пользователей', index=False)

    print("Все аналитические данные успешно сохранены в analytics_vkusvill.xlsx")


# Определяем сам DAG
with DAG(
    'vkusvill_pipeline',  # Название пайплайна
    default_args=default_args,  # Стандартные аргументы
    description='Пайплайн для обработки файла',  # Описание
    schedule_interval=None,  # Пайплайн не будет запускаться по расписанию
    start_date=days_ago(1),  # Начальная дата запуска
    catchup=False,  # Не выполнять пропущенные запуски
) as dag:

    # Пути к файлам
    EVENTS_FILE = '/root/airflow/dags/project_vkusvill/result_analyst/events.csv'
    ORDERS_FILE = '/root/airflow/dags/project_vkusvill/result_analyst/orders.csv'

    OUTPUT_NEW_EVENTS = '/root/airflow/dags/project_vkusvill/result_analyst/new_events.csv'
    OUTPUT_NEW_ORDERS = '/root/airflow/dags/project_vkusvill/result_analyst/new_orders.csv'
    OUTPUT_NEW_FINAL_AGGREGATED = '/root/airflow/dags/project_vkusvill/result_analyst/final_aggregated.csv'
    OUTPUT_EXCEL = '/root/airflow/dags/project_vkusvill/result_analyst/analytics_vkusvill.xlsx'

    db_path = '/root/airflow/dags/project_vkusvill/result_analyst/vkusvill.db'

    # Задача для проверки наличия исходного файла events.csv
    check_file_task_events = PythonOperator(
        task_id='check_file_task_events',
        python_callable=check_file_exists,  # Функция для проверки файла
        op_args=[EVENTS_FILE],  # Параметры для функции
        dag=dag
    )

    # Задача для проверки наличия исходного файла orders.csv
    check_file_task_orders = PythonOperator(
        task_id='check_file_task_orders',
        python_callable=check_file_exists,  # Функция для проверки файла
        op_args=[ORDERS_FILE],  # Параметры для функции
        dag=dag
    )

    # Задача для проверки наличия обязательных столбцов в файле events.csv
    validate_columns_task_events = PythonOperator(
        task_id='validate_columns_task_events',
        python_callable=validate_columns_events,  # Функция для проверки файла
        op_args=[EVENTS_FILE],  # Параметры для функции
        dag=dag
    )

    # Задача для проверки наличия обязательных столбцов в файле orders.csv
    validate_columns_task_orders = PythonOperator(
        task_id='validate_columns_task_orders',
        python_callable=validate_columns_orders,  # Функция для проверки файла
        op_args=[ORDERS_FILE],  # Параметры для функции
        dag=dag
    )

    # Задача для проверки наличия обязательных столбцов в файле events.csv
    validate_task_events = PythonOperator(
        task_id='validate_task_events',
        python_callable=validate_events_file,  # Функция для проверки файла
        op_args=[EVENTS_FILE],  # Параметры для функции
        dag=dag
    )

    # Задача для проверки типов данных в файле orders.csv
    validate_task_orders = PythonOperator(
        task_id='validate_task_orders',
        python_callable=validate_orders_file,  # Функция для проверки файла
        op_args=[ORDERS_FILE],  # Параметры для функции
        dag=dag
    )

    # Задача для проверки структуры и бизнес-правила для файлов events.csv и orders.csv
    validate_structures_task = PythonOperator(
        task_id='validate_structures_task',
        python_callable=validate_data_structures,  # Функция для проверки файла
        op_args=[EVENTS_FILE, ORDERS_FILE],
        dag=dag
    )
    
     # Задача для преобразования типов данных в файле events.csv
    preprocess_events_task = PythonOperator(
        task_id='preprocess_events_task',
        python_callable=preprocess_data_events,  # Функция для проверки файла
        op_args=[EVENTS_FILE, OUTPUT_NEW_EVENTS],
        dag=dag
    )

    # Задача для преобразования типов данных в файле orders.csv
    preprocess_orders_task = PythonOperator(
        task_id='preprocess_orders_task',
        python_callable=preprocess_data_orders,  # Функция для проверки файла
        op_args=[ORDERS_FILE, OUTPUT_NEW_ORDERS],
        dag=dag
    )

    # Задача для проверки преобразования данных в файле events.csv
    check_new_events_task = PythonOperator(
        task_id='check_new_events_task',
        python_callable=check_processed_events,  # Функция для проверки файла
        op_args=[OUTPUT_NEW_EVENTS],
        dag=dag
    )

    # Задача для проверки преобразования данных в файле orders.csv
    check_new_orders_task = PythonOperator(
        task_id='check_new_orders_task',
        python_callable=check_processed_orders,  # Функция для проверки файла
        op_args=[OUTPUT_NEW_ORDERS],
        dag=dag
    )

    # Задача по загрузке данных в базу данных из файлов: new_events.csv, new_orders.csv
    loading_db_task = PythonOperator(
        task_id='loading_db_task',
        python_callable=init_db,  # Функция для проверки данных
        op_args=[db_path],
        dag=dag
    )

    # Задача для проверки данных в базе данных с данными из файлов: new_events.csv, new_orders.csv
    validate_loaded_task = PythonOperator(
        task_id='validate_loaded_task',
        python_callable=validate_loaded_data,  # Функция для проверки данных
        dag=dag
    )

    # Задача для проверки данных в таблице events
    check_table_events_task = PythonOperator(
        task_id='check_table_events_task',
        python_callable=check_table_events,  # Функция для проверки данных
        op_args=[db_path],
        dag=dag
    )

     # Задача для проверки данных в таблице orders
    check_table_orders_task = PythonOperator(
        task_id='check_table_orders_task',
        python_callable=check_table_orders,  # Функция для проверки данных
        op_args=[db_path],
        dag=dag
    )

    # Задача по объединению данных из таблиц events и orders
    aggregated_task = PythonOperator(
        task_id='aggregated_task',
        python_callable=aggregated_data, 
        op_args=[db_path],
        dag=dag
    )

    # Задача по сохранению объединеных данных из таблиц events и orders в файл final_aggregated.csv
    save_to_csv_task = PythonOperator(
        task_id='save_to_csv_task',
        python_callable=save_to_csv, 
        op_args=[OUTPUT_NEW_FINAL_AGGREGATED, db_path],
        dag=dag
    )

    # Задача по расчету конверсии по всем посетителям
    conversion_total_task = PythonOperator(
        task_id='conversion_total_task',
        python_callable=conversion_total, 
        op_args=[db_path],
        dag=dag
    )

    # Задача по расчету конверсии по каждому посетителю
    conversion_user_id_task = PythonOperator(
        task_id='conversion_user_id_task',
        python_callable=conversion_user_id,  
        op_args=[db_path],
        dag=dag
    )

    # Задача по расчету еженедельной выручки накопительным итогом
    revenue_week_task = PythonOperator(
        task_id='revenue_week_task',
        python_callable=revenue_week,  
        op_args=[db_path],
        dag=dag
    )

    # Задача по анализу популярных страниц
    user_behavior_task = PythonOperator(
        task_id='user_behavior_task',
        python_callable=user_behavior,  
        op_args=[db_path],
        dag=dag
    )

    # Задача по сохранению результатов всех аналитических функций в один файл Excel на разные листы
    save_to_excel_task = PythonOperator(
        task_id='save_to_excel_task',
        python_callable=save_all_analysis_to_excel,  
        op_args=[OUTPUT_EXCEL],
        dag=dag
    )


    # Последовательность выполнения задач
    [check_file_task_events, check_file_task_orders] >> validate_columns_task_events \
     >> validate_columns_task_orders >> [validate_task_events,  validate_task_orders] \
     >> validate_structures_task >> [preprocess_events_task, preprocess_orders_task] \
     >> check_new_events_task >> check_new_orders_task >> loading_db_task >> validate_loaded_task \
     >> [check_table_events_task, check_table_orders_task] >> aggregated_task  >> save_to_csv_task \
     >> [conversion_total_task, conversion_user_id_task, revenue_week_task, user_behavior_task] \
     >>  save_to_excel_task      


