# -*- coding: utf-8 -*-
"""
Created on Sun Jun 22 13:20:41 2025

@author: Анастасия Рахманина
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import sqlite3

# Конфигурация DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2015, 1, 1),
    'retries': 1
}

# Пути к файлам 
CSV_PATHS = {
    'booking': 'C:/Users/Анастасия Рахманина/airflow/data/booking.csv',
    'client': 'C:/Users/Анастасия Рахманина/airflow/data/client.csv',
    'hotel': 'C:/Users/Анастасия Рахманина/airflow/data/hotel.csv'
}

# Оператор извлечения данных
def extract_data(file_type):
    df = pd.read_csv(CSV_PATHS[file_type])
    return df

# Оператор трансформации
def transform_data(**kwargs):
    ti = kwargs['ti']
    
    # Получаем данные
    booking = ti.xcom_pull(task_ids='extract_booking')
    client = ti.xcom_pull(task_ids='extract_client')
    hotel = ti.xcom_pull(task_ids='extract_hotel')
    
    # Объединяем таблицы
    merged = pd.merge(
        pd.merge(booking, client, on='client_id'),
        hotel,
        on='hotel_id'
    )
    
    # Конвертация валют (GBP/EUR → USD)
    exchange_rates = {'GBP': 1.27, 'EUR': 1.08}
    merged['price_usd'] = merged.apply(
        lambda x: x['price'] * exchange_rates[x['currency']],
        axis=1
    )
    
    # Удаляем ненужные колонки
    cols_to_drop = ['price', 'currency']
    merged = merged.drop(columns=[col for col in cols_to_drop if col in merged.columns])
    
    # Преобразование дат
    date_cols = ['booking_date']
    for col in date_cols:
        if col in merged.columns:
            merged[col] = pd.to_datetime(merged[col])
    
    return merged

# Оператор загрузки
def load_to_db(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform_data')
    
    conn = sqlite3.connect('bookings.db')
    data.to_sql('transformed_bookings', conn, if_exists='replace', index=False)
    conn.close()


with DAG(
    'booking_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    
    extract_booking = PythonOperator(
        task_id='extract_booking',
        python_callable=extract_data,
        op_kwargs={'file_type': 'booking'}
    )
    
    extract_client = PythonOperator(
        task_id='extract_client',
        python_callable=extract_data,
        op_kwargs={'file_type': 'client'}
    )
    
    extract_hotel = PythonOperator(
        task_id='extract_hotel',
        python_callable=extract_data,
        op_kwargs={'file_type': 'hotel'}
    )
    
    
    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )
    
    
    load = PythonOperator(
        task_id='load_to_db',
        python_callable=load_to_db,
        provide_context=True
    )
    
    
    [extract_booking, extract_client, extract_hotel] >> transform >> load