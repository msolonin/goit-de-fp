# Проект складаеться з 2-х частин

# 1
- Для запуску першої частини треба спочатку запустити generate_data_kafka_stream_hw_1.py, 
цей скрипт буде зчитувати дані з бази таблиці athlete_event_results та писати в топік athletes_msolonin

- Потім запустити скрипт hw_part_1.py котрий виконае 6 кроків першої частини дз

# 2
- Друга частина складаеться з 3-х файлів котрі запускаютьсь один за одним: landing_to_bronze.py, bronze_to_silver.py, silver_to_gold.py.

- Або project_solution.py як DAG в Airflow
