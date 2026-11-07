# Используем официальный образ Airflow той же версии, что и в compose-файле
FROM apache/airflow:2.11.1

# Копируем файл с зависимостями внутрь образа
COPY requirements.txt /

# Устанавливаем пакеты от имени пользователя airflow (это важно для безопасности)
# и явно указываем версию apache-airflow, чтобы избежать конфликтов
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt