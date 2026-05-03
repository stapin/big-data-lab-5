# Используем официальный легкий образ Python
FROM python:3.10-slim

# Устанавливаем Java (OpenJDK 17) и wget (для скачивания драйверов)
RUN apt-get update && \
    apt-get install -y default-jre wget && \
    rm -rf /var/lib/apt/lists/*

# Устанавливаем PySpark. Теперь это будет ЕДИНСТВЕННАЯ версия Spark в системе
RUN pip install --no-cache-dir pyspark==3.5.0

# Задаем рабочую директорию
WORKDIR /workspace

# Команда-заглушка, чтобы контейнер работал в фоне, и мы могли зайти в него через VS Code
CMD ["tail", "-f", "/dev/null"]