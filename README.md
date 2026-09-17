# Big Data Lab 5 — Настройка Spark и модель кластеризации (PySpark)

Цель работы: развернуть и проверить окружение для Spark-вычислений и разработать на PySpark модель кластеризации на базе алгоритма K-средних. Источники данных (СУБД, витрины) в этой лабораторной не используются — вся работа идёт с локальным файлом датасета.

1. **WordCount** (`workspace/word_count.py`) — проверочный пример, подтверждающий работоспособность компонентов Spark-платформы.
2. **Seeder** (`workspace/download_data.py`) — скачивает датасет Open Food Facts и сохраняет его локально в `workspace/openfoodfacts.csv.gz`.
3. **Модель** (`workspace/main.py`, `workspace/model.py`) — читает CSV, отбирает нужные колонки, приводит типы, сэмплирует данные под доступные системные ресурсы, собирает и масштабирует признаки (`VectorAssembler` + `StandardScaler`), обучает K-Means (`k=5`), оценивает качество кластеризации (silhouette score) и сохраняет результат локально в `workspace/clustering_results`.

---

## Состав репозитория

| Файл/папка | Назначение |
|---|---|
| `docker-compose.yaml` | Локальное окружение: один PySpark-контейнер |
| `Dockerfile` | Образ PySpark-окружения |
| `workspace/word_count.py` | Проверочный WordCount-пример |
| `workspace/download_data.py` | Скачивание датасета Open Food Facts |
| `workspace/main.py`, `workspace/model.py` | ML-пайплайн кластеризации (K-Means) |
| `workspace/spark_manager.py`, `workspace/spark_config.yaml` | Управление Spark-сессией по именованному профилю |
| `workspace/config.py` | Загрузка Spark-конфигурации из YAML |

---

## 1. Поднять окружение

Из корня репозитория:

```powershell
docker compose up -d --build
```

Поднимется один контейнер — `spark-pure-container` (`./workspace` смонтирован в `/workspace`).

---

## 2. Проверить работоспособность Spark (WordCount)

```powershell
docker exec -it spark-pure-container python word_count.py
```

Скрипт создаёт `SparkSession`, считает слова в тестовом наборе строк через RDD `map`/`reduceByKey` и печатает результат в лог.

---

## 3. Скачать датасет

```powershell
docker exec -it spark-pure-container python download_data.py
```

Скачивает архив `openfoodfacts.csv.gz` (~1.2 ГБ, может занять несколько минут). Повторные запуски не скачивают файл заново — он закэширован в `./workspace`.

---

## 4. Запустить модель кластеризации

```powershell
docker exec -it spark-pure-container python main.py
```

Пайплайн читает `openfoodfacts.csv.gz`, отбирает и очищает колонки (`product_name`, `energy-kcal_100g`, `proteins_100g`, `fat_100g`, `carbohydrates_100g`), берёт 5%-сэмпл, обучает K-Means и логирует silhouette score. Результат (`id`, `product_name`, `cluster_label`) сохраняется в `workspace/clustering_results/*.csv`.

---

## 5. Повторный прогон / остановка

Модель пишет результат с `mode="overwrite"`, поэтому шаг 4 можно просто повторить.

Остановить окружение:

```powershell
docker compose down
```
