# -*- coding: utf-8 -*-
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# spark-submit --jars ../jars/spark-streaming-kafka-0-8-assembly_2.11-2.4.4.jar spark.py

SPARK_APP_NAME = "KafkaTwitterMsgCount"
SPARK_CHECKPOINT_TMP_DIR = "tmp"
SPARK_BATCH_INTERVAL = 30
SPARK_LOG_LEVEL = "OFF"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "tweets"

# Создание спарк контекста
sc = SparkContext(appName=SPARK_APP_NAME)
# Включаем логирование
sc.setLogLevel(SPARK_LOG_LEVEL)
# Создание объекта SparkStreaming
ssc = StreamingContext(sc, SPARK_BATCH_INTERVAL)
# Используется для обновления результата. Чекпоинт каждый batch интервал
ssc.checkpoint(SPARK_CHECKPOINT_TMP_DIR)

# Создаем consumer-а
kafka_stream = KafkaUtils.createDirectStream(ssc, topics=[KAFKA_TOPIC],
                                             kafkaParams={"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

# Выставляю начальные количества
counts = kafka_stream.mapValues(lambda cnt: 1 if not cnt else int(cnt))
# с помощью reduce подсчитываем число твитов пользователя за 1 минуту каждые 0 секунд
counts_1_min = counts.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y,
                                           windowDuration=60, slideDuration=30)
# за 10 минут каждые 30 секунд
counts_10_min = counts.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y,
                                            windowDuration=600, slideDuration=30)

# Сортируем результат по убыванию для соответствующих окон
counts_1_min = counts_1_min.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))
counts_10_min = counts_10_min.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

# Вывод результатов
counts_1_min.pprint()
counts_10_min.pprint()

# Запускаем спарк стрим
ssc.start()
# Ожидаем завершения
ssc.awaitTermination()
