# -*- coding: utf-8 -*-
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import *
from pyspark import sql


# spark-submit --jars ../jars/spark-streaming-kafka-0-8-assembly_2.11-2.4.4.jar spark.py

SPARK_APP_NAME_COUNT = "KafkaTwitterCount"
SPARK_CHECKPOINT_TMP_DIR = "tmp/spark_streaming"
SPARK_BATCH_INTERVAL = 60
SPARK_LOG_LEVEL = "OFF"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "tweets"


field = [
    StructField("id", StringType(), False),
    StructField("screen_name", StringType(), False),
    StructField("text", StringType(), False),
    StructField("retweet_cnt", IntegerType(), False)
]
schema = StructType(field)


# Создание спарк контекст
sc = SparkContext(appName=SPARK_APP_NAME_COUNT)
sqlContext = sql.SQLContext(sc)
# Включаем логирование
sc.setLogLevel(SPARK_LOG_LEVEL)
# Создание SparkStreaming
ssc = StreamingContext(sc, SPARK_BATCH_INTERVAL)
ssc.checkpoint(SPARK_CHECKPOINT_TMP_DIR)

# Создаем стрим, который слушает топик из кафки и возвращает данные для дальнейшей обработки
kafka_stream = KafkaUtils.createDirectStream(ssc, topics=[KAFKA_TOPIC],
                                             kafkaParams={"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
# Дисериализую json
retweets = kafka_stream.mapValues(json.loads)


# Обновляю состояния
def update_state_retweet(new_retweets, prev_state):
    print('update_state_retweet ', new_retweets, prev_state)
    if prev_state is None:
        prev_state = new_retweets[0]
        prev_state['retweet_cnt'] = 0
    prev_state['retweet_cnt'] += len(new_retweets)
    return prev_state


total_counts = retweets.updateStateByKey(update_state_retweet)
# Сортировка
total_counts = total_counts.transform(lambda rdd: rdd.sortBy(lambda raw: raw[1]['retweet_cnt'], ascending=False))
total_counts.pprint()


# Записываем в файл результат, предварительно превратив rdd в dataframe
def save(rdd):
    rdd\
        .map(lambda raw: (raw[0], raw[1]['screen_name'], raw[1]['text'], raw[1]['retweet_cnt']))\
        .toDF(schema).coalesce(1).write\
        .option("header", "false")\
        .format("csv")\
        .mode("overwrite")\
        .save('out/retweets_cnt')


total_counts.foreachRDD(save)
# Запускаем спарк стрим
ssc.start()


# Ожидаем заверешения через 30 минут
ssc.awaitTermination(1800)
# Выгружаем файл, в которой сохраняли данные
total_counts_loaded = sqlContext.read.load(path="out/retweets_cnt", format="csv", header="false",
                                           sep=',', shema=schema)
# Отображаем 5 сообщений с наибольшими ретвитами
total_counts_loaded.show(5)
