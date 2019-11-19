# Реализуйте подсчет среднего рейтинга продуктов. Результат сохранить в HDFS в файле "avg_rating.csv".
# Формат каждой записи: ProdId,Rating

# Запуск
# spark-submit avg_rating.py

import json

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Настройка конфигурации
conf = SparkConf().setAppName("AvgRating")
sc = SparkContext(conf=conf)

# Входной файл
lines = sc.textFile('data/samples_100.json')


# Десериализую в JSON и вытаскиваю оценку
def review_rating(review_json_item):
    dict_review_item = json.loads(review_json_item)
    return dict_review_item['asin'], dict_review_item['overall']


ratings = lines.map(review_rating)
# Подсчитываю суммарный рейтинг и количество, после считаю средний рейтинг
avg_ratings = ratings \
    .aggregateByKey((0, 0),
                    lambda x, value: (x[0] + value, x[1] + 1),
                    lambda x, y: (x[0] + y[0], x[1] + y[1])) \
    .mapValues(lambda x: x[0] / x[1])

# Делаем схему
sqlContext = SQLContext(sc)
schema = StructType([StructField(name='id', dataType=StringType(), nullable=False),
                     StructField('avg_rating', DoubleType(), True)])

# Сохраняем файл в csv
df_data = sqlContext.createDataFrame(avg_ratings, schema)
df_data.coalesce(1).write.format('csv').option('header', 'false').save('out/avg_rating/')
