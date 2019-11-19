# Напишите программу, которая каждому ProdId из "avg_rating.csv" ставит в соответстие названием продукта.
# Результат сохранить в HDFS в файле "prodname_avg_rating.csv": ProdId,Name,Rating

# Запуск
# spark-submit prod_name_rating.py

import json

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Настройка конфигурации
conf = SparkConf().setAppName("ProdNameAvgRating")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# Загружаю названия продуктов
prod_info = sc.textFile('data/sample_100_meta.json')


def get_prod_title(line):
    # Заменяю одинарные кавычки двойными
    line = line.replace("'", '"')
    try:
        prod = json.loads(line)
        return prod['asin'], prod['title']
    except ValueError:
        return None


columns = ['id', 'title']
# Достаю названия продуктов
prod_names = prod_info.map(get_prod_title).filter(lambda x: x).toDF(columns)

schema = StructType([StructField(name="id", dataType=StringType(), nullable=False),
                     StructField("rating", DoubleType(), True)])
# Загружаю рейтинги продуктов
ratings = sqlContext.read.load(path='out/avg_rating/part-*.csv', format="csv", schema=schema, header="false",
                               inferSchema="false", sep=",", nullValue="null", mode="DROPMALFORMED")

# Объединяем данные
join_df = prod_names.join(ratings, ['id'], how='full')

# Сохраняем файл в csv
join_df.coalesce(1).write.format("csv").option("header", "false").save('out/prod_name_rating')
