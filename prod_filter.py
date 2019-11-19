# Напишите программу, которая выводит средний рейтинги всех продуктов из "prodname_avg_rating.csv",
# в названии которых встречается введенное при запуске слово: ProdId,Name,Rating

# Запуск
# spark-submit prod_filter.py

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Настройка конфигурации
conf = SparkConf().setAppName("ProdNameAvgRatingSearch")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


schema_new = StructType([StructField(name="id", dataType=StringType(), nullable=False),
                         StructField(name="title", dataType=StringType(), nullable=True),
                         StructField("rating", DoubleType(), True)])
# Входной файл
ratings = sqlContext.read.load(path='out/prod_name_rating/part-*.csv', format="csv", schema=schema_new, header="false",
                               inferSchema="false", sep=",", nullValue="null", mode="DROPMALFORMED")

# Ищем слово и выводим
search_string = 'Barnes'
result = ratings.filter(ratings.title.like('%' + search_string + '%'))

result.show()
