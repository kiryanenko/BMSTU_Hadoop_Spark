# Задача 4. Подсчет количества твитов пользователей

Напишите программу, которая подсчитывает количество твитов каждого пользователя в течение 1 мин. 
и в течение 10 мин. каждые 30 сек (window). Выведите результат в отсортированном по убыванию виде. 
В списке id пользователей заменить на их screen_name.

## Установка zookeeper и kafka

```
brew install zookeeper
brew install kafka
```

## Запуск

1. Запуск zookeeper и kafka
    ```
    zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties
    kafka-server-start /usr/local/etc/kafka/server.properties
    ```
3. Создание kafka topic
    ```
    kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic tweets
    ```
3. Запуск программы
    ```
    python tweets-kafka.py
    spark-submit --jars ../jars/spark-streaming-kafka-0-8-assembly_2.11-2.4.4.jar tweets-spark.py
    ```
    
## Результаты

```
(BMSTU_Hadoop_Spark) $ python tweets-kafka.py 
Time=Sun Dec 15 21:20:44 +0000 2019     ID=200036850    screen_name=rentvchannel
Time=Sun Dec 15 21:22:21 +0000 2019     ID=72525490     screen_name=vesti_news
Time=Sun Dec 15 21:30:44 +0000 2019     ID=200036850    screen_name=rentvchannel
Time=Sun Dec 15 21:34:25 +0000 2019     ID=285532415    screen_name=tass_agency
Time=Sun Dec 15 21:37:21 +0000 2019     ID=72525490     screen_name=vesti_news
```

```

-------------------------------------------
Time: 2019-12-16 00:37:30
-------------------------------------------
('vesti_news', 1)

-------------------------------------------
Time: 2019-12-16 00:37:30
-------------------------------------------
('tass_agency', 1)
('vesti_news', 1)
('rentvchannel', 2)

```
