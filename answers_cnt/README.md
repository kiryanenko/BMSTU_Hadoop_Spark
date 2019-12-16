# Задача 6. Подсчет количества ответов на твиты пользователей

Исходные данные:
- id пользователей: "285532415", "147964447", "34200559", "338960856", "200036850", "72525490", "20510157", "99918629"


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

-------------------------------------------
Time: 2019-12-16 16:17:00
-------------------------------------------
('1206562090858749952', {'id': '1206562090858749952', 'screen_name': 'darchojandreos1', 'text': 'В Донбассе посмертно наградили Георгиевским крестом ДНР четырех российских журналистов https://t.co/r05z87TEC9', 'answers_cnt': 14})
('1206561470194094080', {'id': '1206561470194094080', 'screen_name': 'fjdhdhdcj', 'text': 'В Омске начались слушания по уголовному делу об издевательствах над восьмилетним мальчиком https://t.co/hJZLHGLuPW', 'answers_cnt': 12})
('1206551202399428611', {'id': '1206551202399428611', 'screen_name': 'taticev1', 'text': 'Спикер Госдумы Вячеслав Володин указал на дефицит кадров в медицине:\nhttps://t.co/ufZn6YiFjY https://t.co/HLz0yr0fvS', 'answers_cnt': 12})
('1206554608862146561', {'id': '1206554608862146561', 'screen_name': 'OlegAle64212611', 'text': 'Эстония запретила ввоз из России картофеля и яблок:\nhttps://t.co/uRftzhnRR9 https://t.co/cGERsT1wn1', 'answers_cnt': 12})
('1206442968846077952', {'id': '1206442968846077952', 'screen_name': 'petr_steven', 'text': 'В России до конца 2020 года создадут многоцелевые мины нового поколения:\nhttps://t.co/PEoveRo4GQ https://t.co/TLBMXyW7Xq', 'answers_cnt': 11})

```
