# -*- coding: utf-8 -*-
import tweepy
from tweepy.streaming import json
from kafka import KafkaProducer

# Запуск zookeeper и kafka
# zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties
# kafka-server-start /usr/local/etc/kafka/server.properties

# Создание kafka topic
# kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic tweets


producer = KafkaProducer(bootstrap_servers="localhost:9092")
topic_name = "tweets"

consumer_token = "pMtrIkJf2DwbvvEfjRCvq9yvF"
consumer_secret = "QAhPzTykUImbW84EjsVqyOWc0RigCtzgQI8BpbQqg1hXb766jI"
access_token = "4196894355-3R3wcEGy4Bc25rhnTPkOxfCmMbFABmehQy6qScl"
access_secret = "UqvOEoaIAwR3E67ZBq2L7eaXCcN42c8vLvBXh6u9B4uL3"
auth = tweepy.OAuthHandler(consumer_token, consumer_secret)
auth.set_access_token(access_token, access_secret)
api = tweepy.API(auth)

# Набор id пользователей для прослушки
user_ids = ["285532415", "147964447", "34200559", "338960856", "200036850", "72525490", "20510157", "99918629"]


# «Прослушиваем» твиты и проверяем, получили ли твит от нужного id
class TweetsStreamListener(tweepy.StreamListener):
    def on_data(self, raw_data):
        data = json.loads(raw_data)
        # по факту поле всегда есть, но для логики
        if "in_reply_to_status_id_str" in data:
            # берем это поле – это будет id исходного твита
            tweet_id = data["in_reply_to_status_id_str"]
            # если не пусто, то далее присваиваем screen_name ответчику
            if tweet_id is not None:
                screen_name = data["user"]["screen_name"]
                status = api.get_status(id=tweet_id)
                value = json.dumps({
                    'id': tweet_id,
                    'screen_name': screen_name,
                    'text': status.text
                })
                print(value)
                producer.send(topic_name, key=tweet_id.encode(), value=value.encode())


# Создаем объект прослушивания
listener = TweetsStreamListener()
# Устанавливаем стрим для АПИ твиттера с созданной «прослушкой»
stream = tweepy.Stream(auth=api.auth, listener=listener)
# Начинаем фильтрацию сообщений
stream.filter(follow=user_ids)
