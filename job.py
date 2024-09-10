
from datetime import datetime
import json
import random
import psycopg2
from confluent_kafka import Consumer, KafkaException, KafkaError ,SerializingProducer
import os
import logging




####################################################################################Set up logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
conf={
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'error_cb': lambda err: logger.error(f"Message delivery failed: {err}")
}
consumer = Consumer(conf | {
    'group.id': 'voting_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
} )
############################################################

def delivery_report(err, msg):
  if err is not None:
    logger.error(f"Message delivery failed: {err}")
  else:
    logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

##########################################################

producer = SerializingProducer(conf)

if __name__ == '__main__':

    conn = psycopg2.connect(
    host='localhost',
    port=5433,
    dbname='voting',
    user='postgres',
    password='postgres')
    cur = conn.cursor()
    candidate = cur.execute(
    """SELECT row_to_json(col) FROM (Select * from candidate) col ;"""
    )
    candidate = [candidate[0] for candidate in cur.fetchall()]

    consumer.subscribe(['voter'])
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else : 
                voter = json.load(msg.value().decode('utf-8'))
                choosen =random.choice(candidate)
                vote = voter | choosen | {'vote_date':datetime.now.strftime('%Y-%m-%d %H:%M:%S'),"vote":1}
                try :
                    print('User {} is voting on {}'.format(vote['voter_id'],vote['candidate_id']))
                    cur.execute(
                        """INSERT INTO vote (voter_id, candidate_id, vote_date, vote) VALUES (%s, %s, %s, %s)""",
                        (vote['voter_id'], vote['candidate_id'], vote['vote_date'], vote['vote'])
                    )
                    conn.commit()
                    producer.produce('voter',key = vote['voter_id'] ,value = json.dumps(vote).encode('utf-8'),on_delivery=delivery_report)
                except Exception as e:
                    logger.error(f'Error occurred: {e}')
    except Exception as e:
        raise e
