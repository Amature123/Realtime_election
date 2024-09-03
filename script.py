import psycopg2
import subprocess
import time
import requests
import logging
from confluent_kafka import SerializingProducer
import simplejson as json
import uuid
import os
# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

###ENV
BASE_URL = 'http://randomuser.me/api/?nat=gb'
PARTIES = ['Gay_party', 'Lesbian_party', 'Bissing_party']
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
##Fake data
def generate_candidate_data(candidate_number, parties):
    response = requests.get(BASE_URL + '&gender=' + ('female' if candidate_number % 2 == 1 else 'male'))
    try:
        user_data = response.json()['results'][0]
        result = {
            'candidate_id': user_data['login']['uuid'],
            'candidate_name': user_data['name']['first'] + ' ' + user_data['name']['last'],
            'party_affiliation': PARTIES[candidate_number % parties],
            'biography': 'I am a candidate',
            'campaign_platform':'campaign_platform',
            'photo_url': user_data['picture']['large']
        }
        return result
    except Exception as e:
        logger.error(f"Error generating candidate data: {e}")
        return None

def generate_voter_data(voter_number):
    response = requests.get(BASE_URL)
    try:
        user_data = response.json()['results'][0]
        result = {
            'voter_id': user_data['login']['uuid'],
            'voter_name': user_data['name']['first'] + ' ' + user_data['name']['last'],
            'age': user_data['dob']['age'],
            'gender': user_data['gender'],
            'nationality': user_data['nat'],
            'registion_number': user_data['login']['username'],
            'address':{
                'street':user_data['location']['street']['number'] + ' ' + user_data['location']['street']['name'],
                'city':user_data['location']['city'],
                'state':user_data['location']['state'],
                'country':user_data['location']['country'],
                'zip':user_data['location']['postcode']
            },
            'voter_email': user_data['email'],
            'voter_phone': user_data['phone'],
            'pic_url': user_data['picture']['large']
        }
        return result
    except Exception as e:
        logger.error(f"Error generating voter data: {e}")
        return None

###Kafka handler
def json_serializer(data):
  if isinstance(data,uuid.UUID):
    return str(data)
  raise TypeError(f"Type {type(data)} not serializable")

def delivery_report(err, msg):
  if err is not None:
    logger.error(f"Message delivery failed: {err}")
  else:
    logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def produce_data_to_kafka(producer , topic, data):
  logger.info(f"Producing record: {data}")
  producer.produce(topic=topic, 
                   key=str(data['voter_id']), 
                   value=json.dumps(data,default=json_serializer).encode('utf-8'), 
                   on_delivery=delivery_report)
                   
  producer.flush()


### Main code 
try:
    prodcuer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'error_cb': lambda err: logger.error(f"Message delivery failed: {err}")
    }
    ###Generate producer
    producer = SerializingProducer(prodcuer_config)
    ###Connect psql
    conn = psycopg2.connect(
        host='localhost',
        port=5433,
        dbname='voting',
        user='postgres',
        password='postgres')
    cur = conn.cursor()
    cur.execute(
        """SELECT * FROM candidate"""
    )
    candidate = cur.fetchall()
    ###Generate some data
    if len(candidate) == 0:
        for i in range(3):
            candidate_data = generate_candidate_data(i, 3)

            cur.execute(
                """INSERT INTO candidate (candidate_id, candidate_name, party_affiliation,biography, campaign_platform, photo_url) VALUES (%s, %s, %s, %s, %s,%s)""",
                (candidate_data['candidate_id'], candidate_data['candidate_name'], candidate_data['party_affiliation'], candidate_data['biography'],candidate_data['campaign_platform'],candidate_data['photo_url'])
            )
        conn.commit()

    for i in range(100):
        voter_data = generate_voter_data(i)
        cur.execute(
            """INSERT INTO voter (voter_id, voter_name, age, gender, nationality, registration_number, address_street,address_city, address_state, address_country, address_postal_code, voter_email, voter_phone, pic_url) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                voter_data['voter_id'], 
                voter_data['voter_name'], 
                voter_data['age'], 
                voter_data['gender'], 
                voter_data['nationality'], 
                voter_data['registration_number'], 
                voter_data['address']['street'],
                voter_data['address']['city'], 
                voter_data['address']['state'], 
                voter_data['address']['country'], 
                voter_data['address']['zip'], 
                voter_data['voter_email'], 
                voter_data['voter_phone'], 
                voter_data['pic_url']
            )
        )
        conn.commit()
        produce_data_to_kafka(producer,'voter',voter_data)
        time.sleep(1)

except Exception as e:
    logger.error(f"Error connecting to PostgreSQL: {e}")