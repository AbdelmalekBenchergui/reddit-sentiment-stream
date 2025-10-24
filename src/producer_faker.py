from faker import Faker
from kafka import KafkaProducer
import json, time, random

fake = Faker('fr_FR')

def generate_user():
    user = {
        'first_name': fake.first_name(),
        'last_name': fake.last_name(),
        'email': fake.email(),
        'username': fake.user_name(),
        'dob': fake.date_of_birth().isoformat(),
        'address': fake.address().replace('\n', ', ')
    }
    return user

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Streaming fake users to Kafka topic 'users_created' ...")
try:
    while True:
        user = generate_user()
        producer.send('users_created', user)
        print("Sent:", user)
        time.sleep(random.uniform(0.5, 2.0))
except KeyboardInterrupt:
    print("Stopping producer...")
finally:
    producer.close()


