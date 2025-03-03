from confluent_kafka import Consumer, KafkaException
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import socket
import json

def oauth_cb(oauth_config):
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token("eu-west-2")
    # Note that this library expects oauth_cb to return expiry time in seconds since epoch, while the token generator returns expiry in ms
    return auth_token, expiry_ms/1000

consumer = Consumer({
    "debug": "all",
    'bootstrap.servers': "b-2.greencluster.jdc7ic.c3.kafka.eu-west-2.amazonaws.com:9098",
    'client.id': socket.gethostname(),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'OAUTHBEARER',
    'oauth_cb': oauth_cb,
    'group.id': 'event-group-id',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['events'])


try:
    while True:
        msg = consumer.poll(1.0)  # Poll for messages (1 second timeout)

        if msg is None:
            continue  # No new messages, keep polling
        
        if msg.error():
            raise KafkaException(msg.error())

        # Print raw message content for debugging
        raw_message = msg.value().decode('utf-8')
        print(f"Raw message: {raw_message}")

except json.JSONDecodeError as e:
    print(f"Error decoding JSON: {e}")
