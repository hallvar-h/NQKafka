from nqkafka.utils import get_last_message_from_topic

msg = get_last_message_from_topic(bootstrap_servers='localhost:40000', topic='time')
print(msg.value)