from confluent_kafka import Producer, Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

# Konfigurasi Kafka
bootstrap_servers = 'localhost:9092'
topic_name = 'test_topic'

# Membuat topik Kafka
def create_topic():
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
    topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
    admin_client.create_topics([topic])

# Mengirim pesan ke Kafka
def send_message(producer, message):
    try:
        producer.produce(topic_name, value=message)
        producer.flush()
        print('Pesan berhasil dikirim ke Kafka.')
    except KafkaException as e:
        print('Kesalahan saat mengirim pesan ke Kafka: {}'.format(str(e)))

# Menerima pesan dari Kafka
def consume_messages(consumer):
    consumer.subscribe([topic_name])
    while True:
        try:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print("Kesalahan saat menerima pesan: {}".format(msg.error()))
                continue

            print('Menerima pesan: {}'.format(msg.value().decode('utf-8')))
            consumer.commit()
        except KeyboardInterrupt:
            break

# Membuat producer dan consumer
def main():
    create_topic()

    producer = Producer({'bootstrap.servers': bootstrap_servers})
    consumer = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'my-consumer-group',
        'auto.offset.reset': 'earliest'
    })

    while True:
        print('\n--- Pilih aksi ---')
        print('1. Kirim pesan')
        print('2. Terima pesan')
        print('0. Keluar')

        choice = input('Pilihan Anda: ')

        if choice == '1':
            message = input('Masukkan pesan yang ingin dikirim: ')
            send_message(producer, message)
        elif choice == '2':
            consume_messages(consumer)
        elif choice == '0':
            break
        else:
            print('Pilihan tidak valid. Silakan pilih angka 0-2.')

if __name__ == '__main__':
    main()
