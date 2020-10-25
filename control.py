import pika
import pymongo
import sys
import time

repository_ip = '192.168.1.150'
repository_port = 5672

if len(sys.argv) == 5 and sys.argv[1] == '-rip' and sys.argv[3] == '-rport':
    repository_ip = sys.argv[2]
    repository_port = sys.argv[4]
else:
    print("invalid command")
    exit()

username = 'tom_swift'
password = 'flying_lab'
credentials = pika.PlainCredentials(username, password)
parameters = pika.ConnectionParameters(repository_ip, repository_port, '/', credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

# declare/initialize all exchanges
channel.exchange_declare(exchange="Squires", exchange_type='direct',
                         durable=True)
channel.exchange_declare(exchange="Goodwin", exchange_type='direct',
                         durable=True)
channel.exchange_declare(exchange="Library", exchange_type='direct',
                         durable=True)

# declare/initialize all queues
channel.queue_declare(exchange="Food", durable=True)
channel.queue_declare(exchange="Meetings", durable=True)
channel.queue_declare(exchange="Rooms", durable=True)
channel.queue_declare(exchange="Classrooms", durable=True)
channel.queue_declare(exchange="Auditorium", durable=True)
channel.queue_declare(exchange="Noise", durable=True)
channel.queue_declare(exchange="Seating", durable=True)
channel.queue_declare(exchange="Wishes", durable=True)

# bind queues with exchanges
channel.queue_bind(exchange="Squires", queue="Food",
                   routing_key="Food")
channel.queue_bind(exchange="Squires", queue="Meetings",
                   routing_key="Meetings")
channel.queue_bind(exchange="Squires", queue="Rooms",
                   routing_key="Rooms")
channel.queue_bind(exchange="Goodwin", queue="Classrooms",
                   routing_key="Classrooms")
channel.queue_bind(exchange="Goodwin", queue="Auditorium",
                   routing_key="Auditorium")
channel.queue_bind(exchange="Library", queue="Noise",
                   routing_key="Noise")
channel.queue_bind(exchange="Library", queue="Seating",
                   routing_key="Seating")
channel.queue_bind(exchange="Library", queue="Wishes",
                   routing_key="Wishes")

print("[Ctrl 01] – Connecting to RabbitMQ instance on", repository_ip,
      "with port", repository_port)
print("[Ctrl 02] – Initialized Exchanges and Queues:",
      "{<LIST ALL EXCHANGES:QUEUES PAIRS>}")
print("[Ctrl 03] – Initialized MongoDB datastore")
database = pymongo.MongoClient().test

while True:
    data = input("[Ctrl 04] – > Enter a command:<ENTER YOUR PRODUCE / CONSUME "
                 "/ EXIT COMMAND>")
    command = data.split(':')[0]
    place = data.split(':')[1].split('+')[0]
    msgid = "team_05" + "$" + str((time.time()))
    subject = place.split('+')[1].split(' ')[0]
    message = place.split('+')[0].split(' ')[1]
    information = {
        "Action": command,
        "Place": place,
        "Msg_ID": msgid,
        "Subject": subject,
        "Message": message
    }
    database.utilization.insert(information)

    print("[Ctrl 05] – Inserted command into MongoDB: "
          "<MONGODB FORMAT INFO>")
    print(information)
    if command == 'p':
        print("[Ctrl 06] – Produced message ", message, " on ",
              place, ": ", subject)
        channel.basic_publish(exchange=place, routing_key=subject,
                              body=message)

    elif command == 'c':
        def callback(ch, method, properties, body):
            print("[Ctrl 07] – Consumed message ", body, " on ",
                  place, ": ", subject)
        channel.basic_consume(callback, queue=subject, no_ack=True)
        channel.start_consuming()

    elif command == 'exit':
        print("[Ctrl 08] – Exiting")
        exit()
