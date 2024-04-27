Build:
    mvn -q clean package

Test Examples:
    java -jar target/rabbitmq-streams.jar produce -h 172.25.161.131 -ps 250 -pc 5000 -ms 1000000 -ml 10000000

    java -jar target/rabbitmq-streams.jar consume -h 172.25.161.131
    count=5,000   duration=640 ms   rate=7,812 msg/sec

    java -jar target/rabbitmq-streams.jar delete -h 172.25.161.131



Usage: rabbitmq-streams [-bs=batch size] [-h=host] [-ml=max length (bytes)]
                        [-ms=max segment (bytes)] [-p=port] [-pc=produce count]
                        [-pd=produce delay (millis)] [-ps=payload size]
                        [-pw=password] [-sn=stream name] [-u=username] command
test RabbitMQ streams performance
    command             consume, produce, or delete
    -bs=batch size
    -h=host
    -ml=max length (bytes)
    -ms=max segment (bytes)
    -p=port
    -pc=produce count
    -pd=produce delay (millis)
    -ps=payload size
    -pw=password
    -sn=stream name
    -u=username