# Klone - a Kafka replicator with exactly once semantics

Klone replicates messages from a source to a Kafka clutsre. Currently Kafka is the only supported source, with RabbitMQ and JDBC sources planned. 

- Exactly Once - use transactionally commit offsets to the desitnation cluster to to provide exactlu once symmantics
- Cloud Native - Klone is designed to run on a container orchestrator - this allows a simple design with let it crash principles
- Mertics - expose metrics on a Prometheus endpoint

## Usage

```shell
docker run --rm fergusn/klone --help

docker run --rm fergusn/klone kafka --bootrap-server kafka1:9092 --group-id example --topic t1 --topic t2  \
                              kafka --bootrap-server kafka2:9092 
                             
```
