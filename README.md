# Lenses Serde for Kafka consumer offset topic

Kafka contains a topic called `__consumer_offsets` that stores the offsets for each consumer group. This topic 
stores the offsets committed by the consumers. Alongside the offset information the topic also stores the
metadata of the consumer group, such as the topic partitions assigned to the group, the consumer group id,etc.

The Lenses Serde for Kafka consumer offset topic is a tool that allows you to only read the topic content.


## Build

To build:

```bash
mvn clean package
```

To format the code run:

```bash
mvn com.coveo:fmt-maven-plugin:format
```

To add license header, run:

```bash
mvn license:format
```