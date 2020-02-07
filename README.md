# Kafka Articles

# Simple Consumer e Producer

See classes:
*  `io.vepo.kafka.articles.SimpleConsumer`
*  `io.vepo.kafka.articles.SimpleProducer`

To execute producer:

```bash
mvn clean compile exec:java -Dexec.mainClass="io.vepo.kafka.articles.SimpleProducer" -Dexec.args="test"
```

To execute consumer:

```bash
mvn clean compile exec:java -Dexec.mainClass="io.vepo.kafka.articles.SimpleConsumer" -Dexec.args="test"
```