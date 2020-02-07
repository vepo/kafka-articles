package io.vepo.kafka.articles;

import static java.util.Arrays.asList;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class SimpleConsumer {

    public static void main(String[] argv) throws Exception {
        if (argv.length != 1) {
            System.err.println("Please specify 1 parameters: topic-name");
            System.exit(-1);
        }
        String topicName = argv[0];
        System.out.println("Reading messages from Topic: " + topicName);

        // Configure the Consumer
        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "SimpleConsumerGroup");
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        try (Consumer<String, String> consumer = new KafkaConsumer<>(configProperties)) {
            consumer.subscribe(asList(topicName));
            mainLoop: while (true) {
                for (ConsumerRecord<String, String> message : consumer.poll(Duration.ofSeconds(1))) {
                    System.out.println("Message received from partition=" + message.partition() + " with offset="
                            + message.offset());
                    System.out.println("Key=" + message.key() + "\t value=" + message.value());
                    if ("exit".equals(message.value())) {
                        break mainLoop;
                    }
                }
            }
        }
        System.out.println("Exiting...");
    }
}
