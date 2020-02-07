package io.vepo.kafka.articles;

import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class SimpleProducer {

    public static void main(String[] argv) throws Exception {
        if (argv.length != 1) {
            System.err.println("Please specify 1 parameters: topic-name");
            System.exit(-1);
        }
        String topicName = argv[0];
        System.out.println("Enter message(type exit to quit)");

        // Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (Scanner in = new Scanner(System.in);
                Producer<String, String> producer = new KafkaProducer<>(configProperties)) {
            String line;
            do {
                line = in.nextLine();
                ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName, line);
                Future<RecordMetadata> results = producer.send(rec);
                RecordMetadata metadata = results.get();
                System.out
                    .println("Message sent on partition=" + metadata.partition() + " with offset=" + metadata.offset());
            } while (!line.equals("exit"));
            System.out.print("Exiting...");
        }
    }
}