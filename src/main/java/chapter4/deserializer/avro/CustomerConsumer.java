package chapter4.deserializer.avro;

import chapter3.serializer.avro.Customer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class CustomerConsumer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "CountryCounter");
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", KafkaAvroDeserializer.class.getName());
        properties.put("specific.avro.reader", "true");
        properties.put("schema.registry.url", "http://localhost:8081");

        String topic = "customerContacts";

        try (KafkaConsumer<String, Customer> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            System.out.println("Reading topic:" + topic);

            while (true) {
                ConsumerRecords<String, Customer> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, Customer> record : records) {
                    Customer customer = record.value();
                    System.out.println(String.format("topic = %s, partition = %d, offset = %d, key = %s, customer id = %s, customer name = %s",
                            record.topic(), record.partition(), record.offset(), record.key(), customer.getId(), customer.getName()));
                }
                consumer.commitSync();
            }
        }
    }
}
