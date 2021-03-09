package chapter4.commit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class AsynchronousCommitWithCallback {

    public static void main(String[] args) {
        String deserializerName = StringDeserializer.class.getName();
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "CountryCounter");
        properties.put("key.deserializer", deserializerName);
        properties.put("value.deserializer", deserializerName);
        properties.put("enable.auto.commit", "false");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList("CustomerCountry"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format("topic = %s, partition = %d, offset = %d, customer = %s, country = %s",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value()));
                }

                consumer.commitAsync((offsets, exception) -> {
                    if (exception != null) {
                        System.err.println("Commit failed for offsets=" + offsets + ", error=" + exception);
                    }
                });
            }
        }
    }
}
