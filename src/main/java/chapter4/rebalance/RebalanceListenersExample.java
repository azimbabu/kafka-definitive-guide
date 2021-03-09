package chapter4.rebalance;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class RebalanceListenersExample {

    public static void main(String[] args) {
        String deserializerName = StringDeserializer.class.getName();
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "CountryCounter");
        properties.put("key.deserializer", deserializerName);
        properties.put("value.deserializer", deserializerName);
        properties.put("enable.auto.commit", "false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

        try {
            consumer.subscribe(Collections.singletonList("CustomerCountry"), new HandleRebalance(consumer, currentOffsets));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format("topic = %s, partition = %d, offset = %d, customer = %s, country = %s",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value()));

                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()+1, "no metadata"));
                }

                consumer.commitAsync(currentOffsets, null);
            }
        } catch (WakeupException ignored) {
            // Ignore, we're closing
        } catch (Exception e) {
            System.err.println("Unexpected error=" + e);
        }  finally {
            try {
                consumer.commitSync(currentOffsets);
            } finally {
                consumer.close();
                System.out.println("Closed consumer and we are done");
            }
        }
    }

    private static class HandleRebalance implements ConsumerRebalanceListener {
        private Consumer<String, String> consumer;
        private Map<TopicPartition, OffsetAndMetadata> currentOffsets;

        public HandleRebalance(Consumer<String, String> consumer, Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
            this.consumer = consumer;
            this.currentOffsets = currentOffsets;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            System.out.println("Lost partitions in rebalance. " + "Committing current offsets: " + currentOffsets);
            consumer.commitSync(currentOffsets);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

        }
    }
}
