package chapter4.commit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SpecifiedOffsetCommit {

  public static void main(String[] args) {
    String deserializerName = StringDeserializer.class.getName();
    Properties properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("group.id", "CountryCounter");
    properties.put("key.deserializer", deserializerName);
    properties.put("value.deserializer", deserializerName);
    properties.put("enable.auto.commit", "false");

    Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    int count = 0;

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
      consumer.subscribe(Collections.singletonList("CustomerCountry"));
      while (true) {
        boolean commitPending = true;
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord<String, String> record : records) {
          System.out.println(
              String.format(
                  "topic = %s, partition = %d, offset = %d, customer = %s, country = %s",
                  record.topic(),
                  record.partition(),
                  record.offset(),
                  record.key(),
                  record.value()));

          currentOffsets.put(
              new TopicPartition(record.topic(), record.partition()),
              new OffsetAndMetadata(record.offset() + 1, "no metadata"));

          if (count % 1000 == 0) {
            consumer.commitAsync(currentOffsets, null);
            commitPending = false;
          }
          count++;
        }

        if (commitPending) {
          consumer.commitAsync(currentOffsets, null);
        }
      }
    }
  }
}
