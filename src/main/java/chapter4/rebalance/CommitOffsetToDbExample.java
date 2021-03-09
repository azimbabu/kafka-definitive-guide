package chapter4.rebalance;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class CommitOffsetToDbExample {

  public static void main(String[] args) {
    String deserializerName = StringDeserializer.class.getName();
    Properties properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("group.id", "CountryCounter");
    properties.put("key.deserializer", deserializerName);
    properties.put("value.deserializer", deserializerName);
    properties.put("enable.auto.commit", "false"); // Disable automatic commit.

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

    try {
      consumer.subscribe(
          Collections.singletonList("CustomerCountry"), new SaveOffsetsOnRebalance(consumer));
      consumer.poll(Duration.ofMillis(0));

      for (TopicPartition topicPartition : consumer.assignment()) {
        consumer.seek(topicPartition, getOffsetFromDB(topicPartition));
      }

      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> record : records) {
          processRecord(record);
          storeRecordInDB(record);
          storeOffsetInDB(record.topic(), record.partition(), record.offset());
        }
        commitDBTransaction();
      }
    } finally {
      consumer.close();
    }
  }

  private static void commitDBTransaction() {}

  private static void storeOffsetInDB(String topic, int partition, long offset) {}

  private static void storeRecordInDB(ConsumerRecord<String, String> record) {}

  private static void processRecord(ConsumerRecord<String, String> record) {}

  private static long getOffsetFromDB(TopicPartition topicPartition) {
    return 0;
  }

  private static class SaveOffsetsOnRebalance implements ConsumerRebalanceListener {
    private final Consumer<String, String> consumer;

    public SaveOffsetsOnRebalance(Consumer<String, String> consumer) {
      this.consumer = consumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      commitDBTransaction();
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
      for (TopicPartition topicPartition : consumer.assignment()) {
        consumer.seek(topicPartition, getOffsetFromDB(topicPartition));
      }
    }
  }
}
