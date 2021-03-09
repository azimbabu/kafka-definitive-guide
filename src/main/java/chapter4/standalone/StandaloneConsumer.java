package chapter4.standalone;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class StandaloneConsumer {

  public static void main(String[] args) {
    String deserializerName = StringDeserializer.class.getName();
    Properties properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("group.id", "CountryCounterStandalone");
    properties.put("key.deserializer", deserializerName);
    properties.put("value.deserializer", deserializerName);

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
      String topic = "CustomerCountry";
      List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
      if (CollectionUtils.isEmpty(partitionInfos)) {
        System.out.println("No paritions available for topic=" + topic);
        return;
      }

      List<TopicPartition> topicPartitions =
          partitionInfos.stream()
              .map(
                  partitionInfo ->
                      new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
              .collect(Collectors.toList());
      consumer.assign(topicPartitions);

      while (true) {
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
        }
        consumer.commitSync();
      }
    }
  }
}
