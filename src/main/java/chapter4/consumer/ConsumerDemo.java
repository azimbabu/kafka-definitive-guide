package chapter4.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConsumerDemo {

  public static void main(String[] args) {
    String deserializerName = StringDeserializer.class.getName();
    Properties properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("group.id", "CountryCounter");
    properties.put("key.deserializer", deserializerName);
    properties.put("value.deserializer", deserializerName);

    Map<String, Integer> countryCount = new HashMap<>();
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
      consumer.subscribe(Collections.singletonList("CustomerCountry"));
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
          countryCount.put(record.value(), countryCount.getOrDefault(record.value(), 0) + 1);
          JSONObject json = new JSONObject(countryCount);
          System.out.println(json.toString(4));
        }
      }
    }
  }
}
