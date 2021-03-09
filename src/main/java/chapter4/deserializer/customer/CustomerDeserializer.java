package chapter4.deserializer.customer;

import chapter3.serializer.custom.Customer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class CustomerDeserializer implements Deserializer<Customer> {
  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("group.id", "Customers");
    properties.put("key.deserializer", StringDeserializer.class.getName());
    properties.put("value.deserializer", CustomerDeserializer.class.getName());

    try (KafkaConsumer<String, Customer> consumer = new KafkaConsumer<>(properties)) {
      String topic = "Customers";
      consumer.subscribe(Collections.singletonList(topic));
      while (true) {
        ConsumerRecords<String, Customer> records = consumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord<String, Customer> record : records) {
          Customer customer = record.value();
          System.out.println(
              String.format(
                  "topic = %s, partition = %d, offset = %d, key = %s, customer id = %s, customer name = %s",
                  record.topic(),
                  record.partition(),
                  record.offset(),
                  record.key(),
                  customer.getId(),
                  customer.getName()));
        }
        consumer.commitSync();
      }
    }
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    // nothing to configure
  }

  @Override
  public Customer deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    } else if (data.length < 8) {
      throw new SerializationException(
          "Size of data received by deserializer is shorter than expected");
    }

    try {
      ByteBuffer buffer = ByteBuffer.wrap(data);
      int id = buffer.getInt();
      int nameSize = buffer.getInt();
      byte[] nameBytes = new byte[nameSize];
      buffer.get(nameBytes);
      String name = new String(nameBytes, StandardCharsets.UTF_8);
      return new Customer(id, name);
    } catch (Exception e) {
      throw new SerializationException("Error when deserializing byte[] to Customer " + e);
    }
  }

  @Override
  public void close() {
    // nothing to close
  }
}
