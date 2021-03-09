package chapter3.serializer.custom;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class CustomerSerializer implements Serializer<Customer> {
  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("key.serializer", StringSerializer.class.getName());
    properties.put("value.serializer", CustomerSerializer.class.getName());
    String topic = "Customers";

    Producer<String, Customer> producer = new KafkaProducer<>(properties);
    while (true) {
      int id = ThreadLocalRandom.current().nextInt();
      Customer customer = new Customer(id, RandomStringUtils.randomAlphabetic(16));
      System.out.println("Generated Customer " + customer);
      ProducerRecord<String, Customer> record =
          new ProducerRecord<>(topic, "Customer-" + id, customer);
      producer.send(record);
    }
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    // nothing to configure
  }

  @Override
  public byte[] serialize(String topic, Customer data) {
    try {
      byte[] serializedName;
      int stringSize;
      if (data == null) {
        return null;
      } else if (data.getName() != null) {
        serializedName = data.getName().getBytes(StandardCharsets.UTF_8);
        stringSize = serializedName.length;
      } else {
        serializedName = new byte[0];
        stringSize = 0;
      }

      ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + stringSize);
      buffer.putInt(data.getId());
      buffer.putInt(stringSize);
      buffer.put(serializedName);
      return buffer.array();
    } catch (Exception e) {
      throw new SerializationException("Error when serializing Customer to byte[] " + e);
    }
  }

  @Override
  public void close() {
    // nothing to close
  }
}
