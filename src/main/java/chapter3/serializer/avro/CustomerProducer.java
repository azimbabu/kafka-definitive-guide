package chapter3.serializer.avro;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CustomerProducer {

  public static void main(String[] args) throws ExecutionException, InterruptedException {
      String serializerName = KafkaAvroSerializer.class.getName();
      String schemaRegistryUrl = "http://localhost:8081";

      Properties properties = new Properties();
      properties.put("bootstrap.servers", "localhost:9092");
      properties.put("key.serializer", serializerName);
      properties.put("value.serializer", serializerName);
      properties.put("schema.registry.url", schemaRegistryUrl);

      String topic = "customerContacts";

      try (Producer<String, Customer> producer = new KafkaProducer<>(properties)) {
          for (int i=0; i < 10; i++) {
              Customer customer = CustomerGenerator.getNext();
              System.out.println("Generated Customer " + customer);
              ProducerRecord<String, Customer> record = new ProducerRecord<>(topic, customer.getName().toString(), customer);
              producer.send(record);
          }
      }
  }
}
