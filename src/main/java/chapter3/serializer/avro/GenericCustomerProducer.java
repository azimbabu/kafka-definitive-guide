package chapter3.serializer.avro;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class GenericCustomerProducer {

  public static void main(String[] args) throws IOException, URISyntaxException {
      String serializerName = KafkaAvroSerializer.class.getName();
      String schemaRegistryUrl = "http://localhost:8081";

      Properties properties = new Properties();
      properties.put("bootstrap.servers", "localhost:9092");
      properties.put("key.serializer", serializerName);
      properties.put("value.serializer", serializerName);
      properties.put("schema.registry.url", schemaRegistryUrl);

      Path path = Paths.get(GenericCustomerProducer.class.getClassLoader().getResource("avro/schema/customer-schema.avsc").toURI());
      String schemaString = Files.readString(path);
      Schema.Parser parser = new Schema.Parser();
      Schema schema = parser.parse(schemaString);

      String topic = "customerContacts";
      int maxCustomers = 10;
      Producer<String, GenericRecord> producer = new KafkaProducer<>(properties);
      for (int nCustomers=0; nCustomers < maxCustomers; nCustomers++) {
          String name = "Example Customer " + nCustomers;
          String email = "example" + nCustomers + "@example.com";

          GenericRecord customer = new GenericData.Record(schema);
          customer.put("id", nCustomers);
          customer.put("name", name);
          customer.put("email", email);

          ProducerRecord<String, GenericRecord> data = new ProducerRecord<>(topic, name, customer);
          producer.send(data);
          System.out.println(data);
      }

  }
}
