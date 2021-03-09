package chapter3.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

  public static void main(String[] args) {
      Properties kafkaProps = getProperties();
      fireAndForgetSend(kafkaProps);
      synchronousSend(kafkaProps);
      asynchronousSend(kafkaProps);
  }

    private static Properties getProperties() {
        String serializerName = StringSerializer.class.getName();
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", serializerName);
        kafkaProps.put("value.serializer", serializerName);
        return kafkaProps;
    }

    private static void fireAndForgetSend(Properties kafkaProps) {
        ProducerRecord<String, String> record = new ProducerRecord<>("CustomerCountry", "Precision Products", "France");
        try(Producer<String, String> producer = new KafkaProducer<>(kafkaProps)) {
            producer.send(record);
            System.out.println("Fire and forget send");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void synchronousSend(Properties kafkaProps) {
        ProducerRecord<String, String> record = new ProducerRecord<>("CustomerCountry", "Precision Products", "France");
        try(Producer<String, String> producer = new KafkaProducer<>(kafkaProps)) {
            RecordMetadata recordMetadata = producer.send(record).get();
            System.out.println("Synchronous send, recordMetadata=" + recordMetadata);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void asynchronousSend(Properties kafkaProps) {
        ProducerRecord<String, String> record = new ProducerRecord<>("CustomerCountry", "Biomedical Materials", "USA");
        try(Producer<String, String> producer = new KafkaProducer<>(kafkaProps)) {
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                } else {
                    System.out.println("Synchronous send, recordMetadata=" + metadata);
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
