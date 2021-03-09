package chapter4.exitpoll;

import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SimpleMovingAvgNewConsumer {

    private Properties kafkaProperties = new Properties();
    private KafkaConsumer<String, String> consumer;

  public static void main(String[] args) {
      if (args.length == 0) {
          System.err.println(String.format("%s, {brokers} {group.id} {topic} {window-size}"));
          return;
      }

      final SimpleMovingAvgNewConsumer movingAvg = new SimpleMovingAvgNewConsumer();
      String brokers = args[0];
      String groupId = args[1];
      String topic = args[2];
      int window = Integer.parseInt(args[3]);

      CircularFifoBuffer buffer = new CircularFifoBuffer(window);
      movingAvg.configure(brokers, groupId);

      final Thread mainThread = Thread.currentThread();

      // Registering a shutdown hook so we can exit cleanly
      Runtime.getRuntime().addShutdownHook(new Thread() {
          @Override
          public void run() {
              System.out.println("Starting exit...");
              // Note that shutdownhook runs in a separate thread, so the only thing we can safely do to a consumer is wake it up
              movingAvg.consumer.wakeup();
              try {
                  mainThread.join();
              } catch (InterruptedException e) {
                  e.printStackTrace();
              }
          }
      });

      try {
          movingAvg.consumer.subscribe(Collections.singletonList(topic));

          while (true) {
              ConsumerRecords<String, String> records = movingAvg.consumer.poll(Duration.ofMillis(1000));
              System.out.println(System.currentTimeMillis() + "  --  waiting for data...");

              for (ConsumerRecord<String, String> record : records) {
                  System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());

                  int sum = 0;

                  try {
                      int num = Integer.parseInt(record.value());
                      buffer.add(num);
                  } catch (NumberFormatException e) {
                      // just ignore strings
                  }

                  for (Object o : buffer) {
                      sum += (Integer) o;
                  }

                  if (buffer.size() > 0) {
                      System.out.println("Moving avg is: " + (sum/buffer.size()));
                  }
              }

              for (TopicPartition topicPartition : movingAvg.consumer.assignment()) {
                  System.out.println("Committing offset at position: " + movingAvg.consumer.position(topicPartition));
              }
              movingAvg.consumer.commitSync();
          }
      } catch (WakeupException e) {
          // ignore for shutdown
      } finally {
          movingAvg.consumer.close();
          System.out.println("Closed consumer and we are done");
      }
  }

    private void configure(String servers, String groupId) {
        String deserializerName = StringDeserializer.class.getName();
        kafkaProperties.put("bootstrap.servers", servers);
        kafkaProperties.put("group.id", groupId);
        kafkaProperties.put("key.deserializer", deserializerName);
        kafkaProperties.put("value.deserializer", deserializerName);
        kafkaProperties.put("enable.auto.commit", "false");
    }
}
