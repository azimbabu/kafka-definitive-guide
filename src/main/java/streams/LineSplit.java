package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Create topics if not created already :
 *
 * <p>bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1
 * --partitions 1 --topic streams-plaintext-input
 *
 * <p>bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1
 * --partitions 1 --topic streams-linesplit-output
 *
 * <p>Run producer from command line :
 *
 * <p>bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic
 * streams-plaintext-input
 *
 * <p>Run consumer from command line :
 *
 * <p>bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic
 * streams-linesplit-output --from-beginning
 *
 * <p>Then run the main method of LineSplit class
 */
public class LineSplit {

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-linesplit");
    props.put(
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
        "localhost:9092"); // assuming that the Kafka broker this application is talking to runs on
                           // local machine with port 9092
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    final StreamsBuilder builder = new StreamsBuilder();
    KStream<String, String> source = builder.stream("streams-plaintext-input");
    source
        .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
        .to("streams-linesplit-output");

    final Topology topology = builder.build();
    System.out.println(topology.describe());
    final KafkaStreams streams = new KafkaStreams(topology, props);
    final CountDownLatch latch = new CountDownLatch(1);

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread("streams-shutdown-hook") {
              @Override
              public void run() {
                streams.close();
                latch.countDown();
                System.out.println("Shutting down");
              }
            });

    try {
      streams.start();
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
      System.exit(1);
    }

    System.exit(0);
  }
}
