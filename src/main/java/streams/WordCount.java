package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Create topics if not created already :
 *
 * <p>bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1
 * --partitions 1 --topic streams-plaintext-input
 *
 * <p>bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1
 * --partitions 1 --topic streams-wordcount-output --config cleanup.policy=compact
 *
 * <p>Run producer from command line :
 *
 * <p>bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic
 * streams-plaintext-input
 *
 * <p>Run consumer from command line :
 *
 * <p>bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \ --topic
 * streams-wordcount-output \ --from-beginning \ --formatter kafka.tools.DefaultMessageFormatter \
 * --property print.key=true \ --property print.value=true \ --property
 * key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \ --property
 * value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 *
 * <p>Then run the main method of WordCount class
 */
public class WordCount {

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
    props.put(
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
        "localhost:9092"); // assuming that the Kafka broker this application is talking to runs on
                           // local machine with port 9092
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    final StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> source = builder.stream("streams-plaintext-input");
    source
        .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
        .groupBy((key, value) -> value)
        // Materialize the result into a KeyValueStore named "counts-store".
        // The Materialized store is always of type <Bytes, byte[]> as this is the format of the
        // inner most store.
        .count(Materialized.as("counts-store"))
        .toStream()
        .to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

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
