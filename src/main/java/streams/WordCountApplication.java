package streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

/**
 * Create topics if not created already :
 *
 * bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic streams-plaintext-input
 *
 * bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic streams-wordcount-output --config cleanup.policy=compact
 *
 * Run producer from command line :
 *
 * bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic streams-plaintext-input
 *
 * Run consumer from command line :
 *
 * bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
 --topic streams-wordcount-output \
 --from-beginning \
 --formatter kafka.tools.DefaultMessageFormatter \
 --property print.key=true \
 --property print.value=true \
 --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
 --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 *
 * Then run the main method of WordCountApplication class
 */
public class WordCountApplication {

  public static void main(String[] args) {
    // Serializers/deserializers (serde) for String and Long types
    final Serde<String> stringSerde = Serdes.String();
    final Serde<Long> longSerde = Serdes.Long();

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    StreamsBuilder builder = new StreamsBuilder();

    // Construct a `KStream` from the input topic "streams-plaintext-input", where message values
    // represent lines of text (for the sake of this example, we ignore whatever may be stored
    // in the message keys).
    KStream<String, String> textLines =
        builder.stream("streams-plaintext-input", Consumed.with(stringSerde, stringSerde));

    KTable<String, Long> wordCounts =
        textLines
            // Split each text line, by whitespace, into words.
            .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))

            // Group the text words as message keys
            .groupBy((key, value) -> value)

            // Count the occurrences of each word (message key).
            .count();

    // Store the running counts as a changelog stream to the output topic.
    wordCounts.toStream().to("streams-wordcount-output", Produced.with(stringSerde, longSerde));

    KafkaStreams streams = new KafkaStreams(builder.build(), props);
    streams.start();
  }
}
