/**
 * Implementa una classe StreamsCountMovie (Streams DSL) che legge dal topic
 * "streams-movie-input" i cui valori del messaggio rappresentano linee di
 * testo. Splitta ogni linea in parole e poi conta le occorrenze che vengono
 * scritte nel topic "streams-countmovie-output".
 * 
 */

package Streams;

import org.apache.kafka.streams.Topology;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
//import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
//import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamsCountMovie {

	public static void main(String[] args) throws Exception {
		Properties props = new Properties();

		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-countmovie");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		final StreamsBuilder builder = new StreamsBuilder();

		builder.<String, String>stream("streams-movie-input")
				.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
				// In questo Arrays.asList possiamo instanziare altre funzioni di count (avg,
				// min, max).
				.groupBy((key, value) -> value)
				.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store")).toStream()
				.to("streams-countmovie-output", Produced.with(Serdes.String(), Serdes.Long()));

		final Topology topology = builder.build();
		final KafkaStreams streams = new KafkaStreams(topology, props);
		final CountDownLatch latch = new CountDownLatch(1);

		// Shutdown
		Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
			@Override
			public void run() {
				streams.close();
				latch.countDown();
			}
		});

		try {
			streams.start();
			latch.await();
		} catch (Throwable e) {
			System.exit(1);
		}
		System.exit(0);
	}
}
