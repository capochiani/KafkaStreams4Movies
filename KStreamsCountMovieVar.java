/**
 * Implementa una classe KStreamCountMovieVar.
 * Con variabili esterne dichiarate.
 * 
 * Correlazione Classi: KStreamCountMovie & KStreamCountMovieVar
 * 
 */

package Streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KStreamsCountMovieVar {
	// var
	public static final String INPUT_TOPIC = "/movie-stream:movie-input";
	public static final String OUTPUT_TOPIC = "movie-output";
	public static final String DEFAULT_STREAM = "/movie-stream";
	public static final String APP_ID = "movie_counting";

	public static void main(String[] args) {
		Properties props = new Properties();

		props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		final StreamsBuilder builder = new StreamsBuilder();

		// KSTREAM
		KStream<String, String> countMovieStream = builder.stream(INPUT_TOPIC);
		countMovieStream.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
				.groupBy((key, value) -> value)
				.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"))
				.mapValues(x -> x.toString()).toStream();

		countMovieStream.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
		// FINE KSTREAM

		final Topology topology = builder.build();
		System.out.println(topology.describe());
		final KafkaStreams streams = new KafkaStreams(topology, props);
		final CountDownLatch latch = new CountDownLatch(1);

		// Shoutdown
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
