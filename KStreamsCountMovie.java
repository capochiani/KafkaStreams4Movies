/**
 * Implementa una classe KStreamCountMovie.
 * Senza variabili esterne dichiarate.
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

public class KStreamsCountMovie {

	public static void main(String[] args) {
		Properties props = new Properties();

		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-countmovie");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		final StreamsBuilder builder = new StreamsBuilder();

		/*
		 * Inizio Snippet. Qui inizia la vera differenza rispetto alle due classi
		 * precedenti (StreamsCountMovie, StreamsCountMovieTopic)
		 */

		KStream<String, String> source = builder.stream("streams-movie-input");
		source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
				// In questo Arrays.asList possiamo instanziare altre funzioni di count (avg,
				// min, max).
				.groupBy((key, value) -> value)
				// .windowedBy(SessionWindows.with(Duration.ofSeconds(60)).grace(Duration.ofSeconds(2)))
				.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store")).toStream()
				.to("streams-countmovie-output", Produced.with(Serdes.String(), Serdes.Long()));

		/*
		 * Fine Snippet
		 */

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
