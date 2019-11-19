
/**
 * Implementa una classe StreamsCountMovieTopic (Streams DSL) che legge dal
 * topic "streams-movie-input" i cui valori del messaggio rappresentano linee di
 * testo. Splitta ogni linea in parole e poi scrive in un sink topic
 * "streams-countmovieTopic-output".
 * 
 */

package Streams;

import org.apache.kafka.streams.Topology;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamsCountMovieTopic {

	public static void main(String[] args) throws Exception {
		Properties props = new Properties();

		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-countmovieTopic");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		final StreamsBuilder builder = new StreamsBuilder();

		builder.stream("streams-movie-input").to("streams-countmovieTopic-output");

		/*
		 * Oppure:
		 * 
		 * builder.<String, String>stream("streams-movie-input").flatMapValues(value ->
		 * Arrays.asList(value.split("\\W+"))) .to("streams-countmovieTopic-output");
		 */

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
