package com.pocs.kafka.demo;

import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class SimpleKafkaProducer {

	private final static String TOPIC = "multi-broker-topic";
	private final static Properties props = new Properties();
	private final static String _BROKER_1 = "localhost:9092";
	private final static String _BROKER_2 = "localhost:9093";
	private final static String KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
	private final static String VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

	private Producer<String, String> kafkaProducer = null;
	private ProducerRecord<String, String> producerRecord = null;

	static {
		props.setProperty("bootstrap.servers", _BROKER_1 + "," + _BROKER_2);
		props.setProperty("key.serializer", KEY_SERIALIZER);
		props.setProperty("value.serializer", VALUE_SERIALIZER);
	}

	public static void main(String[] args) {

		SimpleKafkaProducer simpleKafkaProducer = new SimpleKafkaProducer();

		simpleKafkaProducer.initialize();

		simpleKafkaProducer.startProducingMessages();
	}

	private void startProducingMessages() {

		final Scanner scanner = new Scanner(System.in);

		System.out.println("Simple Kafka Producer... Started");

		int countOfMessages = 0;

		while (true) {
			System.out.println("Type exit to quit");

			System.out.print("Please enter your Message:\t");

			String message = scanner.nextLine();

			if (message != null && message.equalsIgnoreCase("quit")) {

				scanner.close();
				kafkaProducer.flush();
				kafkaProducer.close();
				break;
			} else if (message != null && !message.trim().equals("")) {

				countOfMessages++;

				sendMessageToKafka(message, countOfMessages);
			}
		}

	}

	private void initialize() {

		kafkaProducer = new KafkaProducer<String, String>(props);
	}

	private void sendMessageToKafka(final String message, final Integer countOfMessages) {

		producerRecord = new ProducerRecord<String, String>(TOPIC, System.currentTimeMillis() + "", message);

		if (countOfMessages % 2 == 0) {
			fireAndForget();
		} else if (countOfMessages % 3 == 0) {
			synchronousSend();
		} else {
			asynchronousSend();
		}

	}

	private void synchronousSend() {

		// Synchronous when message loss is not affordable
		try {
			RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get(); // or
																						// get(with
																						// timeout)
																						// from
																						// the
																						// Future
																						// Object
			System.out.println("Message sent with Synchronous approach");
			System.out.println("Message went to partition: " + recordMetadata.partition() + "With Offset "
					+ recordMetadata.offset());
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void fireAndForget() {
		// When loosing messages is not critical
		kafkaProducer.send(producerRecord);

		System.out.println("Message sent with Fire And Forget approach");
	}

	private void asynchronousSend() {

		// Fastest & the best approach. But it has a limit of in flight
		// messages. The property "max.in.flight.requests.per.connection"
		// defines how many messages can be send in continuation prior to
		// receiving a response from the broker. The default value of this
		// property is 5.
		kafkaProducer.send(producerRecord, new MyProducerCallback());
	}

}
