package com.pocs.kafka.demo.partition.rebalance;

import java.util.Calendar;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class RandomProducer {

	private final static String TOPIC = "random-producer-topic";
	private final static Properties props = new Properties();
	private final static String _BROKER_1 = "localhost:9092";
	private final static String _BROKER_2 = "localhost:9093";
	private final static String KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
	private final static String VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

	private static Producer<String, String> kafkaProducer = null;

	static {
		props.setProperty("bootstrap.servers", _BROKER_1 + "," + _BROKER_2);
		props.setProperty("key.serializer", KEY_SERIALIZER);
		props.setProperty("value.serializer", VALUE_SERIALIZER);
	}

	public static void main(String[] args) {

		kafkaProducer = new KafkaProducer<String, String>(props);

		Random rg = new Random();
		String msg = null;
		Calendar dt = Calendar.getInstance();
		dt.set(2016, 1, 1);

		try {
			while (true) {
				for (int i = 0; i < 100; i++) {
					msg = dt.get(Calendar.YEAR) + "-" + dt.get(Calendar.MONTH) + "-" + dt.get(Calendar.DATE) + ","
							+ rg.nextInt(1000);
					kafkaProducer.send(new ProducerRecord<String, String>(TOPIC, 0, "KEY", msg)).get();

					msg = dt.get(Calendar.YEAR) + "-" + dt.get(Calendar.MONTH) + "-" + dt.get(Calendar.DATE) + ","
							+ rg.nextInt(1000);
					kafkaProducer.send(new ProducerRecord<String, String>(TOPIC, 0, "KEY", msg)).get();

				}

				dt.add(Calendar.DATE, 1);

				System.out.println("Data Sent for " + dt.get(Calendar.YEAR) + "-" + dt.get(Calendar.MONTH) + "-"
						+ dt.get(Calendar.DATE));
			}
		} catch (Exception e) {
			System.out.println("Producer Interrupted " + e);
		} finally {
			kafkaProducer.close();
		}
	}
}
