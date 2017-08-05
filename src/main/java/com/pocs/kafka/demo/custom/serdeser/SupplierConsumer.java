package com.pocs.kafka.demo.custom.serdeser;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SupplierConsumer {

	private final static Properties props = new Properties();

	private final static String TOPIC = "supplierTopic";
	private final static String GROUP_NAME = "supplierTopicGroup";
	private final static String _BROKER_1 = "localhost:9092";
	private final static String _BROKER_2 = "localhost:9093";
	private final static String KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
	private final static String VALUE_DESERIALIZER = "com.pocs.kafka.demo.custom.serdeser.SupplierDeserializer";

	static {
		props.setProperty("bootstrap.servers", _BROKER_1 + "," + _BROKER_2);
		props.setProperty("group.id", GROUP_NAME);
		props.setProperty("key.deserializer", KEY_DESERIALIZER);
		props.setProperty("value.deserializer", VALUE_DESERIALIZER);
		props.setProperty("enable.auto.commit", "false");

	}

	public static void main(String[] args) {

		KafkaConsumer<String, Supplier> consumer = null;
		try {
			consumer = new KafkaConsumer<String, Supplier>(props);
			consumer.subscribe(Arrays.asList(TOPIC));

			while (true) {
				ConsumerRecords<String, Supplier> consumerRecords = consumer.poll(100);

				for (ConsumerRecord<String, Supplier> consumerRecord : consumerRecords) {
					System.out.println("Supplier Id = "
							+ String.valueOf(consumerRecord.value().getSupplierId() + " Supplier Name = "
									+ consumerRecord.value().getSupplierName())
							+ " Start Date = " + String.valueOf(consumerRecord.value().getStartDate()));
				}
				consumer.commitAsync();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			consumer.commitSync();
			consumer.close();
		}
	}
}
