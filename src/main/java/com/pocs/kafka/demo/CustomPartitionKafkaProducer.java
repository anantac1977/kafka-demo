package com.pocs.kafka.demo;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Custom Partitioner for customPartitionerTopic.
 * 
 * Use case: Assume we are collecting data from a bunch of sensors. All of the
 * sensors are sending data to the single topic "customPartitionerTopic". Let's
 * plan for 10 partitions for the topic. But consider 3 partitions dedicated for
 * a sensor named TSS. And seven partitions for the other sensors.
 * 
 * @author ananta.choudhury
 *
 */
public class CustomPartitionKafkaProducer {

	private final static String TOPIC = "customPartitionerTopic";
	private final static Properties props = new Properties();
	private final static String _BROKER_1 = "localhost:9092";
	private final static String _BROKER_2 = "localhost:9093";
	private final static String KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
	private final static String VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
	private final static String CUSTOM_PARTITIONER_CLASS = "com.pocs.kafka.demo.SensorPartitioner";

	private Producer<String, String> kafkaProducer = null;
	private ProducerRecord<String, String> producerRecord = null;

	static {
		props.setProperty("bootstrap.servers", _BROKER_1 + "," + _BROKER_2);
		props.setProperty("key.serializer", KEY_SERIALIZER);
		props.setProperty("value.serializer", VALUE_SERIALIZER);
		props.setProperty("partitioner.class", CUSTOM_PARTITIONER_CLASS);
		props.setProperty("speed.sensor.name", "TSS"); //This is not a property of Kafka
	}

	public static void main(String[] args) {

		CustomPartitionKafkaProducer customPartitionKafkaProducer = new CustomPartitionKafkaProducer();

		customPartitionKafkaProducer.initialize();

		customPartitionKafkaProducer.startProducingMessages();
	}

	private void startProducingMessages() {

		Map<String, String> messagesMap = new HashMap<String, String>();
		
		for(int i = 0; i < 10; i++){
			messagesMap.put("SSP"+i, "500"+i);
			messagesMap.put("TSS"+i, "400"+i);
		}
		sendMessageToKafka(messagesMap);
		kafkaProducer.flush();
		kafkaProducer.close();
	}

	private void initialize() {

		kafkaProducer = new KafkaProducer<String, String>(props);
	}

	private void sendMessageToKafka(final Map<String, String> messagesMap) {

		for (final Map.Entry<String, String> entry: messagesMap.entrySet()) {
			producerRecord = new ProducerRecord<String, String>(TOPIC, entry.getKey(), entry.getValue());
			asynchronousSend();
		}
		

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
