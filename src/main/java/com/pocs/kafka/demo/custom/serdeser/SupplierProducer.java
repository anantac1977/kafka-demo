package com.pocs.kafka.demo.custom.serdeser;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.pocs.kafka.demo.MyProducerCallback;

public class SupplierProducer {

	private final static String TOPIC = "supplierTopic";
	private final static Properties props = new Properties();
	private final static String _BROKER_1 = "localhost:9092";
	private final static String _BROKER_2 = "localhost:9093";
	private final static String KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
	private final static String VALUE_SERIALIZER = "com.pocs.kafka.demo.custom.serdeser.SupplierSerializer";

	private Producer<String, Supplier> kafkaProducer = null;
	private ProducerRecord<String, Supplier> producerRecord = null;

	static {
		props.setProperty("bootstrap.servers", _BROKER_1 + "," + _BROKER_2);
		props.setProperty("key.serializer", KEY_SERIALIZER);
		props.setProperty("value.serializer", VALUE_SERIALIZER);
	}

	public static void main(String[] args) throws ParseException {

		SupplierProducer supplierProducer = new SupplierProducer();

		supplierProducer.initialize();

		supplierProducer.startProducingMessages();
	}

	private void startProducingMessages() throws ParseException {

		Map<String, Supplier> messagesMap = new HashMap<String, Supplier>();
		
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		
		for(int i = 0; i < 10; i++){
			messagesMap.put("SUP"+i, new Supplier(i, "XYZ Supplier"+i, df.parse("2017-08-02")));
		}
		sendMessageToKafka(messagesMap);
		kafkaProducer.flush();
		kafkaProducer.close();
	}

	private void initialize() {

		kafkaProducer = new KafkaProducer<String, Supplier>(props);
	}

	private void sendMessageToKafka(final Map<String, Supplier> messagesMap) {

		for (final Map.Entry<String, Supplier> entry: messagesMap.entrySet()) {
			producerRecord = new ProducerRecord<String, Supplier>(TOPIC, entry.getKey(), entry.getValue());
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
