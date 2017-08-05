package com.pocs.kafka.demo.partition.rebalance;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class RandomConsumer {

	private static KafkaConsumer<String, String> consumer = null;
	
	private final static Properties props = new Properties();
	
	private final static String TOPIC = "random-producer-topic";
	
	private final static String _BROKER_1 = "localhost:9092";
	private final static String _BROKER_2 = "localhost:9093";
	private final static String KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
	private final static String VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
	private final static String GROUP_NAME = "RG";


	static {
		props.setProperty("bootstrap.servers", _BROKER_1 + "," + _BROKER_2);
		props.setProperty("group.id", GROUP_NAME);
		props.setProperty("key.deserializer", KEY_DESERIALIZER);
		props.setProperty("value.deserializer", VALUE_DESERIALIZER);
		props.setProperty("enable.auto.commit", "false");

	}

	public static void main(String[] args) {
	
		consumer = new KafkaConsumer<String, String>(props);
		
		RebalanceListener rebalanceListener  =new RebalanceListener(consumer);
		
		consumer.subscribe(Arrays.asList(TOPIC), rebalanceListener);
		
		try{
			
			while(true){
				ConsumerRecords<String, String> records = consumer.poll(100);
				for(ConsumerRecord<String, String> record : records){
					
					System.out.println("Topic: "+record.topic() + "Partition: "+record.partition()+ "Offset: "+record.offset());
					rebalanceListener.addOffset(record.topic(), record.partition(), record.offset());
				}
			}
		}catch(Exception e){
			System.out.println(e);
		}finally{
			consumer.close();
		}
	}
}
