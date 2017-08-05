package com.pocs.kafka.demo.partition.rebalance;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class RebalanceListener implements ConsumerRebalanceListener {

	KafkaConsumer<String, String> consumer = null;
	private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();
	public RebalanceListener(final KafkaConsumer<String, String> consumer) {
		this.consumer = consumer;
	}

	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

		System.out.println("Following Partitions Assigned...");
		for(TopicPartition partition : partitions){
			System.out.println(partition.partition()+",");
		}
		
	}

	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		System.out.println("Following Partitions Revoked...");
		for(TopicPartition partition : partitions){
			System.out.println(partition.partition()+",");
		}
		
		System.out.println("Following Partitions Committed...");
		for(TopicPartition topicPartition : currentOffsets.keySet()){
			System.out.println(topicPartition.partition());
		}
		
		consumer.commitSync(currentOffsets);
		currentOffsets.clear();
	}

	public void addOffset(String topic, int partition, long offset) {

		currentOffsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset, "Commit"));
	}

	public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
		return currentOffsets;
	}

}
