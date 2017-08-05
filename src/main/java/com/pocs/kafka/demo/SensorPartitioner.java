package com.pocs.kafka.demo;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

public class SensorPartitioner implements Partitioner {

	private String speedSensorName;
	
	public void configure(Map<String, ?> configs) {

		speedSensorName = configs.get("speed.sensor.name").toString();
	}

	public void close() {
		// TODO Auto-generated method stub

	}

	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

		final List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
		final int numPartitions = partitions.size();
		Integer sp = (int) Math.abs(numPartitions * 0.3);
		
		int p = 0;
		
		if(keyBytes == null ){
			throw new InvalidRecordException("All records must have sensor name as the key");
		}
		
		if(((String) key).startsWith(speedSensorName)){
			
			p = Utils.toPositive(Utils.murmur2(valueBytes)) % sp;
			
		}else{

			p = Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - sp) + sp;
			
		}
		System.out.println("Key = "+ (String) key + " Partition = "+ p);
		return p;
	}

}
