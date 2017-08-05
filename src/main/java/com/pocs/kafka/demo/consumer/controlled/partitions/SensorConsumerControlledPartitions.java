package com.pocs.kafka.demo.consumer.controlled.partitions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class SensorConsumerControlledPartitions {

	public static void main(String[] args) {

		final Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092,localhost:9093");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("enable.auto.commit", "false");

		final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

		final String topicName = "customPartitionerTopic";

		final TopicPartition p0 = new TopicPartition(topicName, 0);
		final TopicPartition p1 = new TopicPartition(topicName, 1);
		final TopicPartition p2 = new TopicPartition(topicName, 2);

		consumer.assign(Arrays.asList(p0, p1, p2));

		System.out.println("Current Position: p0=" + consumer.position(p0) + ", " + "p1=" + consumer.position(p1) + ", "
				+ "p2=" + consumer.position(p2));

		consumer.seek(p0, getOffsetFromDB(p0));
		consumer.seek(p1, getOffsetFromDB(p1));
		consumer.seek(p2, getOffsetFromDB(p2));

		System.out.println("New Position: p0=" + consumer.position(p0) + ", " + "p1=" + consumer.position(p1) + ", "
				+ "p2=" + consumer.position(p2));

		System.out.println("Start Fetching Now...");
		int rCount;
		try {

			do {
				ConsumerRecords<String, String> records = consumer.poll(1000);
				rCount = records.count();
				System.out.println("Records Polled = " + rCount);

				for (ConsumerRecord<String, String> record : records) {
					saveAndCommit(consumer, record);
				}
			} while (rCount > 0);
		} catch (Exception e) {
			System.out.println("Exception in Main..." + e);
		} finally {
			consumer.close();
		}

	}

	private static long getOffsetFromDB(TopicPartition p) {
		long offset = 0;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root");
			PreparedStatement stmt = con
					.prepareStatement("select TopicOffsets from tss_offsets where topic_name=? and TopicPartitions=?");

			stmt.setString(1, p.topic());
			stmt.setInt(2, p.partition());
			
			ResultSet rs = stmt.executeQuery();

			if (rs != null && rs.next()) {
				offset = rs.getLong(1);
			}
		} catch (Exception e) {
			System.out.println(e);
		}
		return offset;
	}

	private static void saveAndCommit(KafkaConsumer<String, String> consumer, ConsumerRecord<String, String> record) {

		System.out.println("Topic = " + record.topic() + " Partition = " + record.partition() + " Offset = "
				+ record.offset() + " Key = " + record.key() + " Value = " + record.value());

		try {

			Class.forName("com.mysql.jdbc.Driver");
			Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root");
			con.setAutoCommit(false);

			String insertSQL = "insert into tss_data values(?,?)";

			PreparedStatement pstmt = con.prepareStatement(insertSQL);
			pstmt.setString(1, record.key());
			pstmt.setString(2, record.value());

			String updateSQL = "update tss_offsets set TopicOffsets=? where topic_name=? and TopicPartitions=?";

			PreparedStatement pstmt2 = con.prepareStatement(updateSQL);
			pstmt2.setLong(1, record.offset() + 1);
			pstmt2.setString(2, record.topic());
			pstmt2.setInt(3, record.partition());

			pstmt.executeUpdate();
			pstmt2.executeUpdate();

			con.commit();
			con.close();
		} catch (Exception e) {
			System.out.println(e);
		}
	}
}
