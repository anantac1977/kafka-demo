package com.pocs.kafka.demo;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class MyProducerCallback implements Callback {

	
	public void onCompletion(RecordMetadata recordMetadata, Exception ex) {

		if (ex != null) {

			System.out.println("Asynchronous Producer failed with an Exception " + ex.getMessage());
		} else {
			System.out.println("Asynchronous Producer succeeded in sending the message to partition "
					+ recordMetadata.partition() + " with Offset " + recordMetadata.offset());
		}
	}

}
