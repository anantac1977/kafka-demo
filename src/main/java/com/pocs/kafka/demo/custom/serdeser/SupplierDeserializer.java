package com.pocs.kafka.demo.custom.serdeser;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

public class SupplierDeserializer implements Deserializer<Supplier> {

	private String encoding = "UTF8";

	public void close() {
		// Nothing to do

	}

	public void configure(Map<String, ?> configs, boolean isKey) {

		// Nothing to configure
	}

	public Supplier deserialize(String topic, byte[] data) {

		if (data == null) {

			System.out.println("Null received at deserialize");
			return null;
		}

		try {
			ByteBuffer buf = ByteBuffer.wrap(data);

			int id = buf.getInt();
			int sizeOfName = buf.getInt();
			byte[] nameBytes = new byte[sizeOfName];

			buf.get(nameBytes);

			String deserializedName = new String(nameBytes, encoding);

			int sizeOfDate = buf.getInt();
			byte[] dateBytes = new byte[sizeOfDate];
			buf.get(dateBytes);
			String dateString = new String(dateBytes, encoding);
			DateFormat df = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");

			return new Supplier(id, deserializedName, df.parse(dateString));

		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		
		return null;
	}

}
