package com.pocs.kafka.demo.custom.serdeser;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class SupplierSerializer implements Serializer<Supplier> {

	private String encoding = "UTF8";

	public void close() {
		// Nothing to do

	}

	public void configure(Map<String, ?> configs, boolean isKey) {

		// Nothing to configure
	}

	public byte[] serialize(String topic, Supplier data) {

		int sizeOfName;
		int sizeofDate;
		byte[] serializedName;
		byte[] serializedDate;

		if (data == null) {
			return null;
		}
		try {

			serializedName = data.getSupplierName().getBytes(encoding);
			serializedDate = data.getStartDate().toString().getBytes(encoding);

			sizeOfName = serializedName.length;
			sizeofDate = serializedDate.length;

			ByteBuffer buf = ByteBuffer.allocate(4 + 4 + sizeOfName + 4 + sizeofDate);

			buf.putInt(data.getSupplierId());
			buf.putInt(sizeOfName);
			buf.put(serializedName);
			buf.putInt(sizeofDate);
			buf.put(serializedDate);

			return buf.array();

		} catch (Exception e) {

			throw new SerializationException("Error while Serializing Supplier to byte[]");
		}
	}

}
