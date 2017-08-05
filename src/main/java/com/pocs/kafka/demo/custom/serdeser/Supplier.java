package com.pocs.kafka.demo.custom.serdeser;

import java.util.Date;

public class Supplier {

	private int supplierId;

	private String supplierName;

	private Date startDate;

	public Supplier(final int supplierId, final String supplierName, final Date startDate) {
		
		this.supplierId = supplierId;
		this.supplierName = supplierName;
		this.startDate = startDate;
	}

	public int getSupplierId() {
		return supplierId;
	}

	public void setSupplierId(int supplierId) {
		this.supplierId = supplierId;
	}

	public String getSupplierName() {
		return supplierName;
	}

	public void setSupplierName(String supplierName) {
		this.supplierName = supplierName;
	}

	public Date getStartDate() {
		return startDate;
	}

	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}
}
