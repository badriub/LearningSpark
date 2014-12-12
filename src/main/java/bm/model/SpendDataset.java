package bm.model;

import java.io.Serializable;

public class SpendDataset implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private String clientCode;
	private String paymentType;
	private String paymentDate;

	public String getClientCode() {
		return clientCode;
	}

	public void setClientCode(String clientCode) {
		this.clientCode = clientCode;
	}

	public String getPaymentType() {
		return paymentType;
	}

	public void setPaymentType(String paymentType) {
		this.paymentType = paymentType;
	}

	public String getPaymentDate() {
		return paymentDate;
	}

	public void setPaymentDate(String paymentDate) {
		this.paymentDate = paymentDate;
	}
}
