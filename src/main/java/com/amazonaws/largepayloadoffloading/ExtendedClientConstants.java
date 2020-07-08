package com.amazonaws.largepayloadoffloading;

public class ExtendedClientConstants {	
	public static final String RESERVED_ATTRIBUTE_NAME = "ExtendedPayloadSize";
	public static final int MAX_ALLOWED_ATTRIBUTES = 10 - 1; // One(for the reserved attribute) less than max limit(10) of SNS/SQS
	public static final int DEFAULT_PAYLOAD_SIZE_THRESHOLD = 262144;
}