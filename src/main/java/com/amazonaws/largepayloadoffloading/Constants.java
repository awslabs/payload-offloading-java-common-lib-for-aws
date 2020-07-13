package com.amazonaws.largepayloadoffloading;

public class Constants {
    /**
	 * RESERVED_ATTRIBUTE_NAME is shared between SQSExtendedClient and SNSExtendedClient as an attribute key.
	 * This attribute indicates that the message is a pointer to the stored payload and its value shows size of actual payload.
     */
    public static final String RESERVED_ATTRIBUTE_NAME = "AWSOffloadedPayloadSize";
}
