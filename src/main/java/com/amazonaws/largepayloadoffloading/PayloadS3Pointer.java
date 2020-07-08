package com.amazonaws.largepayloadoffloading;

import com.amazonaws.AmazonClientException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class is used for carrying pointer to Amazon S3 objects which contain payloads.
 * For a large payload , an instance of this class will be serialized to JSON and sent
 * through Amazon SQS/SNS.
 */
public class PayloadS3Pointer {
	private static final Log LOG = LogFactory.getLog(PayloadS3Pointer.class);
	private String s3BucketName;
	private String s3Key;

	// Needed for Jackson
	private PayloadS3Pointer() {
	}

	public PayloadS3Pointer(String s3BucketName, String s3Key) {
		this.s3BucketName = s3BucketName;
		this.s3Key = s3Key;
	}

	public String getS3BucketName() {
		return s3BucketName;
	}

	public String getS3Key() {
		return s3Key;
	}

	public String toJson() {
		String s3PointerStr = null;
		try {
			JsonDataConverter jsonDataConverter = new JsonDataConverter();
			s3PointerStr = jsonDataConverter.serializeToJson(this);

		} catch (Exception e) {
			String errorMessage = "Failed to convert S3 object pointer to text.";
			LOG.error(errorMessage, e);
			throw new AmazonClientException(errorMessage, e);
		}
		return s3PointerStr;
	}

	public static PayloadS3Pointer fromJson(String s3PointerJson) {

		PayloadS3Pointer s3Pointer = null;
		try {
			JsonDataConverter jsonDataConverter = new JsonDataConverter();
			s3Pointer = jsonDataConverter.deserializeFromJson(s3PointerJson, PayloadS3Pointer.class);

		} catch (Exception e) {
			String errorMessage = "Failed to read the S3 object pointer from given string.";
			LOG.error(errorMessage, e);
			throw new AmazonClientException(errorMessage, e);
		}
		return s3Pointer;
	}
}
