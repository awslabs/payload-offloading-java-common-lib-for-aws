package com.amazonaws.payloadoffloading;

import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.UUID;

/**
 * S3 based implementation for PayloadStore.
 */
public class S3BackedPayloadStore implements PayloadStore {
    private static final Log LOG = LogFactory.getLog(S3BackedPayloadStore.class);

    private final String s3BucketName;
    private final S3Dao s3Dao;
    private final SSEAwsKeyManagementParams sseAwsKeyManagementParams;

    public S3BackedPayloadStore(S3Dao s3Dao, String s3BucketName) {
        this(s3Dao, s3BucketName, null);
    }

    public S3BackedPayloadStore(S3Dao s3Dao, String s3BucketName,
                                SSEAwsKeyManagementParams sseAwsKeyManagementParams) {
        this.s3BucketName = s3BucketName;
        this.s3Dao = s3Dao;
        this.sseAwsKeyManagementParams = sseAwsKeyManagementParams;
    }

    @Override
    public String storeOriginalPayload(String payload, Long payloadContentSize) {
        String s3Key = UUID.randomUUID().toString();

        // Store the payload content in S3.
        s3Dao.storeTextInS3(s3BucketName, s3Key, sseAwsKeyManagementParams, payload, payloadContentSize);
        LOG.info("S3 object created, Bucket name: " + s3BucketName + ", Object key: " + s3Key + ".");

        // Convert S3 pointer (bucket name, key, etc) to JSON string
        PayloadS3Pointer s3Pointer = new PayloadS3Pointer(s3BucketName, s3Key);

        return s3Pointer.toJson();
    }

    @Override
    public String getOriginalPayload(String payloadPointer) {
        PayloadS3Pointer s3Pointer = PayloadS3Pointer.fromJson(payloadPointer);

        String s3BucketName = s3Pointer.getS3BucketName();
        String s3Key = s3Pointer.getS3Key();

        String originalPayload = s3Dao.getTextFromS3(s3BucketName, s3Key);
        LOG.info("S3 object read, Bucket name: " + s3BucketName + ", Object key: " + s3Key + ".");
        return originalPayload;
    }

    @Override
    public void deleteOriginalPayload(String payloadPointer) {
        PayloadS3Pointer s3Pointer = PayloadS3Pointer.fromJson(payloadPointer);

        String s3BucketName = s3Pointer.getS3BucketName();
        String s3Key = s3Pointer.getS3Key();
        s3Dao.deletePayloadFromS3(s3BucketName, s3Key);
    }
}
