package com.amazonaws.largepayloadoffloading;

import com.amazonaws.AmazonClientException;
import com.amazonaws.annotation.NotThreadSafe;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Amazon large payload storage configuration options such as Amazon S3 client,
 * bucket name, and payload size threshold for large payloads.
 */
@NotThreadSafe
public class LargePayloadStorageConfiguration {
    private static final Log LOG = LogFactory.getLog(LargePayloadStorageConfiguration.class);

    private AmazonS3 s3;
    private String s3BucketName;
    private int payloadSizeThreshold = 0;
    private boolean alwaysThroughS3 = false;
    private boolean largePayloadSupport = false;
    /**
     * This field is optional, it is set only when we want to configure S3 Server Side Encryption with KMS.
     */
    private SSEAwsKeyManagementParams sseAwsKeyManagementParams;

    public LargePayloadStorageConfiguration() {
        s3 = null;
        s3BucketName = null;
        sseAwsKeyManagementParams = null;
    }

    public LargePayloadStorageConfiguration(LargePayloadStorageConfiguration other) {
        this.s3 = other.getAmazonS3Client();
        this.s3BucketName = other.getS3BucketName();
        this.sseAwsKeyManagementParams = other.getSSEAwsKeyManagementParams();
        this.largePayloadSupport = other.isLargePayloadSupportEnabled();
        this.alwaysThroughS3 = other.isAlwaysThroughS3();
        this.payloadSizeThreshold = other.getPayloadSizeThreshold();
    }

    /**
     * Enables support for large payloads .
     *
     * @param s3           Amazon S3 client which is going to be used for storing large payload.
     * @param s3BucketName Name of the bucket which is going to be used for storing large payload.
     *                     The bucket must be already created and configured in s3.
     */
    public void setLargePayloadSupportEnabled(AmazonS3 s3, String s3BucketName) {
        if (s3 == null || s3BucketName == null) {
            String errorMessage = "S3 client and/or S3 bucket name cannot be null.";
            LOG.error(errorMessage);
            throw new AmazonClientException(errorMessage);
        }
        if (isLargePayloadSupportEnabled()) {
            LOG.warn("Large-payload support is already enabled. Overwriting AmazonS3Client and S3BucketName.");
        }
        this.s3 = s3;
        this.s3BucketName = s3BucketName;
        this.largePayloadSupport = true;
        LOG.info("Large-payload support enabled.");
    }

    /**
     * Enables support for large payload.
     *
     * @param s3           Amazon S3 client which is going to be used for storing large payloads.
     * @param s3BucketName Name of the bucket which is going to be used for storing large payloads.
     *                     The bucket must be already created and configured in s3.
     * @return the updated LargePayloadStorageConfiguration object.
     */
    public LargePayloadStorageConfiguration withLargePayloadSupportEnabled(AmazonS3 s3, String s3BucketName) {
        setLargePayloadSupportEnabled(s3, s3BucketName);
        return this;
    }

    /**
     * Disables support for large payloads.
     */
    public void setLargePayloadSupportDisabled() {
        s3 = null;
        s3BucketName = null;
        largePayloadSupport = false;
        LOG.info("Large-payload support disabled.");
    }

    /**
     * Disables support for large payload.
     *
     * @return the updated LargePayloadStorageConfiguration object.
     */
    public LargePayloadStorageConfiguration withLargePayloadSupportDisabled() {
        setLargePayloadSupportDisabled();
        return this;
    }

    /**
     * Check if the support for large payloads if enabled.
     *
     * @return true if support for large payloads is enabled.
     */
    public boolean isLargePayloadSupportEnabled() {
        return largePayloadSupport;
    }

    /**
     * Gets the Amazon S3 client which is being used for storing large payloads.
     *
     * @return Reference to the Amazon S3 client which is being used.
     */
    public AmazonS3 getAmazonS3Client() {
        return s3;
    }

    /**
     * Gets the name of the S3 bucket which is being used for storing large payload.
     *
     * @return The name of the bucket which is being used.
     */
    public String getS3BucketName() {
        return s3BucketName;
    }

    /**
     * Gets the S3 SSE-KMS encryption params of S3 objects under configured S3 bucket name.
     *
     * @return The S3 SSE-KMS params used for encryption.
     */
    public SSEAwsKeyManagementParams getSSEAwsKeyManagementParams() {
        return sseAwsKeyManagementParams;
    }

    /**
     * Sets the the S3 SSE-KMS encryption params of S3 objects under configured S3 bucket name.
     *
     * @param sseAwsKeyManagementParams The S3 SSE-KMS params used for encryption.
     */
    public void setSSEAwsKeyManagementParams(SSEAwsKeyManagementParams sseAwsKeyManagementParams) {
        this.sseAwsKeyManagementParams = sseAwsKeyManagementParams;
    }

    /**
     * Sets the the S3 SSE-KMS encryption params of S3 objects under configured S3 bucket name.
     *
     * @param sseAwsKeyManagementParams The S3 SSE-KMS params used for encryption.
     * @return the updated LargePayloadStorageConfiguration object
     */
    public LargePayloadStorageConfiguration withSSEAwsKeyManagementParams(SSEAwsKeyManagementParams sseAwsKeyManagementParams) {
        setSSEAwsKeyManagementParams(sseAwsKeyManagementParams);
        return this;
    }

    /**
     * Sets the payload size threshold for storing payloads in Amazon S3.
     *
     * @param payloadSizeThreshold Payload size threshold to be used for storing in Amazon S3.
     *                             Default: 256KB.
     * @return the updated LargePayloadStorageConfiguration object.
     */
    public LargePayloadStorageConfiguration withPayloadSizeThreshold(int payloadSizeThreshold) {
        setPayloadSizeThreshold(payloadSizeThreshold);
        return this;
    }

    /**
     * Gets the payload size threshold for storing payloads in Amazon S3.
     *
     * @return payload size threshold which is being used for storing in Amazon S3. Default: 256KB.
     */
    public int getPayloadSizeThreshold() {
        return payloadSizeThreshold;
    }

    /**
     * Sets the payload size threshold for storing payloads in Amazon S3.
     *
     * @param payloadSizeThreshold Payload size threshold to be used for storing in Amazon S3.
     *                             Default: 256KB.
     */
    public void setPayloadSizeThreshold(int payloadSizeThreshold) {
        this.payloadSizeThreshold = payloadSizeThreshold;
    }

    /**
     * Sets whether or not all payloads regardless of their size should be stored in Amazon S3.
     *
     * @param alwaysThroughS3 Whether or not all payloads regardless of their size
     *                        should be stored in Amazon S3. Default: false
     * @return the updated LargePayloadStorageConfiguration object.
     */
    public LargePayloadStorageConfiguration withAlwaysThroughS3(boolean alwaysThroughS3) {
        setAlwaysThroughS3(alwaysThroughS3);
        return this;
    }

    /**
     * Checks whether or not all payloads regardless of their size are being stored in Amazon S3.
     *
     * @return True if all payloads regardless of their size are being stored in Amazon S3. Default: false
     */
    public boolean isAlwaysThroughS3() {
        return alwaysThroughS3;
    }

    /**
     * Sets whether or not all payloads regardless of their size should be stored in Amazon S3.
     *
     * @param alwaysThroughS3 Whether or not all payloads regardless of their size
     *                        should be stored in Amazon S3. Default: false
     */
    public void setAlwaysThroughS3(boolean alwaysThroughS3) {
        this.alwaysThroughS3 = alwaysThroughS3;
    }
}
