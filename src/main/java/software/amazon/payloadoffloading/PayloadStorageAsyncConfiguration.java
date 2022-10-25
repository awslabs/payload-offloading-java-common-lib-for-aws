package software.amazon.payloadoffloading;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.annotations.NotThreadSafe;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;

/**
 * <p>Amazon payload storage configuration options such as asynchronous Amazon S3 client,
 * bucket name, and payload size threshold for payloads.</p>
 *
 * <p>Server side encryption is optional and can be enabled using with {@link #withServerSideEncryption(ServerSideEncryptionStrategy)}
 * or {@link #setServerSideEncryptionStrategy(ServerSideEncryptionStrategy)}</p>
 *
 * <p>There are two possible options for server side encrption. This can be using a customer managed key or AWS managed CMK.</p>
 *
 * Example usage:
 *
 * <pre>
 *     withServerSideEncryption(ServerSideEncrptionFactory.awsManagedCmk())
 * </pre>
 *
 * or
 *
 * <pre>
 *     withServerSideEncryption(ServerSideEncrptionFactory.customerKey(YOUR_CUSTOMER_ID))
 * </pre>
 *
 * @see software.amazon.payloadoffloading.ServerSideEncryptionFactory
 */
@NotThreadSafe
public class PayloadStorageAsyncConfiguration extends PayloadStorageConfigurationBase {
    private static final Logger LOG = LoggerFactory.getLogger(PayloadStorageAsyncConfiguration.class);

    private S3AsyncClient s3Async;

    public PayloadStorageAsyncConfiguration() {
        s3Async = null;
    }

    public PayloadStorageAsyncConfiguration(PayloadStorageAsyncConfiguration other) {
        super(other);
        this.s3Async = other.getS3AsyncClient();
    }

    /**
     * Enables support for payloads using asynchronous storage.
     *
     * @param s3Async      Amazon S3 client which is going to be used for storing payload.
     * @param s3BucketName Name of the bucket which is going to be used for storing payload.
     *                     The bucket must be already created and configured in s3.
     */
    public void setPayloadSupportEnabled(S3AsyncClient s3Async, String s3BucketName) {
        if (s3Async == null || s3BucketName == null) {
            String errorMessage = "S3 client and/or S3 bucket name cannot be null.";
            LOG.error(errorMessage);
            throw SdkClientException.create(errorMessage);
        }
        super.setPayloadSupportEnabled(s3BucketName);
        this.s3Async = s3Async;
    }

    /**
     * Enables support for payload.
     *
     * @param s3Async      Amazon S3 client which is going to be used for storing payload.
     * @param s3BucketName Name of the bucket which is going to be used for storing payloads.
     *                     The bucket must be already created and configured in s3.
     * @return the updated PayloadStorageAsyncConfiguration object.
     */
    public PayloadStorageAsyncConfiguration withPayloadSupportEnabled(S3AsyncClient s3Async, String s3BucketName) {
        setPayloadSupportEnabled(s3Async, s3BucketName);
        return this;
    }

    /**
     * Disables support for payloads.
     */
    public void setPayloadSupportDisabled() {
        super.setPayloadSupportDisabled();
        s3Async = null;
        LOG.info("Payload support disabled.");
    }

    /**
     * Disables support for payload.
     *
     * @return the updated PayloadStorageAsyncConfiguration object.
     */
    public PayloadStorageAsyncConfiguration withPayloadSupportDisabled() {
        setPayloadSupportDisabled();
        return this;
    }

    /**
     * Gets the Amazon S3 async client which is being used for storing payloads.
     *
     * @return Reference to the Amazon S3 async client which is being used.
     */
    public S3AsyncClient getS3AsyncClient() {
        return s3Async;
    }

    /**
     * Sets the payload size threshold for storing payloads in Amazon S3.
     *
     * @param payloadSizeThreshold Payload size threshold to be used for storing in Amazon S3.
     *                             Default: 256KB.
     * @return the updated PayloadStorageAsyncConfiguration object.
     */
    public PayloadStorageAsyncConfiguration withPayloadSizeThreshold(int payloadSizeThreshold) {
        setPayloadSizeThreshold(payloadSizeThreshold);
        return this;
    }

    /**
     * Sets whether or not all payloads regardless of their size should be stored in Amazon S3.
     *
     * @param alwaysThroughS3 Whether or not all payloads regardless of their size
     *                        should be stored in Amazon S3. Default: false
     * @return the updated PayloadStorageAsyncConfiguration object.
     */
    public PayloadStorageAsyncConfiguration withAlwaysThroughS3(boolean alwaysThroughS3) {
        setAlwaysThroughS3(alwaysThroughS3);
        return this;
    }

    /**
     * Sets which method of server side encryption should be used, if required.
     *
     * This is optional, it is set only when you want to configure S3 server side encryption with KMS.
     *
     * @param serverSideEncryptionStrategy The method of encryption required for S3 server side encryption with KMS.
     * @return the updated PayloadStorageAsyncConfiguration object.
     */
    public PayloadStorageAsyncConfiguration withServerSideEncryption(ServerSideEncryptionStrategy serverSideEncryptionStrategy) {
        setServerSideEncryptionStrategy(serverSideEncryptionStrategy);
        return this;
    }

    /**
     * Configures the ACL to apply to the Amazon S3 putObject request.
     * @param objectCannedACL
     *            The ACL to be used when storing objects in Amazon S3
     */
    public PayloadStorageAsyncConfiguration withObjectCannedACL(ObjectCannedACL objectCannedACL) {
        setObjectCannedACL(objectCannedACL);
        return this;
    }
}
