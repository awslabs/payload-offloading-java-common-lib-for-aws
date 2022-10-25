package software.amazon.payloadoffloading;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.annotations.NotThreadSafe;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;

/**
 * <p>Amazon payload storage configuration options such as Amazon S3 client,
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
public class PayloadStorageConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(PayloadStorageConfiguration.class);

    private S3Client s3;
    private S3AsyncClient s3Async;
    private String s3BucketName;
    private int payloadSizeThreshold = 0;
    private boolean alwaysThroughS3 = false;
    private boolean payloadSupport = false;
    /**
     * This field is optional, it is set only when we want to configure S3 Server Side Encryption with KMS.
     */
    private ServerSideEncryptionStrategy serverSideEncryptionStrategy;
    /**
     * This field is optional, it is set only when we want to add access control list to Amazon S3 buckets and objects
     */
    private ObjectCannedACL objectCannedACL;

    public PayloadStorageConfiguration() {
        s3 = null;
        s3BucketName = null;
        serverSideEncryptionStrategy = null;
        objectCannedACL = null;
    }

    public PayloadStorageConfiguration(PayloadStorageConfiguration other) {
        this.s3 = other.getS3Client();
        this.s3Async = other.getS3AsyncClient();
        this.s3BucketName = other.getS3BucketName();
        this.payloadSupport = other.isPayloadSupportEnabled();
        this.alwaysThroughS3 = other.isAlwaysThroughS3();
        this.payloadSizeThreshold = other.getPayloadSizeThreshold();
        this.serverSideEncryptionStrategy = other.getServerSideEncryptionStrategy();
        this.objectCannedACL = other.getObjectCannedACL();
    }

    /**
     * Enables support for payloads .
     *
     * @param s3           Amazon S3 client which is going to be used for storing payload.
     * @param s3BucketName Name of the bucket which is going to be used for storing payload.
     *                     The bucket must be already created and configured in s3.
     */
    public void setPayloadSupportEnabled(S3Client s3, String s3BucketName) {
        if (s3 == null || s3BucketName == null) {
            String errorMessage = "S3 client and/or S3 bucket name cannot be null.";
            LOG.error(errorMessage);
            throw SdkClientException.create(errorMessage);
        }
        if (isPayloadSupportEnabled()) {
            LOG.warn("Payload support is already enabled. Overwriting AmazonS3Client and S3BucketName.");
        }
        this.s3 = s3;
        this.s3Async = null;
        this.s3BucketName = s3BucketName;
        this.payloadSupport = true;
        LOG.info("Payload support enabled.");
    }

    /**
     * Enables support for payload.
     *
     * @param s3           Amazon S3 client which is going to be used for storing payloads.
     * @param s3BucketName Name of the bucket which is going to be used for storing payloads.
     *                     The bucket must be already created and configured in s3.
     * @return the updated PayloadStorageConfiguration object.
     */
    public PayloadStorageConfiguration withPayloadSupportEnabled(S3Client s3, String s3BucketName) {
        setPayloadSupportEnabled(s3, s3BucketName);
        return this;
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
        if (isPayloadSupportEnabled()) {
            LOG.warn("Payload support is already enabled. Overwriting AmazonS3Client and S3BucketName.");
        }
        this.s3 = null;
        this.s3Async = s3Async;
        this.s3BucketName = s3BucketName;
        this.payloadSupport = true;
        LOG.info("Payload support enabled.");
    }

    /**
     * Enables support for payload.
     *
     * @param s3Async      Amazon S3 client which is going to be used for storing payload.
     * @param s3BucketName Name of the bucket which is going to be used for storing payloads.
     *                     The bucket must be already created and configured in s3.
     * @return the updated PayloadStorageConfiguration object.
     */
    public PayloadStorageConfiguration withPayloadSupportEnabled(S3AsyncClient s3Async, String s3BucketName) {
        setPayloadSupportEnabled(s3Async, s3BucketName);
        return this;
    }

    /**
     * Disables support for payloads.
     */
    public void setPayloadSupportDisabled() {
        s3 = null;
        s3Async = null;
        s3BucketName = null;
        payloadSupport = false;
        LOG.info("Payload support disabled.");
    }

    /**
     * Disables support for payload.
     *
     * @return the updated PayloadStorageConfiguration object.
     */
    public PayloadStorageConfiguration withPayloadSupportDisabled() {
        setPayloadSupportDisabled();
        return this;
    }

    /**
     * Check if the support for payloads if enabled.
     *
     * @return true if support for payloads is enabled.
     */
    public boolean isPayloadSupportEnabled() {
        return payloadSupport;
    }

    /**
     * Gets the Amazon S3 client which is being used for storing payloads.
     *
     * @return Reference to the Amazon S3 client which is being used.
     */
    public S3Client getS3Client() {
        return s3;
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
     * Gets the name of the S3 bucket which is being used for storing payload.
     *
     * @return The name of the bucket which is being used.
     */
    public String getS3BucketName() {
        return s3BucketName;
    }

    /**
     * Sets the payload size threshold for storing payloads in Amazon S3.
     *
     * @param payloadSizeThreshold Payload size threshold to be used for storing in Amazon S3.
     *                             Default: 256KB.
     * @return the updated PayloadStorageConfiguration object.
     */
    public PayloadStorageConfiguration withPayloadSizeThreshold(int payloadSizeThreshold) {
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
     * @return the updated PayloadStorageConfiguration object.
     */
    public PayloadStorageConfiguration withAlwaysThroughS3(boolean alwaysThroughS3) {
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

    /**
     * Sets which method of server side encryption should be used, if required.
     *
     * This is optional, it is set only when you want to configure S3 server side encryption with KMS.
     *
     * @param serverSideEncryptionStrategy The method of encryption required for S3 server side encryption with KMS.
     * @return the updated PayloadStorageConfiguration object.
     */
    public PayloadStorageConfiguration withServerSideEncryption(ServerSideEncryptionStrategy serverSideEncryptionStrategy) {
        setServerSideEncryptionStrategy(serverSideEncryptionStrategy);
        return this;
    }

    /**
     * Sets which method of server side encryption should be use, if required.
     *
     * This is optional, it is set only when you want to configure S3 Server Side Encryption with KMS.
     *
     * @param serverSideEncryptionStrategy The method of encryption required for S3 server side encryption with KMS.
     */
    public void setServerSideEncryptionStrategy(ServerSideEncryptionStrategy serverSideEncryptionStrategy) {
        this.serverSideEncryptionStrategy = serverSideEncryptionStrategy;
    }

    /**
     * The method of service side encryption which should be used, if required.
     *
     * @return The server side encryption method required. Default null.
     */
    public ServerSideEncryptionStrategy getServerSideEncryptionStrategy() {
        return this.serverSideEncryptionStrategy;
    }

    /**
     * Configures the ACL to apply to the Amazon S3 putObject request.
     * @param objectCannedACL
     *            The ACL to be used when storing objects in Amazon S3
     */
    public void setObjectCannedACL(ObjectCannedACL objectCannedACL) {
        this.objectCannedACL = objectCannedACL;
    }

    /**
     * Configures the ACL to apply to the Amazon S3 putObject request.
     * @param objectCannedACL
     *            The ACL to be used when storing objects in Amazon S3
     */
    public PayloadStorageConfiguration withObjectCannedACL(ObjectCannedACL objectCannedACL) {
        setObjectCannedACL(objectCannedACL);
        return this;
    }

    /**
     * Checks whether an ACL have been configured for storing objects in Amazon S3.
     * @return True if ACL is defined
     */
    public boolean isObjectCannedACLDefined() {
        return null != objectCannedACL;
    }

    /**
     * Gets the AWS ACL to apply to the Amazon S3 putObject request.
     * @return Amazon S3 object ACL
     */
    public ObjectCannedACL getObjectCannedACL() {
        return objectCannedACL;
    }
}
