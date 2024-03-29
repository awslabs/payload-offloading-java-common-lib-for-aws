package software.amazon.payloadoffloading;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.annotations.NotThreadSafe;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;

/**
 * <p>Amazon payload storage configuration options such as synchronous Amazon S3 client,
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
public class PayloadStorageConfiguration extends PayloadStorageConfigurationBase {
    private static final Logger LOG = LoggerFactory.getLogger(PayloadStorageConfiguration.class);

    private S3Client s3;

    public PayloadStorageConfiguration() {
        s3 = null;
    }

    public PayloadStorageConfiguration(PayloadStorageConfiguration other) {
        super(other);
        this.s3 = other.getS3Client();
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
        super.setPayloadSupportEnabled(s3BucketName);
        this.s3 = s3;
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
     * Disables support for payloads.
     */
    public void setPayloadSupportDisabled() {
        super.setPayloadSupportDisabled();
        s3 = null;
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
     * Gets the Amazon S3 client which is being used for storing payloads.
     *
     * @return Reference to the Amazon S3 client which is being used.
     */
    public S3Client getS3Client() {
        return s3;
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
     * Configures the ACL to apply to the Amazon S3 putObject request.
     * @param objectCannedACL
     *            The ACL to be used when storing objects in Amazon S3
     */
    public PayloadStorageConfiguration withObjectCannedACL(ObjectCannedACL objectCannedACL) {
        setObjectCannedACL(objectCannedACL);
        return this;
    }
}
