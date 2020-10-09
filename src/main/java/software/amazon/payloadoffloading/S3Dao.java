package software.amazon.payloadoffloading;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.utils.IoUtils;

import java.io.IOException;

/**
 * Dao layer to access S3.
 */
public class S3Dao {
    private static final Logger LOG = LoggerFactory.getLogger(S3Dao.class);
    private final S3Client s3Client;
    private final ServerSideEncryptionStrategy serverSideEncryptionStrategy;
    private final ObjectCannedACL objectCannedACL;

    public S3Dao(S3Client s3Client) {
        this(s3Client, null, null);
    }

    public S3Dao(S3Client s3Client, ServerSideEncryptionStrategy serverSideEncryptionStrategy, ObjectCannedACL objectCannedACL) {
        this.s3Client = s3Client;
        this.serverSideEncryptionStrategy = serverSideEncryptionStrategy;
        this.objectCannedACL = objectCannedACL;
    }

    public String getTextFromS3(String s3BucketName, String s3Key) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(s3BucketName)
                .key(s3Key)
                .build();

        ResponseInputStream<GetObjectResponse> object = null;
        try {
            object = s3Client.getObject(getObjectRequest);
        } catch (SdkException e) {
            String errorMessage = "Failed to get the S3 object which contains the payload.";
            LOG.error(errorMessage, e);
            throw SdkException.create(errorMessage, e);
        }

        String embeddedText;
        try {
            embeddedText = IoUtils.toUtf8String(object);
        } catch (IOException e) {
            String errorMessage = "Failure when handling the message which was read from S3 object.";
            LOG.error(errorMessage, e);
            throw SdkClientException.create(errorMessage, e);

        } finally {
            IoUtils.closeQuietly(object, LOG);
        }

        return embeddedText;
    }

    public void storeTextInS3(String s3BucketName, String s3Key, String payloadContentStr) {
        PutObjectRequest.Builder putObjectRequestBuilder = PutObjectRequest.builder()
                .bucket(s3BucketName)
                .key(s3Key);

        if (objectCannedACL != null) {
            putObjectRequestBuilder.acl(objectCannedACL);
        }

        // https://docs.aws.amazon.com/AmazonS3/latest/dev/kms-using-sdks.html
        if (serverSideEncryptionStrategy != null) {
            serverSideEncryptionStrategy.decorate(putObjectRequestBuilder);
        }

        try {
            s3Client.putObject(putObjectRequestBuilder.build(), RequestBody.fromString(payloadContentStr));
        } catch (SdkException e) {
            String errorMessage = "Failed to store the message content in an S3 object.";
            LOG.error(errorMessage, e);
            throw SdkException.create(errorMessage, e);
        }
    }

    public void deletePayloadFromS3(String s3BucketName, String s3Key) {
        try {
            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                    .bucket(s3BucketName)
                    .key(s3Key)
                    .build();
            s3Client.deleteObject(deleteObjectRequest);

        } catch (SdkException e) {
            String errorMessage = "Failed to delete the S3 object which contains the payload";
            LOG.error(errorMessage, e);
            throw SdkException.create(errorMessage, e);
        }

        LOG.info("S3 object deleted, Bucket name: " + s3BucketName + ", Object key: " + s3Key + ".");
    }
}
