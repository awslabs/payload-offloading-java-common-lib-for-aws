package software.amazon.payloadoffloading;

import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Dao layer to access S3.
 */
public class S3AsyncDao {
    private static final Logger LOG = LoggerFactory.getLogger(S3AsyncDao.class);
    private final S3AsyncClient s3Client;
    private final ServerSideEncryptionStrategy serverSideEncryptionStrategy;
    private final ObjectCannedACL objectCannedACL;

    public S3AsyncDao(S3AsyncClient s3Client) {
        this(s3Client, null, null);
    }

    public S3AsyncDao(
        S3AsyncClient s3Client,
        ServerSideEncryptionStrategy serverSideEncryptionStrategy,
        ObjectCannedACL objectCannedACL) {
        this.s3Client = s3Client;
        this.serverSideEncryptionStrategy = serverSideEncryptionStrategy;
        this.objectCannedACL = objectCannedACL;
    }

    public CompletableFuture<String> getTextFromS3(String s3BucketName, String s3Key) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(s3BucketName)
                .key(s3Key)
                .build();

        return s3Client.getObject(getObjectRequest, AsyncResponseTransformer.toBytes())
            .thenApply(ResponseBytes::asUtf8String)
            .handle((v, tIn) -> {
                if (tIn != null) {
                    Throwable t = Util.unwrapFutureException(tIn);
                    if (t instanceof SdkException) {
                        String errorMessage = "Failed to get the S3 object which contains the payload.";
                        LOG.error(errorMessage, t);
                        throw SdkException.create(errorMessage, t);
                    }
                    if (t instanceof UncheckedIOException) {
                        String errorMessage = "Failure when handling the message which was read from S3 object.";
                        LOG.error(errorMessage, t);
                        throw SdkClientException.create(errorMessage, t);
                    }
                    throw new CompletionException(t);
                }
                return v;
            });
    }

    public CompletableFuture<Void> storeTextInS3(String s3BucketName, String s3Key, String payloadContentStr) {
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

        return s3Client.putObject(putObjectRequestBuilder.build(), AsyncRequestBody.fromString(payloadContentStr))
            .handle((v, tIn) -> {
                if (tIn != null) {
                    Throwable t = Util.unwrapFutureException(tIn);
                    if (t instanceof SdkException) {
                        String errorMessage = "Failed to store the message content in an S3 object.";
                        LOG.error(errorMessage, t);
                        throw SdkException.create(errorMessage, t);
                    }
                    throw new CompletionException(t);
                }
                return null;
            });
    }

    public CompletableFuture<Void> deletePayloadFromS3(String s3BucketName, String s3Key) {
        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                .bucket(s3BucketName)
                .key(s3Key)
                .build();
        return s3Client.deleteObject(deleteObjectRequest)
            .handle((v, tIn) -> {
                if (tIn != null) {
                    Throwable t = Util.unwrapFutureException(tIn);
                    if (t instanceof SdkException) {
                        String errorMessage = "Failed to delete the S3 object which contains the payload";
                        LOG.error(errorMessage, t);
                        throw SdkException.create(errorMessage, t);
                    }
                    throw new CompletionException(t);
                }

                LOG.info("S3 object deleted, Bucket name: " + s3BucketName + ", Object key: " + s3Key + ".");
                return null;
            });
    }

    public CompletableFuture<Void> deletePayloadsFromS3(String s3BucketName, Collection<String> s3Keys) {
        DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder()
            .bucket(s3BucketName)
            .delete(Delete.builder()
                .objects(s3Keys.stream()
                    .map(s3Key -> ObjectIdentifier.builder()
                        .key(s3Key)
                        .build())
                    .collect(Collectors.toList()))
                .build())
            .build();

        return s3Client.deleteObjects(deleteObjectsRequest)
            .handle((v, tIn) -> {
                if (tIn != null) {
                    Throwable t = Util.unwrapFutureException(tIn);
                    if (t instanceof SdkException) {
                        String errorMessage = "Failed to delete the S3 object which contains the payload";
                        LOG.error(errorMessage, t);
                        throw SdkException.create(errorMessage, t);
                    }
                    throw new CompletionException(t);
                }

                LOG.info("S3 object deleted, Bucket name: " + s3BucketName + ", Object keys: " + s3Keys + ".");
                return null;
            });
    }
}
