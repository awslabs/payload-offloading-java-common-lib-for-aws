package software.amazon.payloadoffloading;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Dao layer to access S3.
 */
public class S3Dao {
    private static final Log LOG = LogFactory.getLog(S3Dao.class);
    private final AmazonS3 s3Client;
    private final SSEAwsKeyManagementParams sseAwsKeyManagementParams;
    private final CannedAccessControlList cannedAccessControlList;

    public S3Dao(AmazonS3 s3Client) {
        this(s3Client, null, null);
    }

    public S3Dao(AmazonS3 s3Client, SSEAwsKeyManagementParams sseAwsKeyManagementParams, CannedAccessControlList cannedAccessControlList) {
        this.s3Client = s3Client;
        this.sseAwsKeyManagementParams = sseAwsKeyManagementParams;
        this.cannedAccessControlList = cannedAccessControlList;
    }

    public String getTextFromS3(String s3BucketName, String s3Key) {
        GetObjectRequest getObjectRequest = new GetObjectRequest(s3BucketName, s3Key);
        String embeddedText = null;
        S3Object obj = null;

        try {
            obj = s3Client.getObject(getObjectRequest);

        } catch (AmazonServiceException e) {
            String errorMessage = "Failed to get the S3 object which contains the payload.";
            LOG.error(errorMessage, e);
            throw new AmazonServiceException(errorMessage, e);

        } catch (AmazonClientException e) {
            String errorMessage = "Failed to get the S3 object which contains the payload.";
            LOG.error(errorMessage, e);
            throw new AmazonClientException(errorMessage, e);
        }

        S3ObjectInputStream is = obj.getObjectContent();

        try {
            embeddedText = IOUtils.toString(is);

        } catch (IOException e) {
            String errorMessage = "Failure when handling the message which was read from S3 object.";
            LOG.error(errorMessage, e);
            throw new AmazonClientException(errorMessage, e);

        } finally {
            IOUtils.closeQuietly(is, LOG);
        }

        return embeddedText;
    }

//    public void storeTextInS3(String s3BucketName, String s3Key, SSEAwsKeyManagementParams sseAwsKeyManagementParams,
//                              CannedAccessControlList cannedAccessControlList, String payloadContentStr, Long payloadContentSize) {
//        InputStream payloadContentStream = new ByteArrayInputStream(payloadContentStr.getBytes(StandardCharsets.UTF_8));
//        ObjectMetadata payloadContentStreamMetadata = new ObjectMetadata();
//        payloadContentStreamMetadata.setContentLength(payloadContentSize);
//        PutObjectRequest putObjectRequest = new PutObjectRequest(s3BucketName, s3Key,
//                payloadContentStream, payloadContentStreamMetadata);
//
//        if (cannedAccessControlList != null) {
//            putObjectRequest.withCannedAcl(cannedAccessControlList);
//        }
//
//        // https://docs.aws.amazon.com/AmazonS3/latest/dev/kms-using-sdks.html
//        if (sseAwsKeyManagementParams != null) {
//            LOG.debug("Using SSE-KMS in put object request.");
//            putObjectRequest.setSSEAwsKeyManagementParams(sseAwsKeyManagementParams);
//        }
//
//        try {
//            s3Client.putObject(putObjectRequest);
//
//        } catch (AmazonServiceException e) {
//            String errorMessage = "Failed to store the message content in an S3 object.";
//            LOG.error(errorMessage, e);
//            throw new AmazonServiceException(errorMessage, e);
//
//        } catch (AmazonClientException e) {
//            String errorMessage = "Failed to store the message content in an S3 object.";
//            LOG.error(errorMessage, e);
//            throw new AmazonClientException(errorMessage, e);
//        }
//    }
//
//    public void storeTextInS3(String s3BucketName, String s3Key, SSEAwsKeyManagementParams sseAwsKeyManagementParams,
//                              String payloadContentStr, Long payloadContentSize) {
//        storeTextInS3(s3BucketName, s3Key, sseAwsKeyManagementParams, null, payloadContentStr, payloadContentSize);
//    }

    public void storeTextInS3(String s3BucketName, String s3Key, String payloadContentStr, Long payloadContentSize) {
        InputStream payloadContentStream = new ByteArrayInputStream(payloadContentStr.getBytes(StandardCharsets.UTF_8));
        ObjectMetadata payloadContentStreamMetadata = new ObjectMetadata();
        payloadContentStreamMetadata.setContentLength(payloadContentSize);
        PutObjectRequest putObjectRequest = new PutObjectRequest(s3BucketName, s3Key,
                payloadContentStream, payloadContentStreamMetadata);

        if (cannedAccessControlList != null) {
            putObjectRequest.withCannedAcl(cannedAccessControlList);
        }

        // https://docs.aws.amazon.com/AmazonS3/latest/dev/kms-using-sdks.html
        if (sseAwsKeyManagementParams != null) {
            LOG.debug("Using SSE-KMS in put object request.");
            putObjectRequest.setSSEAwsKeyManagementParams(sseAwsKeyManagementParams);
        }

        try {
            s3Client.putObject(putObjectRequest);

        } catch (AmazonServiceException e) {
            String errorMessage = "Failed to store the message content in an S3 object.";
            LOG.error(errorMessage, e);
            throw new AmazonServiceException(errorMessage, e);

        } catch (AmazonClientException e) {
            String errorMessage = "Failed to store the message content in an S3 object.";
            LOG.error(errorMessage, e);
            throw new AmazonClientException(errorMessage, e);
        }
    }

    public void deletePayloadFromS3(String s3BucketName, String s3Key) {
        try {
            s3Client.deleteObject(s3BucketName, s3Key);

        } catch (AmazonServiceException e) {
            String errorMessage = "Failed to delete the S3 object which contains the payload";
            LOG.error(errorMessage, e);
            throw new AmazonServiceException(errorMessage, e);

        } catch (AmazonClientException e) {
            String errorMessage = "Failed to delete the S3 object which contains the payload";
            LOG.error(errorMessage, e);
            throw new AmazonClientException(errorMessage, e);
        }

        LOG.info("S3 object deleted, Bucket name: " + s3BucketName + ", Object key: " + s3Key + ".");
    }
}
