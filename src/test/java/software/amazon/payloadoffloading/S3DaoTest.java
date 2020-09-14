package software.amazon.payloadoffloading;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import junitparams.JUnitParamsRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(JUnitParamsRunner.class)
public class S3DaoTest {

    private static String s3ServerSideEncryptionKMSKeyId = "test-customer-managed-kms-key-id";
    private static final String S3_BUCKET_NAME = "test-bucket-name";
    private static final String ANY_PAYLOAD = "AnyPayload";
    private static final String ANY_S3_KEY = "AnyS3key";
    private static final Long ANY_PAYLOAD_LENGTH = 300000L;
    private SSEAwsKeyManagementParams sseAwsKeyManagementParams;
    private CannedAccessControlList cannedAccessControlList;
    private AmazonS3 s3Client;
    private S3Dao dao;

    @Before
    public void setup() {
        s3Client = mock(AmazonS3.class);
        sseAwsKeyManagementParams = new SSEAwsKeyManagementParams(s3ServerSideEncryptionKMSKeyId);
        cannedAccessControlList = CannedAccessControlList.BucketOwnerFullControl;
    }

    @Test
    public void storeTextInS3WithoutSSEOrCannedTest() {
        dao = new S3Dao(s3Client);
        ArgumentCaptor<PutObjectRequest> argument = ArgumentCaptor.forClass(PutObjectRequest.class);

        dao.storeTextInS3(S3_BUCKET_NAME, ANY_S3_KEY, ANY_PAYLOAD, ANY_PAYLOAD_LENGTH);

        verify(s3Client, times(1)).putObject(argument.capture());

        assertNull(argument.getValue().getSSEAwsKeyManagementParams());
        assertNull(argument.getValue().getCannedAcl());
        assertEquals(S3_BUCKET_NAME, argument.getValue().getBucketName());
    }

    @Test
    public void storeTextInS3WithSSETest() {
        dao = new S3Dao(s3Client, sseAwsKeyManagementParams, null);
        ArgumentCaptor<PutObjectRequest> argument = ArgumentCaptor.forClass(PutObjectRequest.class);

        dao.storeTextInS3(S3_BUCKET_NAME, ANY_S3_KEY, ANY_PAYLOAD, ANY_PAYLOAD_LENGTH);

        verify(s3Client, times(1)).putObject(argument.capture());

        assertEquals(sseAwsKeyManagementParams, argument.getValue().getSSEAwsKeyManagementParams());
        assertNull(argument.getValue().getCannedAcl());
        assertEquals(S3_BUCKET_NAME, argument.getValue().getBucketName());
    }

    @Test
    public void storeTextInS3WithBothTest() {
        dao = new S3Dao(s3Client, sseAwsKeyManagementParams, cannedAccessControlList);
        ArgumentCaptor<PutObjectRequest> argument = ArgumentCaptor.forClass(PutObjectRequest.class);

        dao.storeTextInS3(S3_BUCKET_NAME, ANY_S3_KEY, ANY_PAYLOAD, ANY_PAYLOAD_LENGTH);

        verify(s3Client, times(1)).putObject(argument.capture());

        assertEquals(sseAwsKeyManagementParams, argument.getValue().getSSEAwsKeyManagementParams());
        assertEquals(cannedAccessControlList, argument.getValue().getCannedAcl());
        assertEquals(S3_BUCKET_NAME, argument.getValue().getBucketName());
    }
}
