package software.amazon.payloadoffloading;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class S3DaoTest {

    private static final String s3ServerSideEncryptionKMSKeyId = "test-customer-managed-kms-key-id";
    private static final String S3_BUCKET_NAME = "test-bucket-name";
    private static final String ANY_PAYLOAD = "AnyPayload";
    private static final String ANY_S3_KEY = "AnyS3key";
    private final ServerSideEncryptionStrategy serverSideEncryptionStrategy = ServerSideEncryptionFactory.awsManagedCmk();
    private final ObjectCannedACL objectCannedACL = ObjectCannedACL.PUBLIC_READ;
    private S3Client s3Client;
    private S3Dao dao;

    @BeforeEach
    public void setup() {
        s3Client = mock(S3Client.class);
    }

    @Test
    public void storeTextInS3WithoutSSEOrCannedTest() {
        dao = new S3Dao(s3Client);
        ArgumentCaptor<PutObjectRequest> argument = ArgumentCaptor.forClass(PutObjectRequest.class);

        dao.storeTextInS3(S3_BUCKET_NAME, ANY_S3_KEY, ANY_PAYLOAD);

        verify(s3Client, times(1)).putObject(argument.capture(), any(RequestBody.class));

        assertNull(argument.getValue().serverSideEncryption());
        assertNull(argument.getValue().acl());
        assertEquals(S3_BUCKET_NAME, argument.getValue().bucket());
    }

    @Test
    public void storeTextInS3WithSSETest() {
        dao = new S3Dao(s3Client, serverSideEncryptionStrategy, null);
        ArgumentCaptor<PutObjectRequest> argument = ArgumentCaptor.forClass(PutObjectRequest.class);

        dao.storeTextInS3(S3_BUCKET_NAME, ANY_S3_KEY, ANY_PAYLOAD);

        verify(s3Client, times(1)).putObject(argument.capture(), any(RequestBody.class));

        assertEquals(ServerSideEncryption.AWS_KMS, argument.getValue().serverSideEncryption());
        assertNull(argument.getValue().acl());
        assertEquals(S3_BUCKET_NAME, argument.getValue().bucket());
    }

    @Test
    public void storeTextInS3WithBothTest() {
        dao = new S3Dao(s3Client, serverSideEncryptionStrategy, objectCannedACL);
        ArgumentCaptor<PutObjectRequest> argument = ArgumentCaptor.forClass(PutObjectRequest.class);

        dao.storeTextInS3(S3_BUCKET_NAME, ANY_S3_KEY, ANY_PAYLOAD);

        verify(s3Client, times(1)).putObject(argument.capture(), any(RequestBody.class));

        assertEquals(ServerSideEncryption.AWS_KMS, argument.getValue().serverSideEncryption());
        assertEquals(objectCannedACL, argument.getValue().acl());
        assertEquals(S3_BUCKET_NAME, argument.getValue().bucket());
    }
}