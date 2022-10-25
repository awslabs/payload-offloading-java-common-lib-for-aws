package software.amazon.payloadoffloading;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import junitparams.JUnitParamsRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

@RunWith(JUnitParamsRunner.class)
public class S3AsyncDaoTest {

    private static String s3ServerSideEncryptionKMSKeyId = "test-customer-managed-kms-key-id";
    private static final String S3_BUCKET_NAME = "test-bucket-name";
    private static final String ANY_PAYLOAD = "AnyPayload";
    private static final String ANY_S3_KEY = "AnyS3key";
    private ServerSideEncryptionStrategy serverSideEncryptionStrategy = ServerSideEncryptionFactory.awsManagedCmk();
    private ObjectCannedACL objectCannedACL = ObjectCannedACL.PUBLIC_READ;
    private S3AsyncClient s3AsyncClient;
    private S3AsyncDao dao;

    @Before
    public void setup() {
        s3AsyncClient = mock(S3AsyncClient.class);
    }

    @Test
    public void storeTextInS3WithoutSSEOrCannedTest() {
        dao = new S3AsyncDao(s3AsyncClient);
        when(s3AsyncClient.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class))).thenReturn(
            CompletableFuture.completedFuture(null));
        ArgumentCaptor<PutObjectRequest> argument = ArgumentCaptor.forClass(PutObjectRequest.class);

        dao.storeTextInS3(S3_BUCKET_NAME, ANY_S3_KEY, ANY_PAYLOAD).join();

        verify(s3AsyncClient, times(1)).putObject(argument.capture(), any(AsyncRequestBody.class));

        assertNull(argument.getValue().serverSideEncryption());
        assertNull(argument.getValue().acl());
        assertEquals(S3_BUCKET_NAME, argument.getValue().bucket());
    }

    @Test
    public void storeTextInS3WithSSETest() {
        dao = new S3AsyncDao(s3AsyncClient, serverSideEncryptionStrategy, null);
        when(s3AsyncClient.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class))).thenReturn(
            CompletableFuture.completedFuture(null));
        ArgumentCaptor<PutObjectRequest> argument = ArgumentCaptor.forClass(PutObjectRequest.class);

        dao.storeTextInS3(S3_BUCKET_NAME, ANY_S3_KEY, ANY_PAYLOAD).join();

        verify(s3AsyncClient, times(1)).putObject(argument.capture(), any(AsyncRequestBody.class));

        assertEquals(ServerSideEncryption.AWS_KMS, argument.getValue().serverSideEncryption());
        assertNull(argument.getValue().acl());
        assertEquals(S3_BUCKET_NAME, argument.getValue().bucket());
    }

    @Test
    public void storeTextInS3WithBothTest() {
        dao = new S3AsyncDao(s3AsyncClient, serverSideEncryptionStrategy, objectCannedACL);
        when(s3AsyncClient.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class))).thenReturn(
            CompletableFuture.completedFuture(null));
        ArgumentCaptor<PutObjectRequest> argument = ArgumentCaptor.forClass(PutObjectRequest.class);

        dao.storeTextInS3(S3_BUCKET_NAME, ANY_S3_KEY, ANY_PAYLOAD).join();

        verify(s3AsyncClient, times(1)).putObject(argument.capture(), any(AsyncRequestBody.class));

        assertEquals(ServerSideEncryption.AWS_KMS, argument.getValue().serverSideEncryption());
        assertEquals(objectCannedACL, argument.getValue().acl());
        assertEquals(S3_BUCKET_NAME, argument.getValue().bucket());
    }

    @Test
    public void getTextTest() {
        dao = new S3AsyncDao(s3AsyncClient);
        when(s3AsyncClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenReturn(
            CompletableFuture.completedFuture(ResponseBytes.fromByteArray(
                GetObjectRequest.builder().build(), ANY_PAYLOAD.getBytes(StandardCharsets.UTF_8))));

        String payload = dao.getTextFromS3(S3_BUCKET_NAME, ANY_S3_KEY).join();

        verify(s3AsyncClient, times(1)).getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class));

        assertEquals(payload, ANY_PAYLOAD);
    }

    @Test
    public void deleteTextTest() {
        dao = new S3AsyncDao(s3AsyncClient);
        when(s3AsyncClient.deleteObject(any(DeleteObjectRequest.class))).thenReturn(
            CompletableFuture.completedFuture(null));

        dao.deletePayloadFromS3(S3_BUCKET_NAME, ANY_S3_KEY).join();

        verify(s3AsyncClient, times(1)).deleteObject(any(DeleteObjectRequest.class));
    }
}
