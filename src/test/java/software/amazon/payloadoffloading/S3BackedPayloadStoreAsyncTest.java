package software.amazon.payloadoffloading;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import junitparams.JUnitParamsRunner;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;

@RunWith(JUnitParamsRunner.class)
public class S3BackedPayloadStoreAsyncTest {
    private static final String S3_BUCKET_NAME = "test-bucket-name";
    private static final String ANY_PAYLOAD = "AnyPayload";
    private static final String ANY_S3_KEY = "AnyS3key";
    private static final String INCORRECT_POINTER_EXCEPTION_MSG = "Failed to read the S3 object pointer from given string";
    private PayloadStoreAsync payloadStore;
    private S3AsyncDao s3AsyncDao;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void setup() {
        s3AsyncDao = mock(S3AsyncDao.class);
        payloadStore = new S3BackedPayloadStoreAsync(s3AsyncDao, S3_BUCKET_NAME);
    }

    @Test
    public void testStoreOriginalPayloadOnSuccess() {
        when(s3AsyncDao.storeTextInS3(any(String.class), any(String.class), any(String.class))).thenReturn(
            CompletableFuture.completedFuture(null));
        String actualPayloadPointer = payloadStore.storeOriginalPayload(ANY_PAYLOAD).join();

        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ServerSideEncryptionStrategy> sseArgsCaptor = ArgumentCaptor.forClass(ServerSideEncryptionStrategy.class);
        ArgumentCaptor<ObjectCannedACL> cannedArgsCaptor = ArgumentCaptor.forClass(ObjectCannedACL.class);

        verify(s3AsyncDao, times(1)).storeTextInS3(eq(S3_BUCKET_NAME), keyCaptor.capture(),
            eq(ANY_PAYLOAD));

        PayloadS3Pointer expectedPayloadPointer = new PayloadS3Pointer(S3_BUCKET_NAME, keyCaptor.getValue());
        assertEquals(expectedPayloadPointer.toJson(), actualPayloadPointer);
    }

    @Test
    public void testStoreOriginalPayloadWithS3KeyOnSuccess() {
        when(s3AsyncDao.storeTextInS3(any(String.class), any(String.class), any(String.class))).thenReturn(
            CompletableFuture.completedFuture(null));
        String actualPayloadPointer = payloadStore.storeOriginalPayload(ANY_PAYLOAD, ANY_S3_KEY).join();

        verify(s3AsyncDao, times(1)).storeTextInS3(eq(S3_BUCKET_NAME), eq(ANY_S3_KEY),
            eq(ANY_PAYLOAD));

        PayloadS3Pointer expectedPayloadPointer = new PayloadS3Pointer(S3_BUCKET_NAME, ANY_S3_KEY);
        assertEquals(expectedPayloadPointer.toJson(), actualPayloadPointer);
    }

    @Test
    public void testStoreOriginalPayloadDoesAlwaysCreateNewObjects() {
        //Store any payload
        when(s3AsyncDao.storeTextInS3(any(String.class), any(String.class), any(String.class))).thenReturn(
            CompletableFuture.completedFuture(null));
        String anyActualPayloadPointer = payloadStore.storeOriginalPayload(ANY_PAYLOAD).join();

        //Store any other payload and validate that the pointers are different
        String anyOtherActualPayloadPointer = payloadStore.storeOriginalPayload(ANY_PAYLOAD).join();

        ArgumentCaptor<String> anyOtherKeyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ServerSideEncryptionStrategy> sseArgsCaptor = ArgumentCaptor.forClass(ServerSideEncryptionStrategy.class);
        ArgumentCaptor<ObjectCannedACL> cannedArgsCaptor = ArgumentCaptor.forClass(ObjectCannedACL.class);

        verify(s3AsyncDao, times(2)).storeTextInS3(eq(S3_BUCKET_NAME), anyOtherKeyCaptor.capture(),
            eq(ANY_PAYLOAD));

        String anyS3Key = anyOtherKeyCaptor.getAllValues().get(0);
        String anyOtherS3Key = anyOtherKeyCaptor.getAllValues().get(1);

        PayloadS3Pointer anyExpectedPayloadPointer = new PayloadS3Pointer(S3_BUCKET_NAME, anyS3Key);
        assertEquals(anyExpectedPayloadPointer.toJson(), anyActualPayloadPointer);

        PayloadS3Pointer anyOtherExpectedPayloadPointer = new PayloadS3Pointer(S3_BUCKET_NAME, anyOtherS3Key);
        assertEquals(anyOtherExpectedPayloadPointer.toJson(), anyOtherActualPayloadPointer);

        assertThat(anyS3Key, Matchers.not(anyOtherS3Key));
        assertThat(anyActualPayloadPointer, Matchers.not(anyOtherActualPayloadPointer));
    }

    @Test
    public void testStoreOriginalPayloadOnS3Failure() {
        CompletableFuture<Void> sdkEx = new CompletableFuture<>();
        sdkEx.completeExceptionally(SdkException.create("S3 Exception", new Throwable()));
        when(s3AsyncDao.storeTextInS3(any(String.class), any(String.class), any(String.class))).thenReturn(sdkEx);

        exception.expect(CompletionException.class);
        exception.expectMessage("S3 Exception");
        //Any S3 Dao exception is thrown back as-is to clients
        payloadStore.storeOriginalPayload(ANY_PAYLOAD).join();
    }

    @Test
    public void testGetOriginalPayloadOnSuccess() {
        PayloadS3Pointer anyPointer = new PayloadS3Pointer(S3_BUCKET_NAME, ANY_S3_KEY);
        when(s3AsyncDao.getTextFromS3(any(String.class), any(String.class))).thenReturn(
            CompletableFuture.completedFuture(ANY_PAYLOAD));
        String actualPayload = payloadStore.getOriginalPayload(anyPointer.toJson()).join();

        ArgumentCaptor<String> bucketNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        verify(s3AsyncDao, times(1)).getTextFromS3(bucketNameCaptor.capture(), keyCaptor.capture());

        assertEquals(ANY_S3_KEY, keyCaptor.getValue());
        assertEquals(S3_BUCKET_NAME, bucketNameCaptor.getValue());
        assertEquals(ANY_PAYLOAD, actualPayload);
    }

    @Test
    public void testGetOriginalPayloadIncorrectPointer() {
        exception.expect(CompletionException.class);
        exception.expectMessage(INCORRECT_POINTER_EXCEPTION_MSG);
        //Any S3 Dao exception is thrown back as-is to clients
        payloadStore.getOriginalPayload("IncorrectPointer").join();
        verifyNoInteractions(s3AsyncDao);
    }

    @Test
    public void testGetOriginalPayloadOnS3Failure() {
        CompletableFuture<String> sdkEx = new CompletableFuture<>();
        sdkEx.completeExceptionally(SdkException.create("S3 Exception", new Throwable()));
        when(s3AsyncDao.getTextFromS3(any(String.class), any(String.class))).thenReturn(sdkEx);
        exception.expect(CompletionException.class);
        exception.expectMessage("S3 Exception");
        //Any S3 Dao exception is thrown back as-is to clients
        PayloadS3Pointer anyPointer = new PayloadS3Pointer(S3_BUCKET_NAME, ANY_S3_KEY);
        payloadStore.getOriginalPayload(anyPointer.toJson()).join();
    }

    @Test
    public void testDeleteOriginalPayloadOnSuccess() {
        when(s3AsyncDao.deletePayloadFromS3(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
        PayloadS3Pointer anyPointer = new PayloadS3Pointer(S3_BUCKET_NAME, ANY_S3_KEY);
        payloadStore.deleteOriginalPayload(anyPointer.toJson()).join();

        ArgumentCaptor<String> bucketNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        verify(s3AsyncDao, times(1)).deletePayloadFromS3(bucketNameCaptor.capture(), keyCaptor.capture());

        assertEquals(ANY_S3_KEY, keyCaptor.getValue());
        assertEquals(S3_BUCKET_NAME, bucketNameCaptor.getValue());
    }

    @Test
    public void testDeleteOriginalPayloadIncorrectPointer() {
        exception.expect(CompletionException.class);
        exception.expectMessage(INCORRECT_POINTER_EXCEPTION_MSG);
        payloadStore.deleteOriginalPayload("IncorrectPointer").join();
        verifyNoInteractions(s3AsyncDao);
    }
}
