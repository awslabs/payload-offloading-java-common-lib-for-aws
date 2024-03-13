package software.amazon.payloadoffloading;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;

public class S3BackedPayloadStoreAsyncTest {
    private static final String S3_BUCKET_NAME = "test-bucket-name";
    private static final String OTHER_S3_BUCKET_NAME = "other-bucket-name";
    private static final String ANY_PAYLOAD = "AnyPayload";
    private static final String ANY_S3_KEY = "AnyS3key";
    private static final String ANY_OTHER_S3_KEY = "AnyOtherS3key";
    private static final String INCORRECT_POINTER_EXCEPTION_MSG = "Failed to read the S3 object pointer from given string";
    private PayloadStoreAsync payloadStore;
    private S3AsyncDao s3AsyncDao;

    @BeforeEach
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
        // Store any payload
        when(s3AsyncDao.storeTextInS3(any(String.class), any(String.class), any(String.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        String anyActualPayloadPointer = payloadStore.storeOriginalPayload(ANY_PAYLOAD).join();

        // Store any other payload and validate that the pointers are different
        String anyOtherActualPayloadPointer = payloadStore.storeOriginalPayload(ANY_PAYLOAD).join();

        ArgumentCaptor<String> anyOtherKeyCaptor = ArgumentCaptor.forClass(String.class);
        verify(s3AsyncDao, times(2)).storeTextInS3(eq(S3_BUCKET_NAME), anyOtherKeyCaptor.capture(), eq(ANY_PAYLOAD));

        String anyS3Key = anyOtherKeyCaptor.getAllValues().get(0);
        String anyOtherS3Key = anyOtherKeyCaptor.getAllValues().get(1);

        PayloadS3Pointer anyExpectedPayloadPointer = new PayloadS3Pointer(S3_BUCKET_NAME, anyS3Key);
        assertEquals(anyExpectedPayloadPointer.toJson(), anyActualPayloadPointer);

        PayloadS3Pointer anyOtherExpectedPayloadPointer = new PayloadS3Pointer(S3_BUCKET_NAME, anyOtherS3Key);
        assertEquals(anyOtherExpectedPayloadPointer.toJson(), anyOtherActualPayloadPointer);

        assertNotEquals(anyS3Key, anyOtherS3Key);
        assertNotEquals(anyActualPayloadPointer, anyOtherActualPayloadPointer);
    }


    @Test
    public void testStoreOriginalPayloadOnS3Failure() {
        CompletableFuture<Void> sdkEx = new CompletableFuture<>();
        sdkEx.completeExceptionally(SdkException.create("S3 Exception", new Throwable()));
        when(s3AsyncDao.storeTextInS3(any(String.class), any(String.class), any(String.class))).thenReturn(sdkEx);

        CompletionException exception = assertThrows(CompletionException.class, () -> {
            // Any S3 Dao exception is thrown back as-is to clients
            payloadStore.storeOriginalPayload(ANY_PAYLOAD).join();
        });
        assertTrue(exception.getMessage().contains("S3 Exception"));
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
        CompletionException exception = assertThrows(CompletionException.class, () -> {
            // Any S3 Dao exception is thrown back as-is to clients
            payloadStore.getOriginalPayload("IncorrectPointer").join();
        });

        assertTrue(exception.getMessage().contains(INCORRECT_POINTER_EXCEPTION_MSG));
        verifyNoInteractions(s3AsyncDao);
    }


    @Test
    public void testGetOriginalPayloadOnS3Failure() {
        CompletableFuture<String> sdkEx = new CompletableFuture<>();
        sdkEx.completeExceptionally(SdkException.create("S3 Exception", new Throwable()));
        when(s3AsyncDao.getTextFromS3(any(String.class), any(String.class))).thenReturn(sdkEx);

        PayloadS3Pointer anyPointer = new PayloadS3Pointer(S3_BUCKET_NAME, ANY_S3_KEY);

        CompletionException exception = assertThrows(CompletionException.class, () -> {
            // Any S3 Dao exception is thrown back as-is to clients
            payloadStore.getOriginalPayload(anyPointer.toJson()).join();
        });

        assertTrue(exception.getMessage().contains("S3 Exception"));
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
        CompletionException exception = assertThrows(CompletionException.class, () -> {
            payloadStore.deleteOriginalPayload("IncorrectPointer").join();
        });

        assertTrue(exception.getMessage().contains(INCORRECT_POINTER_EXCEPTION_MSG));
        verifyNoInteractions(s3AsyncDao);
    }

    @Test
    public void testDeleteOriginalPayloadsOnSuccess() {
        when(s3AsyncDao.deletePayloadsFromS3(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        List<String> payloadPointers = new ArrayList<>();
        payloadPointers.add(new PayloadS3Pointer(S3_BUCKET_NAME, ANY_S3_KEY).toJson());
        payloadStore.deleteOriginalPayloads(payloadPointers).join();

        ArgumentCaptor<String> bucketNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Collection> keyCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(s3AsyncDao, times(1)).deletePayloadsFromS3(bucketNameCaptor.capture(), keyCaptor.capture());

        assertEquals(Collections.singletonList(ANY_S3_KEY), keyCaptor.getValue());
        assertEquals(S3_BUCKET_NAME, bucketNameCaptor.getValue());
    }

    @Test
    public void testDeleteOriginalPayloadsSameBucket() {
        when(s3AsyncDao.deletePayloadsFromS3(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        List<String> payloadPointers = new ArrayList<>();
        payloadPointers.add(new PayloadS3Pointer(S3_BUCKET_NAME, ANY_S3_KEY).toJson());
        payloadPointers.add(new PayloadS3Pointer(S3_BUCKET_NAME, ANY_OTHER_S3_KEY).toJson());
        payloadStore.deleteOriginalPayloads(payloadPointers).join();

        ArgumentCaptor<String> bucketNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Collection> keyCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(s3AsyncDao, times(1)).deletePayloadsFromS3(bucketNameCaptor.capture(), keyCaptor.capture());

        assertEquals(Arrays.asList(ANY_S3_KEY, ANY_OTHER_S3_KEY), keyCaptor.getValue());
        assertEquals(S3_BUCKET_NAME, bucketNameCaptor.getValue());
    }

    @Test
    public void testDeleteOriginalPayloadsDifferentBuckets() {
        when(s3AsyncDao.deletePayloadsFromS3(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        List<String> payloadPointers = new ArrayList<>();
        payloadPointers.add(new PayloadS3Pointer(S3_BUCKET_NAME, ANY_S3_KEY).toJson());
        payloadPointers.add(new PayloadS3Pointer(OTHER_S3_BUCKET_NAME, ANY_OTHER_S3_KEY).toJson());
        payloadStore.deleteOriginalPayloads(payloadPointers).join();

        ArgumentCaptor<String> bucketNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Collection> keyCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(s3AsyncDao, times(2)).deletePayloadsFromS3(bucketNameCaptor.capture(), keyCaptor.capture());

        assertEquals(Collections.singletonList(ANY_S3_KEY), keyCaptor.getAllValues().get(0));
        assertEquals(Collections.singletonList(ANY_OTHER_S3_KEY), keyCaptor.getAllValues().get(1));
        assertEquals(S3_BUCKET_NAME, bucketNameCaptor.getAllValues().get(0));
        assertEquals(OTHER_S3_BUCKET_NAME, bucketNameCaptor.getAllValues().get(1));
    }

    @Test
    public void testDeleteOriginalPayloadsIncorrectPointer() {
        List<String> payloadPointers = new ArrayList<>();
        payloadPointers.add(new PayloadS3Pointer(S3_BUCKET_NAME, ANY_S3_KEY).toJson());
        payloadPointers.add("IncorrectPointer");

        CompletionException exception = assertThrows(CompletionException.class, () -> {
            payloadStore.deleteOriginalPayloads(payloadPointers).join();
        });

        assertTrue(exception.getMessage().contains(INCORRECT_POINTER_EXCEPTION_MSG));
        verifyNoInteractions(s3AsyncDao);
    }

}
