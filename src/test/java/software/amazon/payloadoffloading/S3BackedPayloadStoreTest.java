package software.amazon.payloadoffloading;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class S3BackedPayloadStoreTest {
    private static final String S3_BUCKET_NAME = "test-bucket-name";
    private static final String ANY_PAYLOAD = "AnyPayload";
    private static final String ANY_S3_KEY = "AnyS3key";
    private static final String INCORRECT_POINTER_EXCEPTION_MSG = "Failed to read the S3 object pointer from given string";
    private PayloadStore payloadStore;
    private S3Dao s3Dao;

    @BeforeEach
    public void setup() {
        s3Dao = mock(S3Dao.class);
        payloadStore = new S3BackedPayloadStore(s3Dao, S3_BUCKET_NAME);
    }

    @Test
    public void testStoreOriginalPayloadOnSuccess() {
        String actualPayloadPointer = payloadStore.storeOriginalPayload(ANY_PAYLOAD);

        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ServerSideEncryptionStrategy> sseArgsCaptor = ArgumentCaptor.forClass(ServerSideEncryptionStrategy.class);
        ArgumentCaptor<ObjectCannedACL> cannedArgsCaptor = ArgumentCaptor.forClass(ObjectCannedACL.class);

        verify(s3Dao, times(1)).storeTextInS3(eq(S3_BUCKET_NAME), keyCaptor.capture(),
               eq(ANY_PAYLOAD));

        PayloadS3Pointer expectedPayloadPointer = new PayloadS3Pointer(S3_BUCKET_NAME, keyCaptor.getValue());
        assertEquals(expectedPayloadPointer.toJson(), actualPayloadPointer);
    }

    @Test
    public void testStoreOriginalPayloadWithS3KeyOnSuccess() {
        String actualPayloadPointer = payloadStore.storeOriginalPayload(ANY_PAYLOAD, ANY_S3_KEY);

        verify(s3Dao, times(1)).storeTextInS3(eq(S3_BUCKET_NAME), eq(ANY_S3_KEY),
                eq(ANY_PAYLOAD));

        PayloadS3Pointer expectedPayloadPointer = new PayloadS3Pointer(S3_BUCKET_NAME, ANY_S3_KEY);
        assertEquals(expectedPayloadPointer.toJson(), actualPayloadPointer);
    }

    @Test
    public void testStoreOriginalPayloadDoesAlwaysCreateNewObjects() {
        //Store any payload
        String anyActualPayloadPointer = payloadStore.storeOriginalPayload(ANY_PAYLOAD);

        //Store any other payload and validate that the pointers are different
        String anyOtherActualPayloadPointer = payloadStore.storeOriginalPayload(ANY_PAYLOAD);

        ArgumentCaptor<String> anyOtherKeyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ServerSideEncryptionStrategy> sseArgsCaptor = ArgumentCaptor.forClass(ServerSideEncryptionStrategy.class);
        ArgumentCaptor<ObjectCannedACL> cannedArgsCaptor = ArgumentCaptor.forClass(ObjectCannedACL.class);

        verify(s3Dao, times(2)).storeTextInS3(eq(S3_BUCKET_NAME), anyOtherKeyCaptor.capture(),
                eq(ANY_PAYLOAD));

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
        doThrow(SdkException.create("S3 Exception", new Throwable()))
                .when(s3Dao)
                .storeTextInS3(
                        any(String.class),
                        any(String.class),
                        any(String.class));

        //Any S3 Dao exception is thrown back as-is to clients
        assertThrows(SdkException.class, () -> payloadStore.storeOriginalPayload(ANY_PAYLOAD), "S3 Exception");
    }

    @Test
    public void testGetOriginalPayloadOnSuccess() {
        PayloadS3Pointer anyPointer = new PayloadS3Pointer(S3_BUCKET_NAME, ANY_S3_KEY);
        when(s3Dao.getTextFromS3(any(String.class), any(String.class))).thenReturn(ANY_PAYLOAD);
        String actualPayload = payloadStore.getOriginalPayload(anyPointer.toJson());

        ArgumentCaptor<String> bucketNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        verify(s3Dao, times(1)).getTextFromS3(bucketNameCaptor.capture(), keyCaptor.capture());

        assertEquals(ANY_S3_KEY, keyCaptor.getValue());
        assertEquals(S3_BUCKET_NAME, bucketNameCaptor.getValue());
        assertEquals(ANY_PAYLOAD, actualPayload);
    }

    @Test
    public void testGetOriginalPayloadIncorrectPointer() {
        //Any S3 Dao exception is thrown back as-is to clients
        assertThrows(SdkClientException.class, () -> payloadStore.getOriginalPayload("IncorrectPointer"),
                INCORRECT_POINTER_EXCEPTION_MSG);
        verifyNoInteractions(s3Dao);
    }

    @Test
    public void testGetOriginalPayloadOnS3Failure() {
        when(s3Dao.getTextFromS3(any(String.class), any(String.class)))
                .thenThrow(SdkException.create("S3 Exception", new Throwable()));

        PayloadS3Pointer anyPointer = new PayloadS3Pointer(S3_BUCKET_NAME, ANY_S3_KEY);
        //Any S3 Dao exception is thrown back as-is to clients
        assertThrows(SdkException.class, () -> payloadStore.getOriginalPayload(anyPointer.toJson()),
                "S3 Exception");
    }

    @Test
    public void testDeleteOriginalPayloadOnSuccess() {
        PayloadS3Pointer anyPointer = new PayloadS3Pointer(S3_BUCKET_NAME, ANY_S3_KEY);
        payloadStore.deleteOriginalPayload(anyPointer.toJson());

        ArgumentCaptor<String> bucketNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        verify(s3Dao, times(1)).deletePayloadFromS3(bucketNameCaptor.capture(), keyCaptor.capture());

        assertEquals(ANY_S3_KEY, keyCaptor.getValue());
        assertEquals(S3_BUCKET_NAME, bucketNameCaptor.getValue());
    }

    @Test
    public void testDeleteOriginalPayloadIncorrectPointer() {
        assertThrows(SdkClientException.class, () -> payloadStore.deleteOriginalPayload("IncorrectPointer"),
                INCORRECT_POINTER_EXCEPTION_MSG);
        verifyNoInteractions(s3Dao);
    }
}