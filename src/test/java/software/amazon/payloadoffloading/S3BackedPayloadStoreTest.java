package software.amazon.payloadoffloading;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import junitparams.JUnitParamsRunner;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(JUnitParamsRunner.class)
public class S3BackedPayloadStoreTest {
    private static final String S3_BUCKET_NAME = "test-bucket-name";
    private static final String S3_SERVER_SIDE_ENCRYPTION_KMS_KEY_ID = "test-customer-managed-kms-key-id";
    private static final String ANY_PAYLOAD = "AnyPayload";
    private static final String ANY_S3_KEY = "AnyS3key";
    private static final String INCORRECT_POINTER_EXCEPTION_MSG = "Failed to read the S3 object pointer from given string";
    private static final Long ANY_PAYLOAD_LENGTH = 300000L;
    private PayloadStore payloadStore;
    private S3Dao s3Dao;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void setup() {
        s3Dao = mock(S3Dao.class);
        payloadStore = new S3BackedPayloadStore(s3Dao, S3_BUCKET_NAME);
    }

    @Test
    public void testStoreOriginalPayloadOnSuccess() {
        String actualPayloadPointer = payloadStore.storeOriginalPayload(ANY_PAYLOAD, ANY_PAYLOAD_LENGTH);

        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<SSEAwsKeyManagementParams> sseArgsCaptor = ArgumentCaptor.forClass(SSEAwsKeyManagementParams.class);
        ArgumentCaptor<CannedAccessControlList> cannedArgsCaptor = ArgumentCaptor.forClass(CannedAccessControlList.class);

        verify(s3Dao, times(1)).storeTextInS3(eq(S3_BUCKET_NAME), keyCaptor.capture(),
                eq(ANY_PAYLOAD), eq(ANY_PAYLOAD_LENGTH));

        PayloadS3Pointer expectedPayloadPointer = new PayloadS3Pointer(S3_BUCKET_NAME, keyCaptor.getValue());
        assertEquals(expectedPayloadPointer.toJson(), actualPayloadPointer);
    }

    @Test
    public void testStoreOriginalPayloadWithS3KeyOnSuccess() {
        String actualPayloadPointer = payloadStore.storeOriginalPayload(ANY_PAYLOAD, ANY_PAYLOAD_LENGTH, ANY_S3_KEY);

        verify(s3Dao, times(1)).storeTextInS3(eq(S3_BUCKET_NAME), eq(ANY_S3_KEY),
                eq(ANY_PAYLOAD), eq(ANY_PAYLOAD_LENGTH));

        PayloadS3Pointer expectedPayloadPointer = new PayloadS3Pointer(S3_BUCKET_NAME, ANY_S3_KEY);
        assertEquals(expectedPayloadPointer.toJson(), actualPayloadPointer);
    }

    @Test
    public void testStoreOriginalPayloadDoesAlwaysCreateNewObjects() {
        //Store any payload
        String anyActualPayloadPointer = payloadStore
                .storeOriginalPayload(ANY_PAYLOAD, ANY_PAYLOAD_LENGTH);

        //Store any other payload and validate that the pointers are different
        String anyOtherActualPayloadPointer = payloadStore
                .storeOriginalPayload(ANY_PAYLOAD, ANY_PAYLOAD_LENGTH);

        ArgumentCaptor<String> anyOtherKeyCaptor = ArgumentCaptor.forClass(String.class);

        verify(s3Dao, times(2)).storeTextInS3(eq(S3_BUCKET_NAME), anyOtherKeyCaptor.capture(),
                eq(ANY_PAYLOAD), eq(ANY_PAYLOAD_LENGTH));

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
        doThrow(new AmazonClientException("S3 Exception"))
                .when(s3Dao)
                .storeTextInS3(
                        any(String.class),
                        any(String.class),
                        any(String.class),
                        any(Long.class));

        exception.expect(AmazonClientException.class);
        exception.expectMessage("S3 Exception");
        //Any S3 Dao exception is thrown back as-is to clients
        payloadStore.storeOriginalPayload(ANY_PAYLOAD, ANY_PAYLOAD_LENGTH);
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
        exception.expect(AmazonClientException.class);
        exception.expectMessage(INCORRECT_POINTER_EXCEPTION_MSG);
        //Any S3 Dao exception is thrown back as-is to clients
        payloadStore.getOriginalPayload("IncorrectPointer");
        verifyNoInteractions(s3Dao);
    }

    @Test
    public void testGetOriginalPayloadOnS3Failure() {
        when(s3Dao.getTextFromS3(any(String.class), any(String.class))).thenThrow(new AmazonClientException("S3 Exception"));
        exception.expect(AmazonClientException.class);
        exception.expectMessage("S3 Exception");
        //Any S3 Dao exception is thrown back as-is to clients
        PayloadS3Pointer anyPointer = new PayloadS3Pointer(S3_BUCKET_NAME, ANY_S3_KEY);
        payloadStore.getOriginalPayload(anyPointer.toJson());
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
        exception.expect(AmazonClientException.class);
        exception.expectMessage(INCORRECT_POINTER_EXCEPTION_MSG);
        payloadStore.deleteOriginalPayload("IncorrectPointer");
        verifyNoInteractions(s3Dao);
    }
}