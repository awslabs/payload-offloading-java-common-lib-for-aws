package software.amazon.payloadoffloading;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;

import java.util.Objects;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
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

    private Object[] testData() {
        // Here, we create separate mock of S3Dao because JUnitParamsRunner collects parameters
        // for tests well before invocation of @Before or @BeforeClass methods.
        // That means our default s3Dao mock isn't instantiated until then. For parameterized tests,
        // we instantiate our local S3Dao mock per combination, pass it to S3BackedPayloadStore and also pass it
        // as test parameter to allow verifying calls to the mockS3Dao.
        S3Dao noEncryptionS3Dao = mock(S3Dao.class);
        S3Dao defaultEncryptionS3Dao = mock(S3Dao.class);
        S3Dao customerKMSKeyEncryptionS3Dao = mock(S3Dao.class);
        return new Object[][]{
                // No S3 SSE-KMS encryption
                {
                    new S3BackedPayloadStore(noEncryptionS3Dao, S3_BUCKET_NAME),
                    null,
                    noEncryptionS3Dao
                },
                // S3 SSE-KMS encryption with AWS managed KMS keys
                // TODO MS Not sure how to implement this - customer wants to use their default KMS key
//                {
//                    new S3BackedPayloadStore(defaultEncryptionS3Dao, S3_BUCKET_NAME, S3_SERVER_SIDE_ENCRYPTION_KMS_KEY_ID),
//                    S3_SERVER_SIDE_ENCRYPTION_KMS_KEY_ID,
//                    defaultEncryptionS3Dao
//                },
                // S3 SSE-KMS encryption with customer managed KMS key
                {
                    new S3BackedPayloadStore(customerKMSKeyEncryptionS3Dao, S3_BUCKET_NAME, S3_SERVER_SIDE_ENCRYPTION_KMS_KEY_ID),
                    S3_SERVER_SIDE_ENCRYPTION_KMS_KEY_ID,
                    customerKMSKeyEncryptionS3Dao
                }
        };
    }

    @Test
    @Parameters(method = "testData")
    public void testStoreOriginalPayloadOnSuccess(PayloadStore payloadStore, String expectedParams, S3Dao mockS3Dao) {
        String actualPayloadPointer = payloadStore.storeOriginalPayload(ANY_PAYLOAD);

        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> sseArgsCaptor = ArgumentCaptor.forClass(String.class);

        verify(mockS3Dao, times(1)).storeTextInS3(eq(S3_BUCKET_NAME), keyCaptor.capture(),
                sseArgsCaptor.capture(), eq(ANY_PAYLOAD));

        PayloadS3Pointer expectedPayloadPointer = new PayloadS3Pointer(S3_BUCKET_NAME, keyCaptor.getValue());
        assertEquals(expectedPayloadPointer.toJson(), actualPayloadPointer);

        if (expectedParams == null) {
            assertTrue(sseArgsCaptor.getValue() == null);
        } else {
            assertEquals(expectedParams, sseArgsCaptor.getValue());
        }
    }

    @Test
    @Parameters(method = "testData")
    public void testStoreOriginalPayloadDoesAlwaysCreateNewObjects(PayloadStore payloadStore,
                                                                   String expectedParams,
                                                                   S3Dao mockS3Dao) {
        //Store any payload
        String anyActualPayloadPointer = payloadStore.storeOriginalPayload(ANY_PAYLOAD);

        //Store any other payload and validate that the pointers are different
        String anyOtherActualPayloadPointer = payloadStore.storeOriginalPayload(ANY_PAYLOAD);

        ArgumentCaptor<String> anyOtherKeyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> sseArgsCaptor = ArgumentCaptor.forClass(String.class);

        verify(mockS3Dao, times(2)).storeTextInS3(eq(S3_BUCKET_NAME), anyOtherKeyCaptor.capture(),
                sseArgsCaptor.capture(), eq(ANY_PAYLOAD));

        String anyS3Key = anyOtherKeyCaptor.getAllValues().get(0);
        String anyOtherS3Key = anyOtherKeyCaptor.getAllValues().get(1);

        PayloadS3Pointer anyExpectedPayloadPointer = new PayloadS3Pointer(S3_BUCKET_NAME, anyS3Key);
        assertEquals(anyExpectedPayloadPointer.toJson(), anyActualPayloadPointer);

        PayloadS3Pointer anyOtherExpectedPayloadPointer = new PayloadS3Pointer(S3_BUCKET_NAME, anyOtherS3Key);
        assertEquals(anyOtherExpectedPayloadPointer.toJson(), anyOtherActualPayloadPointer);

        assertThat(anyS3Key, Matchers.not(anyOtherS3Key));
        assertThat(anyActualPayloadPointer, Matchers.not(anyOtherActualPayloadPointer));

        if (expectedParams == null) {
            assertTrue(sseArgsCaptor.getAllValues().stream().allMatch(Objects::isNull));
        } else {
            assertTrue(sseArgsCaptor.getAllValues().stream().allMatch(actualParams ->
                    actualParams.equals(expectedParams)));
        }
    }

    @Test
    @Parameters(method = "testData")
    public void testStoreOriginalPayloadOnS3Failure(PayloadStore payloadStore, String awsKmsKeyId, S3Dao mockS3Dao) {
        doThrow(SdkException.create("S3 Exception", new Throwable()))
                .when(mockS3Dao)
                .storeTextInS3(
                        any(String.class),
                        any(String.class),
                        // Can be String or null
                        any(),
                        any(String.class));

        exception.expect(SdkException.class);
        exception.expectMessage("S3 Exception");
        //Any S3 Dao exception is thrown back as-is to clients
        payloadStore.storeOriginalPayload(ANY_PAYLOAD);
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
        exception.expect(SdkClientException.class);
        exception.expectMessage(INCORRECT_POINTER_EXCEPTION_MSG);
        //Any S3 Dao exception is thrown back as-is to clients
        payloadStore.getOriginalPayload("IncorrectPointer");
        verifyNoInteractions(s3Dao);
    }

    @Test
    public void testGetOriginalPayloadOnS3Failure() {
        when(s3Dao.getTextFromS3(any(String.class), any(String.class))).thenThrow(SdkException.create("S3 Exception", new Throwable()));
        exception.expect(SdkException.class);
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
        exception.expect(SdkClientException.class);
        exception.expectMessage(INCORRECT_POINTER_EXCEPTION_MSG);
        payloadStore.deleteOriginalPayload("IncorrectPointer");
        verifyNoInteractions(s3Dao);
    }
}