package software.amazon.payloadoffloading;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.junit.Assert.*;

/**
 * Tests the PayloadStorageConfiguration class.
 */
public class PayloadStorageConfigurationTest {

    private static String s3BucketName = "test-bucket-name";
    private static String s3ServerSideEncryptionKMSKeyId = "test-customer-managed-kms-key-id";
    private SSEAwsKeyManagementParams sseAwsKeyManagementParams;
    private CannedAccessControlList cannedAccessControlList;

    @Before
    public void setup() {
        sseAwsKeyManagementParams = new SSEAwsKeyManagementParams(s3ServerSideEncryptionKMSKeyId);
        cannedAccessControlList = CannedAccessControlList.BucketOwnerFullControl;
    }

    @Test
    public void testCopyConstructor() {
        AmazonS3 s3 = mock(AmazonS3.class);

        boolean alwaysThroughS3 = true;
        int payloadSizeThreshold = 500;

        PayloadStorageConfiguration payloadStorageConfiguration = new PayloadStorageConfiguration();

        payloadStorageConfiguration.withPayloadSupportEnabled(s3, s3BucketName)
                .withAlwaysThroughS3(alwaysThroughS3).withPayloadSizeThreshold(payloadSizeThreshold)
                .withSSEAwsKeyManagementParams(sseAwsKeyManagementParams)
                .withCannedAccessControlList(cannedAccessControlList);

        PayloadStorageConfiguration newPayloadStorageConfiguration = new PayloadStorageConfiguration(payloadStorageConfiguration);

        assertEquals(s3, newPayloadStorageConfiguration.getAmazonS3Client());
        assertEquals(s3BucketName, newPayloadStorageConfiguration.getS3BucketName());
        assertEquals(sseAwsKeyManagementParams, newPayloadStorageConfiguration.getSSEAwsKeyManagementParams());
        assertEquals(s3ServerSideEncryptionKMSKeyId, newPayloadStorageConfiguration.getSSEAwsKeyManagementParams().getAwsKmsKeyId());
        assertEquals(cannedAccessControlList, newPayloadStorageConfiguration.getCannedAccessControlList());
        assertTrue(newPayloadStorageConfiguration.isPayloadSupportEnabled());
        assertEquals(alwaysThroughS3, newPayloadStorageConfiguration.isAlwaysThroughS3());
        assertEquals(payloadSizeThreshold, newPayloadStorageConfiguration.getPayloadSizeThreshold());
        assertNotSame(newPayloadStorageConfiguration, payloadStorageConfiguration);
    }

    @Test
    public void testPayloadSupportEnabled() {
        AmazonS3 s3 = mock(AmazonS3.class);        
        PayloadStorageConfiguration payloadStorageConfiguration = new PayloadStorageConfiguration();
        payloadStorageConfiguration.setPayloadSupportEnabled(s3, s3BucketName);

        assertTrue(payloadStorageConfiguration.isPayloadSupportEnabled());
        assertNotNull(payloadStorageConfiguration.getAmazonS3Client());
        assertEquals(s3BucketName, payloadStorageConfiguration.getS3BucketName());
    }

    @Test
    public void testDisablePayloadSupport() {
        PayloadStorageConfiguration payloadStorageConfiguration = new PayloadStorageConfiguration();
        payloadStorageConfiguration.setPayloadSupportDisabled();

        assertNull(payloadStorageConfiguration.getAmazonS3Client());
        assertNull(payloadStorageConfiguration.getS3BucketName());
    }

    @Test
    public void testAlwaysThroughS3() {
        PayloadStorageConfiguration payloadStorageConfiguration = new PayloadStorageConfiguration();

        payloadStorageConfiguration.setAlwaysThroughS3(true);
        assertTrue(payloadStorageConfiguration.isAlwaysThroughS3());

        payloadStorageConfiguration.setAlwaysThroughS3(false);
        assertFalse(payloadStorageConfiguration.isAlwaysThroughS3());
    }

    @Test
    public void testSseAwsKeyManagementParams() {
        PayloadStorageConfiguration payloadStorageConfiguration = new PayloadStorageConfiguration();

        assertNull(payloadStorageConfiguration.getSSEAwsKeyManagementParams());

        payloadStorageConfiguration.setSSEAwsKeyManagementParams(sseAwsKeyManagementParams);
        assertEquals(s3ServerSideEncryptionKMSKeyId, payloadStorageConfiguration.getSSEAwsKeyManagementParams()
            .getAwsKmsKeyId());
    }

    @Test
    public void testCannedAccessControlList() {

        PayloadStorageConfiguration payloadStorageConfiguration = new PayloadStorageConfiguration();

        assertFalse(payloadStorageConfiguration.isCannedAccessControlListDefined());

        payloadStorageConfiguration.withCannedAccessControlList(cannedAccessControlList);
        assertTrue(payloadStorageConfiguration.isCannedAccessControlListDefined());
        assertEquals(cannedAccessControlList, payloadStorageConfiguration.getCannedAccessControlList());
    }
}
