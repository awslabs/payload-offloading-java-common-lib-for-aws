package software.amazon.payloadoffloading;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests the PayloadStorageConfiguration class.
 */
public class PayloadStorageConfigurationTest {

    private static final String s3BucketName = "test-bucket-name";
    private static final ServerSideEncryptionStrategy SERVER_SIDE_ENCRYPTION_STRATEGY = ServerSideEncryptionFactory.awsManagedCmk();
    private final ObjectCannedACL objectCannelACL = ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL;

    @Test
    public void testCopyConstructor() {
        S3Client s3 = mock(S3Client.class);

        boolean alwaysThroughS3 = true;
        int payloadSizeThreshold = 500;

        PayloadStorageConfiguration payloadStorageConfiguration = new PayloadStorageConfiguration();

        payloadStorageConfiguration.withPayloadSupportEnabled(s3, s3BucketName)
                .withAlwaysThroughS3(alwaysThroughS3)
                .withPayloadSizeThreshold(payloadSizeThreshold)
                .withServerSideEncryption(SERVER_SIDE_ENCRYPTION_STRATEGY)
                .withObjectCannedACL(objectCannelACL);

        PayloadStorageConfiguration newPayloadStorageConfiguration = new PayloadStorageConfiguration(payloadStorageConfiguration);

        assertEquals(s3, newPayloadStorageConfiguration.getS3Client());
        assertEquals(s3BucketName, newPayloadStorageConfiguration.getS3BucketName());
        assertEquals(SERVER_SIDE_ENCRYPTION_STRATEGY, newPayloadStorageConfiguration.getServerSideEncryptionStrategy());
        assertTrue(newPayloadStorageConfiguration.isPayloadSupportEnabled());
        assertEquals(objectCannelACL, newPayloadStorageConfiguration.getObjectCannedACL());
        assertEquals(alwaysThroughS3, newPayloadStorageConfiguration.isAlwaysThroughS3());
        assertEquals(payloadSizeThreshold, newPayloadStorageConfiguration.getPayloadSizeThreshold());
        assertNotSame(newPayloadStorageConfiguration, payloadStorageConfiguration);
    }

    @Test
    public void testPayloadSupportEnabled() {
        S3Client s3 = mock(S3Client.class);
        PayloadStorageConfiguration payloadStorageConfiguration = new PayloadStorageConfiguration();
        payloadStorageConfiguration.setPayloadSupportEnabled(s3, s3BucketName);

        assertTrue(payloadStorageConfiguration.isPayloadSupportEnabled());
        assertNotNull(payloadStorageConfiguration.getS3Client());
        assertEquals(s3BucketName, payloadStorageConfiguration.getS3BucketName());
    }

    @Test
    public void testDisablePayloadSupport() {
        PayloadStorageConfiguration payloadStorageConfiguration = new PayloadStorageConfiguration();
        payloadStorageConfiguration.setPayloadSupportDisabled();

        assertNull(payloadStorageConfiguration.getS3Client());
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

        assertNull(payloadStorageConfiguration.getServerSideEncryptionStrategy());

        payloadStorageConfiguration.setServerSideEncryptionStrategy(SERVER_SIDE_ENCRYPTION_STRATEGY);
        assertEquals(SERVER_SIDE_ENCRYPTION_STRATEGY, payloadStorageConfiguration.getServerSideEncryptionStrategy());
    }

    @Test
    public void testCannedAccessControlList() {
        PayloadStorageConfiguration payloadStorageConfiguration = new PayloadStorageConfiguration();

        assertFalse(payloadStorageConfiguration.isObjectCannedACLDefined());

        payloadStorageConfiguration.withObjectCannedACL(objectCannelACL);
        assertTrue(payloadStorageConfiguration.isObjectCannedACLDefined());
        assertEquals(objectCannelACL, payloadStorageConfiguration.getObjectCannedACL());
    }
}
