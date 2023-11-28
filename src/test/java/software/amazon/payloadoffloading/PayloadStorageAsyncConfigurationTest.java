package software.amazon.payloadoffloading;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;

/**
 * Tests the PayloadStorageAsyncConfiguration class.
 */
public class PayloadStorageAsyncConfigurationTest {

    private static final String s3BucketName = "test-bucket-name";
    private static final ServerSideEncryptionStrategy SERVER_SIDE_ENCRYPTION_STRATEGY = ServerSideEncryptionFactory.awsManagedCmk();
    private final ObjectCannedACL objectCannelACL = ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL;
    
    @Test
    public void testCopyConstructor() {
        S3AsyncClient s3Async = mock(S3AsyncClient.class);

        boolean alwaysThroughS3 = true;
        int payloadSizeThreshold = 500;

        PayloadStorageAsyncConfiguration payloadStorageConfiguration = new PayloadStorageAsyncConfiguration();

        payloadStorageConfiguration.withPayloadSupportEnabled(s3Async, s3BucketName)
            .withAlwaysThroughS3(alwaysThroughS3)
            .withPayloadSizeThreshold(payloadSizeThreshold)
            .withServerSideEncryption(SERVER_SIDE_ENCRYPTION_STRATEGY)
            .withObjectCannedACL(objectCannelACL);

        PayloadStorageAsyncConfiguration newPayloadStorageConfiguration = new PayloadStorageAsyncConfiguration(payloadStorageConfiguration);

        assertEquals(s3Async, newPayloadStorageConfiguration.getS3AsyncClient());
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
        S3AsyncClient s3Async = mock(S3AsyncClient.class);
        PayloadStorageAsyncConfiguration payloadStorageConfiguration = new PayloadStorageAsyncConfiguration();
        payloadStorageConfiguration.setPayloadSupportEnabled(s3Async, s3BucketName);

        assertTrue(payloadStorageConfiguration.isPayloadSupportEnabled());
        assertNotNull(payloadStorageConfiguration.getS3AsyncClient());
        assertEquals(s3BucketName, payloadStorageConfiguration.getS3BucketName());
    }

    @Test
    public void testDisablePayloadSupport() {
        PayloadStorageAsyncConfiguration payloadStorageConfiguration = new PayloadStorageAsyncConfiguration();
        payloadStorageConfiguration.setPayloadSupportDisabled();

        assertNull(payloadStorageConfiguration.getS3AsyncClient());
        assertNull(payloadStorageConfiguration.getS3BucketName());
    }

    @Test
    public void testAlwaysThroughS3() {
        PayloadStorageAsyncConfiguration payloadStorageConfiguration = new PayloadStorageAsyncConfiguration();

        payloadStorageConfiguration.setAlwaysThroughS3(true);
        assertTrue(payloadStorageConfiguration.isAlwaysThroughS3());

        payloadStorageConfiguration.setAlwaysThroughS3(false);
        assertFalse(payloadStorageConfiguration.isAlwaysThroughS3());
    }

    @Test
    public void testSseAwsKeyManagementParams() {
        PayloadStorageAsyncConfiguration payloadStorageConfiguration = new PayloadStorageAsyncConfiguration();

        assertNull(payloadStorageConfiguration.getServerSideEncryptionStrategy());

        payloadStorageConfiguration.setServerSideEncryptionStrategy(SERVER_SIDE_ENCRYPTION_STRATEGY);
        assertEquals(SERVER_SIDE_ENCRYPTION_STRATEGY, payloadStorageConfiguration.getServerSideEncryptionStrategy());
    }

    @Test
    public void testCannedAccessControlList() {
        PayloadStorageAsyncConfiguration payloadStorageConfiguration = new PayloadStorageAsyncConfiguration();

        assertFalse(payloadStorageConfiguration.isObjectCannedACLDefined());

        payloadStorageConfiguration.withObjectCannedACL(objectCannelACL);
        assertTrue(payloadStorageConfiguration.isObjectCannedACLDefined());
        assertEquals(objectCannelACL, payloadStorageConfiguration.getObjectCannedACL());
    }
}