package com.amazonaws.largepayloadoffloading;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.junit.Assert.*;

/**
 * Tests the LargePayloadStorageConfiguration class.
 */
public class LargePayloadStorageConfigurationTest {

    private static String s3BucketName = "test-bucket-name";
    private static String s3ServerSideEncryptionKMSKeyId = "test-customer-managed-kms-key-id";
    private SSEAwsKeyManagementParams sseAwsKeyManagementParams;

    @Before
    public void setup() {
        sseAwsKeyManagementParams = new SSEAwsKeyManagementParams(s3ServerSideEncryptionKMSKeyId);
    }

    @Test
    public void testCopyConstructor() {
        AmazonS3 s3 = mock(AmazonS3.class);

        boolean alwaysThroughS3 = true;
        int payloadSizeThreshold = 500;

        LargePayloadStorageConfiguration largePayloadStorageConfiguration = new LargePayloadStorageConfiguration();

        largePayloadStorageConfiguration.withLargePayloadSupportEnabled(s3, s3BucketName)
                .withAlwaysThroughS3(alwaysThroughS3).withPayloadSizeThreshold(payloadSizeThreshold)
                .withSSEAwsKeyManagementParams(sseAwsKeyManagementParams);

        LargePayloadStorageConfiguration newLargePayloadStorageConfiguration = new LargePayloadStorageConfiguration(largePayloadStorageConfiguration);

        assertEquals(s3, newLargePayloadStorageConfiguration.getAmazonS3Client());
        assertEquals(s3BucketName, newLargePayloadStorageConfiguration.getS3BucketName());
        assertEquals(sseAwsKeyManagementParams, newLargePayloadStorageConfiguration.getSSEAwsKeyManagementParams());
        assertEquals(s3ServerSideEncryptionKMSKeyId, newLargePayloadStorageConfiguration.getSSEAwsKeyManagementParams().getAwsKmsKeyId());
        assertTrue(newLargePayloadStorageConfiguration.isLargePayloadSupportEnabled());
        assertEquals(alwaysThroughS3, newLargePayloadStorageConfiguration.isAlwaysThroughS3());
        assertEquals(payloadSizeThreshold, newLargePayloadStorageConfiguration.getPayloadSizeThreshold());
        assertNotSame(newLargePayloadStorageConfiguration, largePayloadStorageConfiguration);
    }

    @Test
    public void testLargePayloadSupportEnabled() {
        AmazonS3 s3 = mock(AmazonS3.class);        
        LargePayloadStorageConfiguration largePayloadStorageConfiguration = new LargePayloadStorageConfiguration();
        largePayloadStorageConfiguration.setLargePayloadSupportEnabled(s3, s3BucketName);

        assertTrue(largePayloadStorageConfiguration.isLargePayloadSupportEnabled());
        assertNotNull(largePayloadStorageConfiguration.getAmazonS3Client());
        assertEquals(s3BucketName, largePayloadStorageConfiguration.getS3BucketName());
    }

    @Test
    public void testDisableLargePayloadSupport() {
        LargePayloadStorageConfiguration largePayloadStorageConfiguration = new LargePayloadStorageConfiguration();
        largePayloadStorageConfiguration.setLargePayloadSupportDisabled();

        assertNull(largePayloadStorageConfiguration.getAmazonS3Client());
        assertNull(largePayloadStorageConfiguration.getS3BucketName());
    }

    @Test
    public void testAlwaysThroughS3() {
        LargePayloadStorageConfiguration largePayloadStorageConfiguration = new LargePayloadStorageConfiguration();

        largePayloadStorageConfiguration.setAlwaysThroughS3(true);
        assertTrue(largePayloadStorageConfiguration.isAlwaysThroughS3());

        largePayloadStorageConfiguration.setAlwaysThroughS3(false);
        assertFalse(largePayloadStorageConfiguration.isAlwaysThroughS3());
    }

    @Test
    public void testSseAwsKeyManagementParams() {
        LargePayloadStorageConfiguration largePayloadStorageConfiguration = new LargePayloadStorageConfiguration();

        assertNull(largePayloadStorageConfiguration.getSSEAwsKeyManagementParams());

        largePayloadStorageConfiguration.setSSEAwsKeyManagementParams(sseAwsKeyManagementParams);
        assertEquals(s3ServerSideEncryptionKMSKeyId, largePayloadStorageConfiguration.getSSEAwsKeyManagementParams()
            .getAwsKmsKeyId());
    }
}
