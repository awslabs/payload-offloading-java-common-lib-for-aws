package com.amazonaws.largepayloadoffloading;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.junit.Assert.*;

/**
 * Tests the ExtendedClientConfiguration class.
 */
public class ExtendedClientConfigurationTest {

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

        ExtendedClientConfiguration extendedClientConfig = new ExtendedClientConfiguration();

        extendedClientConfig.withLargePayloadSupportEnabled(s3, s3BucketName)
                .withAlwaysThroughS3(alwaysThroughS3).withPayloadSizeThreshold(payloadSizeThreshold)
                .withSSEAwsKeyManagementParams(sseAwsKeyManagementParams);

        ExtendedClientConfiguration newExtendedClientConfig = new ExtendedClientConfiguration(extendedClientConfig);

        assertEquals(s3, newExtendedClientConfig.getAmazonS3Client());
        assertEquals(s3BucketName, newExtendedClientConfig.getS3BucketName());
        assertEquals(sseAwsKeyManagementParams, newExtendedClientConfig.getSSEAwsKeyManagementParams());
        assertEquals(s3ServerSideEncryptionKMSKeyId, newExtendedClientConfig.getSSEAwsKeyManagementParams().getAwsKmsKeyId());
        assertTrue(newExtendedClientConfig.isLargePayloadSupportEnabled());
        assertEquals(alwaysThroughS3, newExtendedClientConfig.isAlwaysThroughS3());
        assertEquals(payloadSizeThreshold, newExtendedClientConfig.getPayloadSizeThreshold());
        assertNotSame(newExtendedClientConfig, extendedClientConfig);
    }

    @Test
    public void testLargePayloadSupportEnabled() {
        AmazonS3 s3 = mock(AmazonS3.class);        
        ExtendedClientConfiguration extendedClientConfiguration = new ExtendedClientConfiguration();
        extendedClientConfiguration.setLargePayloadSupportEnabled(s3, s3BucketName);

        assertTrue(extendedClientConfiguration.isLargePayloadSupportEnabled());
        assertNotNull(extendedClientConfiguration.getAmazonS3Client());
        assertEquals(s3BucketName, extendedClientConfiguration.getS3BucketName());
    }

    @Test
    public void testDisableLargePayloadSupport() {
        ExtendedClientConfiguration extendedClientConfiguration = new ExtendedClientConfiguration();
        extendedClientConfiguration.setLargePayloadSupportDisabled();

        assertNull(extendedClientConfiguration.getAmazonS3Client());
        assertNull(extendedClientConfiguration.getS3BucketName());
    }

    @Test
    public void testAlwaysThroughS3() {
        ExtendedClientConfiguration extendedClientConfiguration = new ExtendedClientConfiguration();

        extendedClientConfiguration.setAlwaysThroughS3(true);
        assertTrue(extendedClientConfiguration.isAlwaysThroughS3());

        extendedClientConfiguration.setAlwaysThroughS3(false);
        assertFalse(extendedClientConfiguration.isAlwaysThroughS3());
    }

    @Test
    public void testPayloadSizeThreshold() {
        ExtendedClientConfiguration extendedClientConfiguration = new ExtendedClientConfiguration();

        assertEquals(ExtendedClientConstants.DEFAULT_PAYLOAD_SIZE_THRESHOLD,
                extendedClientConfiguration.getPayloadSizeThreshold());

        int payloadLength = 1000;
        extendedClientConfiguration.setPayloadSizeThreshold(payloadLength);
        assertEquals(payloadLength, extendedClientConfiguration.getPayloadSizeThreshold());
    }

    @Test
    public void testSseAwsKeyManagementParams() {
        ExtendedClientConfiguration extendedClientConfiguration = new ExtendedClientConfiguration();

        assertNull(extendedClientConfiguration.getSSEAwsKeyManagementParams());

        extendedClientConfiguration.setSSEAwsKeyManagementParams(sseAwsKeyManagementParams);
        assertEquals(s3ServerSideEncryptionKMSKeyId, extendedClientConfiguration.getSSEAwsKeyManagementParams()
            .getAwsKmsKeyId());
    }
}
