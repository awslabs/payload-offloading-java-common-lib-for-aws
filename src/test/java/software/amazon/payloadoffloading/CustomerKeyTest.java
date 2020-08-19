package software.amazon.payloadoffloading;

import org.junit.Test;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import static org.junit.Assert.assertEquals;

public class CustomerKeyTest {

    public static final String AWS_KMS_KEY_ID = "123456";

    @Test
    public void testCustomerKeyStrategySetsCorrectEncryptionValues() {
        CustomerKey customerKey = new CustomerKey(AWS_KMS_KEY_ID);

        PutObjectRequest.Builder putObjectRequestBuilder = PutObjectRequest.builder();
        customerKey.decorate(putObjectRequestBuilder);
        PutObjectRequest putObjectRequest = putObjectRequestBuilder.build();

        assertEquals(putObjectRequest.serverSideEncryption(), ServerSideEncryption.AWS_KMS);
        assertEquals(putObjectRequest.ssekmsKeyId(), AWS_KMS_KEY_ID);
    }
}