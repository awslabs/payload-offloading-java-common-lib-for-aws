package software.amazon.payloadoffloading;

import org.junit.Test;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import static org.junit.Assert.assertEquals;

public class AwsManagedCmkTest {

    @Test
    public void testAwsManagedCmkStrategySetsCorrectEncryptionValues() {
        AwsManagedCmk awsManagedCmk = new AwsManagedCmk();

        PutObjectRequest.Builder putObjectRequestBuilder = PutObjectRequest.builder();
        awsManagedCmk.decorate(putObjectRequestBuilder);
        PutObjectRequest putObjectRequest = putObjectRequestBuilder.build();

        assertEquals(putObjectRequest.serverSideEncryption(), (ServerSideEncryption.AWS_KMS));
    }
}