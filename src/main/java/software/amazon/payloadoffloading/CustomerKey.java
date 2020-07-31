package software.amazon.payloadoffloading;

import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

public class CustomerKey implements ServerSideEncryptionStrategy {
    private final String awsKmsKeyId;

    public CustomerKey(String awsKmsKeyId) {
        this.awsKmsKeyId = awsKmsKeyId;
    }

    @Override
    public void decorate(PutObjectRequest.Builder putObjectRequestBuilder) {
        putObjectRequestBuilder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
        putObjectRequestBuilder.ssekmsKeyId(awsKmsKeyId);
    }
}
