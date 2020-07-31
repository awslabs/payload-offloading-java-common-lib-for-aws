package software.amazon.payloadoffloading;

import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

public class AwsManagedCmk implements ServerSideEncryptionStrategy {
    @Override
    public void decorate(PutObjectRequest.Builder putObjectRequestBuilder) {
        putObjectRequestBuilder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
    }
}
