package software.amazon.payloadoffloading;

import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public interface ServerSideEncryptionStrategy {
    void decorate(PutObjectRequest.Builder putObjectRequestBuilder);
}
