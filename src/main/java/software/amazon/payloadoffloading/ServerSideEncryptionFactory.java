package software.amazon.payloadoffloading;

public class ServerSideEncryptionFactory {
    public static ServerSideEncryptionStrategy awsManagedCmk() {
        return new AwsManagedCmk();
    }

    public static ServerSideEncryptionStrategy customerKey(String awsKmsKeyId) {
        return new CustomerKey(awsKmsKeyId);
    }
}
