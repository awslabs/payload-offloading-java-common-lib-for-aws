package software.amazon.payloadoffloading;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * S3 based implementation for PayloadStore.
 */
public class S3BackedPayloadStore implements PayloadStore {
    private static final Logger LOG = LoggerFactory.getLogger(S3BackedPayloadStore.class);

    private final String s3BucketName;
    private final S3Dao s3Dao;

    public S3BackedPayloadStore(S3Dao s3Dao, String s3BucketName) {
        this.s3BucketName = s3BucketName;
        this.s3Dao = s3Dao;
    }

    @Override
    public String storeOriginalPayload(String payload) {
        String s3Key = UUID.randomUUID().toString();
        return storeOriginalPayload(payload, s3Key);
    }

    @Override
    public String storeOriginalPayload(String payload, String s3Key) {
        s3Dao.storeTextInS3(s3BucketName, s3Key, payload);
        LOG.info("S3 object created, Bucket name: " + s3BucketName + ", Object key: " + s3Key + ".");

        // Convert S3 pointer (bucket name, key, etc) to JSON string
        PayloadS3Pointer s3Pointer = new PayloadS3Pointer(s3BucketName, s3Key);

        return s3Pointer.toJson();
    }

    @Override
    public String getOriginalPayload(String payloadPointer) {
        PayloadS3Pointer s3Pointer = PayloadS3Pointer.fromJson(payloadPointer);

        String s3BucketName = s3Pointer.getS3BucketName();
        String s3Key = s3Pointer.getS3Key();

        String originalPayload = s3Dao.getTextFromS3(s3BucketName, s3Key);
        LOG.info("S3 object read, Bucket name: " + s3BucketName + ", Object key: " + s3Key + ".");
        return originalPayload;
    }

    @Override
    public void deleteOriginalPayload(String payloadPointer) {
        PayloadS3Pointer s3Pointer = PayloadS3Pointer.fromJson(payloadPointer);

        String s3BucketName = s3Pointer.getS3BucketName();
        String s3Key = s3Pointer.getS3Key();
        s3Dao.deletePayloadFromS3(s3BucketName, s3Key);
    }

    @Override
    public void deleteOriginalPayloads(Collection<String> payloadPointers) {
        // Sort by S3 bucket.
        Map<String, List<PayloadS3Pointer>> offloadedMessages = payloadPointers.stream()
            .map(PayloadS3Pointer::fromJson)
            .collect(Collectors.groupingBy(PayloadS3Pointer::getS3BucketName));

        for (Map.Entry<String, List<PayloadS3Pointer>> bucket : offloadedMessages.entrySet()) {
            String s3BucketName = bucket.getKey();
            List<String> s3Keys = bucket.getValue().stream()
                .map(PayloadS3Pointer::getS3Key)
                .collect(Collectors.toList());
            s3Dao.deletePayloadsFromS3(s3BucketName, s3Keys);
        }
    }
}
