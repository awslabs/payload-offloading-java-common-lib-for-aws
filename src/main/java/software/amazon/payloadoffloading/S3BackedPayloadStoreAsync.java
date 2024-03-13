package software.amazon.payloadoffloading;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * S3 based implementation for PayloadStoreAsync.
 */
public class S3BackedPayloadStoreAsync implements PayloadStoreAsync {
    private static final Logger LOG = LoggerFactory.getLogger(S3BackedPayloadStoreAsync.class);

    private final String s3BucketName;
    private final S3AsyncDao s3Dao;

    public S3BackedPayloadStoreAsync(S3AsyncDao s3Dao, String s3BucketName) {
        this.s3BucketName = s3BucketName;
        this.s3Dao = s3Dao;
    }

    @Override
    public CompletableFuture<String> storeOriginalPayload(String payload) {
        String s3Key = UUID.randomUUID().toString();
        return storeOriginalPayload(payload, s3Key);
    }

    @Override
    public CompletableFuture<String> storeOriginalPayload(String payload, String s3Key) {
        return s3Dao.storeTextInS3(s3BucketName, s3Key, payload)
            .thenApply(v -> {
                LOG.info("S3 object created, Bucket name: " + s3BucketName + ", Object key: " + s3Key + ".");

                // Convert S3 pointer (bucket name, key, etc) to JSON string
                PayloadS3Pointer s3Pointer = new PayloadS3Pointer(s3BucketName, s3Key);

                return s3Pointer.toJson();
            });
    }

    @Override
    public CompletableFuture<String> getOriginalPayload(String payloadPointer) {
        try {
            PayloadS3Pointer s3Pointer = PayloadS3Pointer.fromJson(payloadPointer);

            String s3BucketName = s3Pointer.getS3BucketName();
            String s3Key = s3Pointer.getS3Key();

            return s3Dao.getTextFromS3(s3BucketName, s3Key)
                .thenApply(originalPayload -> {
                    LOG.info("S3 object read, Bucket name: " + s3BucketName + ", Object key: " + s3Key + ".");
                    return originalPayload;
                });
        } catch (Exception e) {
            CompletableFuture<String> futureEx = new CompletableFuture<>();
            futureEx.completeExceptionally((e instanceof RuntimeException) ? e : new CompletionException(e));
            return futureEx;
        }
    }

    @Override
    public CompletableFuture<Void> deleteOriginalPayload(String payloadPointer) {
        try {
            PayloadS3Pointer s3Pointer = PayloadS3Pointer.fromJson(payloadPointer);

            String s3BucketName = s3Pointer.getS3BucketName();
            String s3Key = s3Pointer.getS3Key();
            return s3Dao.deletePayloadFromS3(s3BucketName, s3Key);
        } catch (Exception e) {
            CompletableFuture<Void> futureEx = new CompletableFuture<>();
            futureEx.completeExceptionally((e instanceof RuntimeException) ? e : new CompletionException(e));
            return futureEx;
        }
    }

    @Override
    public CompletableFuture<Void> deleteOriginalPayloads(Collection<String> payloadPointers) {
        // Skip the delete if there are no payloads to delete.
        if (payloadPointers.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        try {
            // Sort by S3 bucket.
            Map<String, List<PayloadS3Pointer>> offloadedMessages = payloadPointers.stream()
                .map(PayloadS3Pointer::fromJson)
                .collect(Collectors.groupingBy(PayloadS3Pointer::getS3BucketName));

            List<CompletableFuture<Void>> deleteFutures = new ArrayList<>(offloadedMessages.size());
            for (Map.Entry<String, List<PayloadS3Pointer>> bucket : offloadedMessages.entrySet()) {
                String s3BucketName = bucket.getKey();
                List<String> s3Keys = bucket.getValue().stream()
                    .map(PayloadS3Pointer::getS3Key)
                    .collect(Collectors.toList());
                deleteFutures.add(s3Dao.deletePayloadsFromS3(s3BucketName, s3Keys));
            }

            return CompletableFuture.allOf(deleteFutures.toArray(new CompletableFuture[0]));
        } catch (Exception e) {
            CompletableFuture<Void> futureEx = new CompletableFuture<>();
            futureEx.completeExceptionally((e instanceof RuntimeException) ? e : new CompletionException(e));
            return futureEx;
        }
    }
}
