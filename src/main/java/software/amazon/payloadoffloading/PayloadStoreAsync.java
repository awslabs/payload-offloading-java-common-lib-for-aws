package software.amazon.payloadoffloading;

import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * An AWS storage service that supports saving high payload sizes.
 */
public interface PayloadStoreAsync {

    /**
     * Stores payload in a store that has higher payload size limit than that is supported by original payload store.
     * <p>
     * This call is asynchronous, and so documented return values and exceptions are propagated through
     * the returned {@link CompletableFuture}.
     *
     * @param payload
     * @return future value of a pointer that must be used to retrieve the original payload later.
     * @throws SdkClientException  If any internal errors are encountered on the client side while
     *                                attempting to make the request or handle the response. For example
     *                                if a network connection is not available.
     * @throws S3Exception If an error response is returned by actual PayloadStore indicating
     *                                either a problem with the data in the request, or a server side issue.
     */
    CompletableFuture<String> storeOriginalPayload(String payload);

    /**
     * Stores payload in a store that has higher payload size limit than that is supported by original payload store.
     * <p>
     * This call is asynchronous, and so documented return values and exceptions are propagated through
     * the returned {@link CompletableFuture}.
     *
     * @param payload
     * @param s3Key
     * @return future value of a pointer that must be used to retrieve the original payload later.
     * @throws SdkClientException  If any internal errors are encountered on the client side while
     *                                attempting to make the request or handle the response. For example
     *                                if a network connection is not available.
     * @throws S3Exception If an error response is returned by actual PayloadStore indicating
     *                                either a problem with the data in the request, or a server side issue.
     */
    CompletableFuture<String> storeOriginalPayload(String payload, String s3Key);

    /**
     * Retrieves the original payload using the given payloadPointer. The pointer must
     * have been obtained using {@link #storeOriginalPayload(String)}
     * <p>
     * This call is asynchronous, and so documented return values and exceptions are propagated through
     * the returned {@link CompletableFuture}.
     *
     * @param payloadPointer
     * @return future value of the original payload
     * @throws SdkClientException  If any internal errors are encountered on the client side while
     *                                attempting to make the request or handle the response. For example
     *                                if payloadPointer is invalid or a network connection is not available.
     * @throws S3Exception If an error response is returned by actual PayloadStore indicating
     *                                a server side issue.
     */
    CompletableFuture<String> getOriginalPayload(String payloadPointer);

    /**
     * Deletes the original payload using the given payloadPointer. The pointer must
     * have been obtained using {@link #storeOriginalPayload(String)}
     * <p>
     * This call is asynchronous, and so documented return values and exceptions are propagated through
     * the returned {@link CompletableFuture}.
     *
     * @param payloadPointer
     * @return future value that completes when the delete operation finishes
     * @throws SdkClientException  If any internal errors are encountered on the client side while
     *                                attempting to make the request or handle the response to/from PayloadStore.
     *                                For example, if payloadPointer is invalid or a network connection is not available.
     * @throws S3Exception If an error response is returned by actual PayloadStore indicating
     *                                a server side issue.
     */
    CompletableFuture<Void> deleteOriginalPayload(String payloadPointer);
}
