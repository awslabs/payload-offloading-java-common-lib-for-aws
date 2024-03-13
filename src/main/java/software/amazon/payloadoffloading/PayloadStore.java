package software.amazon.payloadoffloading;

import java.util.Collection;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * An AWS storage service that supports saving high payload sizes.
 */
public interface PayloadStore {
    /**
     * Stores payload in a store that has higher payload size limit than that is supported by original payload store.
     *
     * @param payload
     * @return a pointer that must be used to retrieve the original payload later.
     * @throws SdkClientException  If any internal errors are encountered on the client side while
     *                                attempting to make the request or handle the response. For example
     *                                if a network connection is not available.
     * @throws S3Exception If an error response is returned by actual PayloadStore indicating
     *                                either a problem with the data in the request, or a server side issue.
     */
    String storeOriginalPayload(String payload);

    /**
     * Stores payload in a store that has higher payload size limit than that is supported by original payload store.
     *
     * @param payload
     * @param s3Key
     * @return a pointer that must be used to retrieve the original payload later.
     * @throws SdkClientException  If any internal errors are encountered on the client side while
     *                                attempting to make the request or handle the response. For example
     *                                if a network connection is not available.
     * @throws S3Exception If an error response is returned by actual PayloadStore indicating
     *                                either a problem with the data in the request, or a server side issue.
     */
    String storeOriginalPayload(String payload, String s3Key);

    /**
     * Retrieves the original payload using the given payloadPointer. The pointer must
     * have been obtained using {@link storeOriginalPayload}
     *
     * @param payloadPointer
     * @return original payload
     * @throws SdkClientException  If any internal errors are encountered on the client side while
     *                                attempting to make the request or handle the response. For example
     *                                if payloadPointer is invalid or a network connection is not available.
     * @throws S3Exception If an error response is returned by actual PayloadStore indicating
     *                                a server side issue.
     */
    String getOriginalPayload(String payloadPointer);

    /**
     * Deletes the original payload using the given payloadPointer. The pointer must
     * have been obtained using {@link storeOriginalPayload}
     *
     * @param payloadPointer
     * @throws SdkClientException  If any internal errors are encountered on the client side while
     *                                attempting to make the request or handle the response to/from PayloadStore.
     *                                For example, if payloadPointer is invalid or a network connection is not available.
     * @throws S3Exception If an error response is returned by actual PayloadStore indicating
     *                                a server side issue.
     */
    void deleteOriginalPayload(String payloadPointer);

    /**
     * Deletes original payloads using the given payloadPointers. The pointers must
     * have been obtained using {@link storeOriginalPayload}
     * <p>
     * This call will be more efficient than deleting payloads one at a time if the payloads
     * are in the same S3 bucket.
     *
     * @param payloadPointers
     * @throws SdkClientException  If any internal errors are encountered on the client side while
     *                                attempting to make the request or handle the response to/from PayloadStore.
     *                                For example, if payloadPointer is invalid or a network connection is not available.
     * @throws S3Exception If an error response is returned by actual PayloadStore indicating
     *                                a server side issue.
     */
    void deleteOriginalPayloads(Collection<String> payloadPointers);
}
