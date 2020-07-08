package com.amazonaws.largepayloadoffloading;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;

/**
 * An AWS storage service that has higher payload size limit than that is supported by
 * AWS messaging services (SQS, SNS).
 */
public interface LargePayloadStore {
    /**
     * Stores payload in a store that has higher payload size limit than that is supported by original payload store.
     * @param payload
     * @param payloadContentSize
     * @return a pointer that must be used to retrieve the original payload later.
     * @throws AmazonClientException
     *             If any internal errors are encountered on the client side while
     *             attempting to make the request or handle the response. For example
     *             if a network connection is not available.
     * @throws AmazonServiceException
     *             If an error response is returned by actual LargePayloadStore indicating
     *             either a problem with the data in the request, or a server side issue.
     */
    String storeOriginalPayload(String payload, Long payloadContentSize);

    /**
     * Retrieves the original payload using the given payloadPointer. The pointer must
     * have been obtained using {@link storeOriginalPayload}
     * @param payloadPointer
     * @return original payload
     * @throws AmazonClientException
     *             If any internal errors are encountered on the client side while
     *             attempting to make the request or handle the response. For example
     *             if payloadPointer is invalid or a network connection is not available.
     * @throws AmazonServiceException
     *             If an error response is returned by actual LargePayloadStore indicating
     *             a server side issue.
     */
    String getOriginalPayload(String payloadPointer);

    /**
     * Deletes the original payload using the given payloadPointer. The pointer must
     * have been obtained using {@link storeOriginalPayload}
     * @param payloadPointer
     * @throws AmazonClientException
     *             If any internal errors are encountered on the client side while
     *             attempting to make the request or handle the response to/from LargePayloadStore.
     *             For example, if payloadPointer is invalid or a network connection is not available.
     * @throws AmazonServiceException
     *             If an error response is returned by actual LargePayloadStore indicating
     *             a server side issue.
     */
    void deleteOriginalPayload(String payloadPointer);
}
