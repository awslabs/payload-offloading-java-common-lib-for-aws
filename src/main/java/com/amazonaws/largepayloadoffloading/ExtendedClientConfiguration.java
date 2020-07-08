package com.amazonaws.largepayloadoffloading;

import com.amazonaws.annotation.NotThreadSafe;

/**
 * Amazon extended client configuration options such as Amazon S3 client,
 * bucket name, and payload size threshold for large payloads.
 */
@NotThreadSafe
public class ExtendedClientConfiguration extends ExtendedClientConfigurationBase<ExtendedClientConfiguration> {

	public ExtendedClientConfiguration() {
		super();
	}

	public ExtendedClientConfiguration(ExtendedClientConfiguration other) {
		super(other);
	}
}
