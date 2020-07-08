package com.amazonaws.largepayloadoffloading;

import java.io.OutputStream;

/**
 * This class is used for checking the size of a string without copying the
 * whole string into memory and converting it to bytes array. Compared to
 * String.getBytes().length, it is more efficient and reliable for large
 * strings.
 */
class CountingOutputStream extends OutputStream {
	private long totalSize;

	@Override
	public void write(int b) {
		++totalSize;
	}

	@Override
	public void write(byte[] b) {
		totalSize += b.length;
	}

	@Override
	public void write(byte[] b, int offset, int len) {
		totalSize += len;
	}

	public long getTotalSize() {
		return totalSize;
	}
}
