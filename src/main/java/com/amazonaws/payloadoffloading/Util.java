package com.amazonaws.payloadoffloading;

import com.amazonaws.AmazonClientException;
import com.amazonaws.util.VersionInfoUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

public class Util {
    private static final Log LOG = LogFactory.getLog(Util.class);

    public static long getStringSizeInBytes(String str) {
        CountingOutputStream counterOutputStream = new CountingOutputStream();

        try {
            Writer writer = new OutputStreamWriter(counterOutputStream, "UTF-8");
            writer.write(str);
            writer.flush();
            writer.close();

        } catch (IOException e) {
            String errorMessage = "Failed to calculate the size of payload.";
            LOG.error(errorMessage, e);
            throw new AmazonClientException(errorMessage, e);
        }

        return counterOutputStream.getTotalSize();
    }

    public static String getUserAgentHeader(String clientName) {
        return clientName + "/" + VersionInfoUtils.getVersion();
    }
}
