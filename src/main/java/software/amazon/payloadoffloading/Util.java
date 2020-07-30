package software.amazon.payloadoffloading;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.util.VersionInfo;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

public class Util {
    private static final Logger LOG = LoggerFactory.getLogger(Util.class);

    public static long getStringSizeInBytes(String str) {
        CountingOutputStream counterOutputStream = new CountingOutputStream();

        try {
            Writer writer = new OutputStreamWriter(counterOutputStream, StandardCharsets.UTF_8);
            writer.write(str);
            writer.flush();
            writer.close();

        } catch (IOException e) {
            String errorMessage = "Failed to calculate the size of payload.";
            LOG.error(errorMessage, e);
            throw SdkClientException.create(errorMessage, e);
        }

        return counterOutputStream.getTotalSize();
    }

    public static String getUserAgentHeader(String clientName) {
        return clientName + "/" + VersionInfo.SDK_VERSION;
    }
}
