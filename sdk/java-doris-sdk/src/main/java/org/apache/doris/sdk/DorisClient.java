package org.apache.doris.sdk;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.sdk.load.DorisLoadClient;
import org.apache.doris.sdk.load.config.DorisConfig;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Top-level entry point for the Apache Doris Java SDK.
 *
 * <pre>
 * // CSV example
 * DorisConfig config = DorisConfig.builder()
 *     .endpoints(Arrays.asList("http://127.0.0.1:8030"))
 *     .user("root").password("password")
 *     .database("test_db").table("users")
 *     .format(DorisConfig.defaultCsvFormat())
 *     .retry(DorisConfig.defaultRetry())
 *     .groupCommit(GroupCommitMode.ASYNC)
 *     .build();
 *
 * DorisLoadClient client = DorisClient.newClient(config);
 * LoadResponse resp = client.load(DorisClient.stringStream("1,Alice,25\n2,Bob,30"));
 * if (resp.getStatus() == LoadResponse.Status.SUCCESS) {
 *     System.out.println("Loaded rows: " + resp.getRespContent().getNumberLoadedRows());
 * }
 * </pre>
 */
public class DorisClient {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private DorisClient() {}

    /**
     * Creates a new thread-safe DorisLoadClient from the given configuration.
     * The client should be reused across multiple load calls (and threads).
     */
    public static DorisLoadClient newClient(DorisConfig config) {
        return new DorisLoadClient(config);
    }

    /** Wraps a UTF-8 string as an InputStream. */
    public static InputStream stringStream(String data) {
        return new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
    }

    /** Wraps a byte array as an InputStream. */
    public static InputStream bytesStream(byte[] data) {
        return new ByteArrayInputStream(data);
    }

    /**
     * Serializes an object to JSON and returns it as an InputStream.
     * Uses Jackson ObjectMapper.
     */
    public static InputStream jsonStream(Object data) throws IOException {
        byte[] bytes = OBJECT_MAPPER.writeValueAsBytes(data);
        return new ByteArrayInputStream(bytes);
    }
}
