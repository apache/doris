package org.apache.doris.sdk.load.config;

import org.junit.Test;
import java.util.Arrays;
import static org.junit.Assert.*;

public class DorisConfigTest {

    private DorisConfig.Builder validBuilder() {
        return DorisConfig.builder()
                .endpoints(Arrays.asList("http://127.0.0.1:8030"))
                .user("root")
                .password("password")
                .database("test_db")
                .table("users")
                .format(new JsonFormat(JsonFormat.Type.OBJECT_LINE));
    }

    @Test
    public void testValidConfigBuilds() {
        DorisConfig config = validBuilder().build();
        assertNotNull(config);
        assertEquals("root", config.getUser());
        assertEquals("test_db", config.getDatabase());
        assertEquals("users", config.getTable());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingUserThrows() {
        DorisConfig.builder()
                .endpoints(Arrays.asList("http://127.0.0.1:8030"))
                .database("db").table("t")
                .format(new JsonFormat(JsonFormat.Type.OBJECT_LINE))
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingDatabaseThrows() {
        DorisConfig.builder()
                .endpoints(Arrays.asList("http://127.0.0.1:8030"))
                .user("root").table("t")
                .format(new JsonFormat(JsonFormat.Type.OBJECT_LINE))
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingTableThrows() {
        DorisConfig.builder()
                .endpoints(Arrays.asList("http://127.0.0.1:8030"))
                .user("root").database("db")
                .format(new JsonFormat(JsonFormat.Type.OBJECT_LINE))
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyEndpointsThrows() {
        DorisConfig.builder()
                .user("root").database("db").table("t")
                .format(new JsonFormat(JsonFormat.Type.OBJECT_LINE))
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullFormatThrows() {
        DorisConfig.builder()
                .endpoints(Arrays.asList("http://127.0.0.1:8030"))
                .user("root").database("db").table("t")
                .build();
    }

    @Test
    public void testDefaultRetryValues() {
        RetryConfig retry = RetryConfig.defaultRetry();
        assertEquals(6, retry.getMaxRetryTimes());
        assertEquals(1000L, retry.getBaseIntervalMs());
        assertEquals(60000L, retry.getMaxTotalTimeMs());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeRetryTimesThrows() {
        RetryConfig.builder().maxRetryTimes(-1).baseIntervalMs(1000).maxTotalTimeMs(60000).build();
    }
}
