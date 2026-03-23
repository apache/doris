package org.apache.doris.sdk.load;

import org.apache.doris.sdk.load.config.*;
import org.apache.doris.sdk.load.exception.StreamLoadException;
import org.apache.doris.sdk.load.internal.StreamLoader;
import org.apache.doris.sdk.load.model.LoadResponse;
import org.apache.doris.sdk.load.model.RespContent;
import org.junit.Test;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class DorisLoadClientTest {

    private DorisConfig buildConfig() {
        return DorisConfig.builder()
                .endpoints(Arrays.asList("http://127.0.0.1:8030"))
                .user("root").password("secret")
                .database("testdb").table("users")
                .format(new JsonFormat(JsonFormat.Type.OBJECT_LINE))
                .retry(new RetryConfig.Builder()
                        .maxRetryTimes(2).baseIntervalMs(10).maxTotalTimeMs(5000).build())
                .groupCommit(GroupCommitMode.OFF)
                .build();
    }

    private RespContent successResp() {
        RespContent r = new RespContent();
        r.setStatus("Success");
        r.setNumberLoadedRows(3);
        r.setLoadBytes(100);
        return r;
    }

    @Test
    public void testSuccessOnFirstAttempt() throws Exception {
        StreamLoader mockLoader = mock(StreamLoader.class);
        when(mockLoader.execute(any())).thenReturn(LoadResponse.success(successResp()));

        DorisLoadClient client = new DorisLoadClient(buildConfig(), mockLoader);
        InputStream data = new ByteArrayInputStream("{\"id\":1}".getBytes());
        LoadResponse resp = client.load(data);

        assertEquals(LoadResponse.Status.SUCCESS, resp.getStatus());
        verify(mockLoader, times(1)).execute(any());
    }

    @Test
    public void testRetryOnStreamLoadException() throws Exception {
        StreamLoader mockLoader = mock(StreamLoader.class);
        when(mockLoader.execute(any()))
                .thenThrow(new StreamLoadException("connection refused"))
                .thenReturn(LoadResponse.success(successResp()));

        DorisLoadClient client = new DorisLoadClient(buildConfig(), mockLoader);
        InputStream data = new ByteArrayInputStream("test".getBytes());
        LoadResponse resp = client.load(data);

        assertEquals(LoadResponse.Status.SUCCESS, resp.getStatus());
        verify(mockLoader, times(2)).execute(any());
    }

    @Test
    public void testNoRetryOnBusinessFailure() throws Exception {
        RespContent failResp = new RespContent();
        failResp.setStatus("Fail");
        failResp.setMessage("table not found");
        StreamLoader mockLoader = mock(StreamLoader.class);
        when(mockLoader.execute(any())).thenReturn(LoadResponse.failure(failResp, "table not found"));

        DorisLoadClient client = new DorisLoadClient(buildConfig(), mockLoader);
        InputStream data = new ByteArrayInputStream("test".getBytes());
        LoadResponse resp = client.load(data);

        assertEquals(LoadResponse.Status.FAILURE, resp.getStatus());
        // Business failure should NOT be retried
        verify(mockLoader, times(1)).execute(any());
    }

    @Test
    public void testExhaustsAllRetries() throws Exception {
        StreamLoader mockLoader = mock(StreamLoader.class);
        when(mockLoader.execute(any())).thenThrow(new StreamLoadException("timeout"));

        DorisLoadClient client = new DorisLoadClient(buildConfig(), mockLoader);
        InputStream data = new ByteArrayInputStream("test".getBytes());

        try {
            client.load(data);
            fail("Expected IOException");
        } catch (IOException e) {
            // 1 initial + 2 retries = 3 total attempts
            verify(mockLoader, times(3)).execute(any());
        }
    }

    @Test
    public void testBackoffCalculation() {
        // attempt=1, base=1000ms → 1000ms
        assertEquals(1000, DorisLoadClient.calculateBackoffMs(1, 1000, 60000, 0));
        // attempt=2, base=1000ms → 2000ms
        assertEquals(2000, DorisLoadClient.calculateBackoffMs(2, 1000, 60000, 0));
        // attempt=3, base=1000ms → 4000ms
        assertEquals(4000, DorisLoadClient.calculateBackoffMs(3, 1000, 60000, 0));
        // constrained by remaining total time
        long constrained = DorisLoadClient.calculateBackoffMs(4, 1000, 60000, 55000);
        assertTrue("constrained interval should be <= remaining time", constrained <= 5000);
    }
}
