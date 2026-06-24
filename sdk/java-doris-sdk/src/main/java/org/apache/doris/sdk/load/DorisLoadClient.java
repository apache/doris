// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.sdk.load;

import org.apache.doris.sdk.load.config.DorisConfig;
import org.apache.doris.sdk.load.exception.StreamLoadException;
import org.apache.doris.sdk.load.internal.RequestBuilder;
import org.apache.doris.sdk.load.internal.StreamLoader;
import org.apache.doris.sdk.load.model.LoadResponse;
import org.apache.http.client.methods.HttpPut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Thread-safe Doris stream load client.
 *
 * <p>Usage:
 * <pre>
 * DorisLoadClient client = new DorisLoadClient(config);
 * LoadResponse resp = client.load(new ByteArrayInputStream(data));
 * if (resp.getStatus() == LoadResponse.Status.SUCCESS) { ... }
 * </pre>
 *
 * <p>Thread safety: this instance can be shared across threads.
 * Each {@link #load} call must receive an independent InputStream.
 */
public class DorisLoadClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DorisLoadClient.class);
    /** Absolute maximum for a single backoff interval: 5 minutes. */
    private static final long ABSOLUTE_MAX_INTERVAL_MS = 300_000L;

    private final DorisConfig config;
    private final StreamLoader streamLoader;

    public DorisLoadClient(DorisConfig config) {
        this.config = config;
        this.streamLoader = new StreamLoader();
    }

    /** Package-private constructor for testing with a mock StreamLoader. */
    DorisLoadClient(DorisConfig config, StreamLoader streamLoader) {
        this.config = config;
        this.streamLoader = streamLoader;
    }

    /**
     * Loads data from the given InputStream into Doris via stream load.
     * The InputStream is fully consumed and buffered before the first attempt.
     * Retries with exponential backoff on retryable errors (network/HTTP failures).
     * Business failures (bad data, schema mismatch, auth) are returned immediately without retry.
     *
     * @param inputStream data to load (consumed once; must not be shared across threads)
     * @return LoadResponse with status SUCCESS or FAILURE
     * @throws IOException if the stream cannot be read or all retries are exhausted
     */
    public LoadResponse load(InputStream inputStream) throws IOException {
        int maxRetries = 6;
        long baseIntervalMs = 1000L;
        long maxTotalTimeMs = 60000L;

        if (config.getRetry() != null) {
            maxRetries = config.getRetry().getMaxRetryTimes();
            baseIntervalMs = config.getRetry().getBaseIntervalMs();
            maxTotalTimeMs = config.getRetry().getMaxTotalTimeMs();
        }

        log.info("Starting stream load: {}.{}", config.getDatabase(), config.getTable());

        // Buffer the InputStream once so retries can replay the body
        byte[] bodyData = readAll(inputStream);

        // Compress once before the retry loop (avoids re-compressing on each retry)
        if (config.isEnableGzip()) {
            bodyData = gzipCompress(bodyData);
        }
        Exception lastException = null;
        LoadResponse lastResponse = null;
        long operationStart = System.currentTimeMillis();

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            if (attempt > 0) {
                log.info("Retry attempt {}/{}", attempt, maxRetries);
                // Use actual wall-clock elapsed time (includes request time, not just sleep time)
                long elapsed = System.currentTimeMillis() - operationStart;
                long backoff = calculateBackoffMs(attempt, baseIntervalMs, maxTotalTimeMs, elapsed);

                if (maxTotalTimeMs > 0 && elapsed + backoff > maxTotalTimeMs) {
                    log.warn("Next retry backoff ({}ms) would exceed total limit ({}ms). Stopping.", backoff, maxTotalTimeMs);
                    break;
                }

                log.info("Waiting {}ms before retry (elapsed so far: {}ms)", backoff, elapsed);
                sleep(backoff);
            } else {
                log.info("Initial load attempt");
            }

            try {
                HttpPut request = RequestBuilder.build(config, bodyData, attempt);
                lastResponse = streamLoader.execute(request);

                if (lastResponse.getStatus() == LoadResponse.Status.SUCCESS) {
                    log.info("Stream load succeeded on attempt {}", attempt + 1);
                    return lastResponse;
                }

                // Business failure (bad data, schema mismatch, auth) — do not retry
                log.error("Load failed (non-retryable): {}", lastResponse.getErrorMessage());
                return lastResponse;

            } catch (StreamLoadException e) {
                // Retryable: network error, HTTP 5xx, etc.
                lastException = e;
                log.error("Attempt {} failed with retryable error: ", attempt + 1, e);

                // Check elapsed wall-clock time (same guard as the Exception branch below)
                long elapsed = System.currentTimeMillis() - operationStart;
                if (maxTotalTimeMs > 0 && elapsed > maxTotalTimeMs) {
                    log.warn("Total elapsed time ({}ms) exceeded limit ({}ms), stopping retries.", elapsed, maxTotalTimeMs);
                    break;
                }
            } catch (Exception e) {
                // Wrap unexpected exceptions as retryable
                lastException = new StreamLoadException("Unexpected error building request: " + e.getMessage(), e);
                log.error("Attempt {} failed with unexpected error: ", attempt + 1, e);

                // Check elapsed wall-clock time
                long elapsed = System.currentTimeMillis() - operationStart;
                if (maxTotalTimeMs > 0 && elapsed > maxTotalTimeMs) {
                    log.warn("Total elapsed time ({}ms) exceeded limit ({}ms), stopping retries.", elapsed, maxTotalTimeMs);
                    break;
                }
            }
        }

        log.debug("Total operation time: {}ms", System.currentTimeMillis() - operationStart);

        if (lastException != null) {
            throw new IOException("Stream load failed after " + (maxRetries + 1) + " attempts", lastException);
        }

        if (lastResponse != null) {
            return lastResponse;
        }

        throw new IOException("Stream load failed: unknown error");
    }

    /**
     * Calculates exponential backoff interval in milliseconds.
     * Package-private for unit testing.
     *
     * Formula: base * 2^(attempt-1), constrained by remaining total time and absolute max.
     *
     * @param elapsedMs actual wall-clock time elapsed since the operation started (includes request time)
     */
    static long calculateBackoffMs(int attempt, long baseIntervalMs, long maxTotalTimeMs, long elapsedMs) {
        if (attempt <= 0) return 0;
        long intervalMs = baseIntervalMs * (1L << (attempt - 1)); // base * 2^(attempt-1)

        if (maxTotalTimeMs > 0) {
            long remaining = maxTotalTimeMs - elapsedMs - 5000; // reserve 5s for the next request
            if (remaining <= 0) {
                intervalMs = 0;
            } else if (intervalMs > remaining) {
                intervalMs = remaining;
            }
        }

        if (intervalMs > ABSOLUTE_MAX_INTERVAL_MS) intervalMs = ABSOLUTE_MAX_INTERVAL_MS;
        if (intervalMs < 0) intervalMs = 0;
        return intervalMs;
    }

    private static byte[] readAll(InputStream inputStream) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] chunk = new byte[8192];
        int read;
        while ((read = inputStream.read(chunk)) != -1) {
            buffer.write(chunk, 0, read);
        }
        return buffer.toByteArray();
    }

    private static byte[] gzipCompress(byte[] data) throws IOException {
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(compressed)) {
            gzip.write(data);
        }
        return compressed.toByteArray();
    }

    private static void sleep(long ms) {
        if (ms <= 0) return;
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() throws IOException {
        streamLoader.close();
    }
}
