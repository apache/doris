package org.apache.doris.sdk.load.config;

/**
 * Exponential backoff retry configuration.
 * Default: 6 retries, 1s base interval, 60s total time limit.
 * Backoff sequence: 1s, 2s, 4s, 8s, 16s, 32s (~63s total).
 */
public class RetryConfig {

    private final int maxRetryTimes;
    private final long baseIntervalMs;
    private final long maxTotalTimeMs;

    private RetryConfig(Builder builder) {
        this.maxRetryTimes = builder.maxRetryTimes;
        this.baseIntervalMs = builder.baseIntervalMs;
        this.maxTotalTimeMs = builder.maxTotalTimeMs;
    }

    /** Creates default retry config (6 retries, 1s base interval, 60s total limit). */
    public static RetryConfig defaultRetry() {
        return builder().maxRetryTimes(6).baseIntervalMs(1000).maxTotalTimeMs(60000).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public int getMaxRetryTimes() { return maxRetryTimes; }
    public long getBaseIntervalMs() { return baseIntervalMs; }
    public long getMaxTotalTimeMs() { return maxTotalTimeMs; }

    public static class Builder {
        private int maxRetryTimes = 6;
        private long baseIntervalMs = 1000;
        private long maxTotalTimeMs = 60000;

        public Builder maxRetryTimes(int val) { this.maxRetryTimes = val; return this; }
        public Builder baseIntervalMs(long val) { this.baseIntervalMs = val; return this; }
        public Builder maxTotalTimeMs(long val) { this.maxTotalTimeMs = val; return this; }

        public RetryConfig build() {
            if (maxRetryTimes < 0) throw new IllegalArgumentException("maxRetryTimes cannot be negative");
            if (baseIntervalMs < 0) throw new IllegalArgumentException("baseIntervalMs cannot be negative");
            if (maxTotalTimeMs < 0) throw new IllegalArgumentException("maxTotalTimeMs cannot be negative");
            return new RetryConfig(this);
        }
    }
}
