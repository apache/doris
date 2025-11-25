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

package org.apache.doris.cdcclient.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.cdcclient.exception.StreamLoadException;
import org.apache.doris.cdcclient.utils.HttpUtil;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** async stream load. */
public class DorisBatchStreamLoad implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DorisBatchStreamLoad.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final List<String> DORIS_SUCCESS_STATUS =
            new ArrayList<>(Arrays.asList("Success"));
    private static final long STREAM_LOAD_MAX_BYTES = 1 * 1024 * 1024 * 1024L; // 1 GB
    private static final long STREAM_LOAD_MAX_ROWS = Integer.MAX_VALUE;
    private final byte[] lineDelimiter = "\n".getBytes();
    ;
    private static final String LOAD_URL_PATTERN = "http://%s/api/%s/%s/_stream_load";
    private String loadUrl;
    private String hostPort;
    private Map<String, BatchRecordBuffer> bufferMap = new ConcurrentHashMap<>();
    private ExecutorService loadExecutorService;
    private LoadAsyncExecutor loadAsyncExecutor;
    private BlockingQueue<BatchRecordBuffer> flushQueue;
    private final AtomicBoolean started;
    private volatile boolean loadThreadAlive = false;
    private AtomicReference<Throwable> exception = new AtomicReference<>(null);
    private long maxBlockedBytes;
    private final AtomicLong currentCacheBytes = new AtomicLong(0L);
    private final Lock lock = new ReentrantLock();
    private final Condition block = lock.newCondition();
    private final Map<String, ReadWriteLock> bufferMapLock = new ConcurrentHashMap<>();
    private int FLUSH_QUEUE_SIZE = 1;
    private int FLUSH_MAX_BYTE_SIZE = 100 * 1024 * 1024;
    private int FLUSH_INTERVAL_MS = 10 * 1000;
    private int RETRY = 3;

    public DorisBatchStreamLoad() {
        this.hostPort = "10.16.10.6:28747";
        this.flushQueue = new LinkedBlockingDeque<>(1);
        // maxBlockedBytes ensures that a buffer can be written even if the queue is full
        this.maxBlockedBytes = (long) FLUSH_MAX_BYTE_SIZE * (FLUSH_QUEUE_SIZE + 1);
        this.loadAsyncExecutor = new LoadAsyncExecutor(FLUSH_QUEUE_SIZE);
        this.loadExecutorService =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(1),
                        new DefaultThreadFactory("stream-load-executor"),
                        new ThreadPoolExecutor.AbortPolicy());
        this.started = new AtomicBoolean(true);
        this.loadExecutorService.execute(loadAsyncExecutor);
    }

    /**
     * write record into cache.
     *
     * @param record
     * @throws IOException
     */
    public void writeRecord(String database, String table, byte[] record) {
        checkFlushException();
        String bufferKey = getTableIdentifier(database, table);

        getLock(bufferKey).readLock().lock();
        BatchRecordBuffer buffer =
                bufferMap.computeIfAbsent(
                        bufferKey,
                        k ->
                                new BatchRecordBuffer(
                                        database, table, this.lineDelimiter, FLUSH_INTERVAL_MS));

        int bytes = buffer.insert(record);
        currentCacheBytes.addAndGet(bytes);
        getLock(bufferKey).readLock().unlock();

        if (currentCacheBytes.get() > maxBlockedBytes) {
            lock.lock();
            try {
                while (currentCacheBytes.get() >= maxBlockedBytes) {
                    checkFlushException();
                    LOG.info(
                            "Cache full, waiting for flush, currentBytes: {}, maxBlockedBytes: {}",
                            currentCacheBytes.get(),
                            maxBlockedBytes);
                    block.await(1, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                this.exception.set(e);
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }
        }
    }

    public boolean forceFlush() {
        return doFlush(null, true, false);
    }

    private synchronized boolean doFlush(
            String bufferKey, boolean waitUtilDone, boolean bufferFull) {
        checkFlushException();
        if (waitUtilDone || bufferFull) {
            boolean flush = flush(bufferKey, waitUtilDone);
            return flush;
        } else if (flushQueue.size() < FLUSH_QUEUE_SIZE) {
            boolean flush = flush(bufferKey, false);
            return flush;
        }
        return false;
    }

    private synchronized boolean flush(String bufferKey, boolean waitUtilDone) {
        if (!waitUtilDone && bufferMap.isEmpty()) {
            // bufferMap may have been flushed by other threads
            LOG.info("bufferMap is empty, no need to flush {}", bufferKey);
            return false;
        }
        if (null == bufferKey) {
            boolean flush = false;
            for (String key : bufferMap.keySet()) {
                BatchRecordBuffer buffer = bufferMap.get(key);
                if (waitUtilDone || buffer.shouldFlush()) {
                    // Ensure that the interval satisfies intervalMS
                    flushBuffer(key);
                    flush = true;
                }
            }
            if (!waitUtilDone && !flush) {
                return false;
            }
        } else if (bufferMap.containsKey(bufferKey)) {
            flushBuffer(bufferKey);
        } else {
            LOG.warn("buffer not found for key: {}, may be already flushed.", bufferKey);
        }
        if (waitUtilDone) {
            waitAsyncLoadFinish();
        }
        return true;
    }

    private synchronized void flushBuffer(String bufferKey) {
        BatchRecordBuffer buffer;
        try {
            getLock(bufferKey).writeLock().lock();
            buffer = bufferMap.remove(bufferKey);
        } finally {
            getLock(bufferKey).writeLock().unlock();
        }
        if (buffer == null) {
            LOG.info("buffer key is not exist {}, skipped", bufferKey);
            return;
        }
        buffer.setLabelName(UUID.randomUUID().toString());
        LOG.debug("flush buffer for key {} with label {}", bufferKey, buffer.getLabelName());
        putRecordToFlushQueue(buffer);
    }

    private void putRecordToFlushQueue(BatchRecordBuffer buffer) {
        checkFlushException();
        if (!loadThreadAlive) {
            throw new RuntimeException("load thread already exit, write was interrupted");
        }
        try {
            flushQueue.put(buffer);
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to put record buffer to flush queue");
        }
        // When the load thread reports an error, the flushQueue will be cleared,
        // and need to force a check for the exception.
        checkFlushException();
    }

    private void checkFlushException() {
        if (exception.get() != null) {
            throw new StreamLoadException(exception.get());
        }
    }

    private void waitAsyncLoadFinish() {
        // Because the queue will have a drainTo operation, it needs to be multiplied by 2
        for (int i = 0; i < FLUSH_QUEUE_SIZE * 2 + 1; i++) {
            // eof buffer
            BatchRecordBuffer empty = new BatchRecordBuffer();
            putRecordToFlushQueue(empty);
        }
    }

    private String getTableIdentifier(String database, String table) {
        return database + "." + table;
    }

    public void close() {
        // close async executor
        this.loadExecutorService.shutdown();
        this.started.set(false);
    }

    @VisibleForTesting
    public boolean mergeBuffer(List<BatchRecordBuffer> recordList, BatchRecordBuffer buffer) {
        boolean merge = false;
        if (recordList.size() > 1) {
            boolean sameTable =
                    recordList.stream()
                                    .map(BatchRecordBuffer::getTableIdentifier)
                                    .distinct()
                                    .count()
                            == 1;
            // Buffers can be merged only if they belong to the same table.
            if (sameTable) {
                for (BatchRecordBuffer recordBuffer : recordList) {
                    if (recordBuffer != null
                            && recordBuffer.getLabelName() != null
                            && !buffer.getLabelName().equals(recordBuffer.getLabelName())
                            && !recordBuffer.getBuffer().isEmpty()) {
                        merge(buffer, recordBuffer);
                        merge = true;
                    }
                }
                LOG.info(
                        "merge {} buffer to one stream load, result bufferBytes {}",
                        recordList.size(),
                        buffer.getBufferSizeBytes());
            }
        }
        return merge;
    }

    private boolean merge(BatchRecordBuffer mergeBuffer, BatchRecordBuffer buffer) {
        if (buffer.getBuffer().isEmpty()) {
            return false;
        }
        if (!mergeBuffer.getBuffer().isEmpty()) {
            mergeBuffer.getBuffer().add(mergeBuffer.getLineDelimiter());
            mergeBuffer.setBufferSizeBytes(
                    mergeBuffer.getBufferSizeBytes() + mergeBuffer.getLineDelimiter().length);
            currentCacheBytes.addAndGet(buffer.getLineDelimiter().length);
        }
        mergeBuffer.getBuffer().addAll(buffer.getBuffer());
        mergeBuffer.setNumOfRecords(mergeBuffer.getNumOfRecords() + buffer.getNumOfRecords());
        mergeBuffer.setBufferSizeBytes(
                mergeBuffer.getBufferSizeBytes() + buffer.getBufferSizeBytes());
        return true;
    }

    private ReadWriteLock getLock(String bufferKey) {
        return bufferMapLock.computeIfAbsent(bufferKey, k -> new ReentrantReadWriteLock());
    }

    class LoadAsyncExecutor implements Runnable {

        private int flushQueueSize;

        public LoadAsyncExecutor(int flushQueueSize) {
            this.flushQueueSize = flushQueueSize;
        }

        @Override
        public void run() {
            LOG.info("LoadAsyncExecutor start");
            loadThreadAlive = true;
            List<BatchRecordBuffer> recordList = new ArrayList<>(flushQueueSize);
            while (started.get()) {
                recordList.clear();
                try {
                    BatchRecordBuffer buffer = flushQueue.poll(2000L, TimeUnit.MILLISECONDS);
                    if (buffer == null) {
                        continue;
                    }
                    if (buffer.getLabelName() == null) {
                        // When the label is empty, it is the eof buffer for checkpoint flush.
                        continue;
                    }

                    recordList.add(buffer);
                    boolean merge = false;
                    if (!flushQueue.isEmpty()) {
                        flushQueue.drainTo(recordList, flushQueueSize - 1);
                        if (mergeBuffer(recordList, buffer)) {
                            load(buffer.getLabelName(), buffer);
                            merge = true;
                        }
                    }

                    if (!merge) {
                        for (BatchRecordBuffer bf : recordList) {
                            if (bf == null || bf.getLabelName() == null) {
                                // When the label is empty, it's eof buffer for checkpointFlush.
                                continue;
                            }
                            load(bf.getLabelName(), bf);
                        }
                    }
                } catch (Exception e) {
                    LOG.error("worker running error", e);
                    exception.set(e);
                    // clear queue to avoid writer thread blocking
                    flushQueue.clear();
                    break;
                }
            }
            LOG.info("LoadAsyncExecutor stop");
            loadThreadAlive = false;
        }

        /** execute stream load. */
        public void load(String label, BatchRecordBuffer buffer) throws IOException {
            BatchBufferHttpEntity entity = new BatchBufferHttpEntity(buffer);
            HttpPut put =
                    new HttpPut(
                            String.format(
                                    LOAD_URL_PATTERN,
                                    hostPort,
                                    buffer.getDatabase(),
                                    buffer.getTable()));
            put.addHeader(HttpHeaders.EXPECT, "100-continue");
            put.addHeader("read_json_by_line", "true");
            put.addHeader("format", "json");
            put.addHeader(
                    HttpHeaders.AUTHORIZATION,
                    "Basic "
                            + new String(
                                    Base64.encodeBase64("root:".getBytes(StandardCharsets.UTF_8))));
            put.setEntity(entity);

            Throwable resEx = new Throwable();
            int retry = 0;
            while (retry <= RETRY) {
                try (CloseableHttpClient httpClient = HttpUtil.getHttpClient()) {
                    try (CloseableHttpResponse response = httpClient.execute(put)) {
                        int statusCode = response.getStatusLine().getStatusCode();
                        String reason = response.getStatusLine().toString();
                        if (statusCode == 200 && response.getEntity() != null) {
                            String loadResult = EntityUtils.toString(response.getEntity());
                            LOG.info("load Result {}", loadResult);
                            RespContent respContent =
                                    OBJECT_MAPPER.readValue(loadResult, RespContent.class);
                            if (DORIS_SUCCESS_STATUS.contains(respContent.getStatus())) {
                                long cacheByteBeforeFlush =
                                        currentCacheBytes.getAndAdd(-respContent.getLoadBytes());
                                LOG.info(
                                        "load success, cacheBeforeFlushBytes: {}, currentCacheBytes : {}",
                                        cacheByteBeforeFlush,
                                        currentCacheBytes.get());
                                lock.lock();
                                try {
                                    block.signal();
                                } finally {
                                    lock.unlock();
                                }
                                return;
                            } else {
                                String errMsg = null;
                                if (StringUtils.isBlank(respContent.getMessage())
                                        && StringUtils.isBlank(respContent.getErrorURL())) {
                                    // sometimes stream load will not return message
                                    errMsg =
                                            String.format(
                                                    "stream load error, response is %s",
                                                    loadResult);
                                    throw new StreamLoadException(errMsg);
                                } else {
                                    errMsg =
                                            String.format(
                                                    "stream load error: %s, see more in %s",
                                                    respContent.getMessage(),
                                                    respContent.getErrorURL());
                                }
                                throw new StreamLoadException(errMsg);
                            }
                        }
                        LOG.error(
                                "stream load failed with {}, reason {}, to retry",
                                hostPort,
                                reason);
                        if (retry == RETRY) {
                            resEx = new StreamLoadException("stream load failed with: " + reason);
                        }
                    } catch (Exception ex) {
                        resEx = ex;
                        LOG.error("stream load error with {}, to retry, cause by", hostPort, ex);
                    }
                }
                retry++;
                try {
                    Thread.sleep(retry * 1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            buffer.clear();
            buffer = null;

            if (retry >= RETRY) {
                throw new StreamLoadException("stream load error: " + resEx.getMessage(), resEx);
            }
        }
    }

    static class DefaultThreadFactory implements ThreadFactory {
        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        DefaultThreadFactory(String name) {
            namePrefix = "pool-" + poolNumber.getAndIncrement() + "-" + name + "-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, namePrefix + threadNumber.getAndIncrement());
            t.setDaemon(false);
            return t;
        }
    }

    @VisibleForTesting
    public AtomicReference<Throwable> getException() {
        return exception;
    }

    @VisibleForTesting
    public boolean isLoadThreadAlive() {
        return loadThreadAlive;
    }
}
