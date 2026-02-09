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

import org.apache.doris.cdcclient.common.Env;
import org.apache.doris.cdcclient.exception.StreamLoadException;
import org.apache.doris.cdcclient.utils.HttpUtil;
import org.apache.doris.job.cdc.request.CommitOffsetRequest;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** async stream load. */
public class DorisBatchStreamLoad implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DorisBatchStreamLoad.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final List<String> DORIS_SUCCESS_STATUS =
            new ArrayList<>(Arrays.asList("Success"));
    private final int FLUSH_QUEUE_SIZE = 1;
    private final long STREAM_LOAD_MAX_BYTES = 500 * 1024 * 1024L; // 500MB
    private final int RETRY = 3;
    private final byte[] lineDelimiter = "\n".getBytes();
    private static final String LOAD_URL_PATTERN = "http://%s/api/%s/%s/_stream_load";
    private static final String COMMIT_URL_PATTERN = "http://%s/api/streaming/commit_offset";
    private String hostPort;
    @Setter private String frontendAddress;
    private Map<String, BatchRecordBuffer> bufferMap = new ConcurrentHashMap<>();
    private ExecutorService loadExecutorService;
    private LoadAsyncExecutor loadAsyncExecutor;
    private BlockingQueue<BatchRecordBuffer> flushQueue;
    private final AtomicBoolean started;
    private volatile boolean loadThreadAlive = false;
    private final CountDownLatch loadThreadStarted = new CountDownLatch(1);
    private AtomicReference<Throwable> exception = new AtomicReference<>(null);
    private long maxBlockedBytes;
    private final AtomicLong currentCacheBytes = new AtomicLong(0L);
    private final Lock lock = new ReentrantLock();
    private final Condition block = lock.newCondition();
    private final Map<String, ReadWriteLock> bufferMapLock = new ConcurrentHashMap<>();
    @Setter @Getter private String currentTaskId;
    private String targetDb;
    private long jobId;
    @Setter private String token;
    // stream load headers
    @Setter private Map<String, String> loadProps = new HashMap<>();
    @Getter private LoadStatistic loadStatistic;

    public DorisBatchStreamLoad(long jobId, String targetDb) {
        this.hostPort = Env.getCurrentEnv().getBackendHostPort();
        this.loadStatistic = new LoadStatistic();
        this.flushQueue = new LinkedBlockingDeque<>(1);
        // maxBlockedBytes is two times of FLUSH_MAX_BYTE_SIZE
        this.maxBlockedBytes = STREAM_LOAD_MAX_BYTES * 2;
        this.loadAsyncExecutor = new LoadAsyncExecutor(FLUSH_QUEUE_SIZE, jobId);
        this.loadExecutorService =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(1),
                        new DefaultThreadFactory("stream-load-executor-" + jobId),
                        new ThreadPoolExecutor.AbortPolicy());
        this.started = new AtomicBoolean(true);
        this.loadExecutorService.execute(loadAsyncExecutor);
        this.targetDb = targetDb;
        this.jobId = jobId;
        // Wait for the load thread to start
        try {
            if (!loadThreadStarted.await(10, TimeUnit.SECONDS)) {
                throw new RuntimeException("LoadAsyncExecutor thread startup timed out");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                    "Thread interrupted while waiting for load thread to start", e);
        }
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
                        bufferKey, k -> new BatchRecordBuffer(database, table, this.lineDelimiter));

        int bytes = buffer.insert(record);
        currentCacheBytes.addAndGet(bytes);
        getLock(bufferKey).readLock().unlock();

        if (currentCacheBytes.get() > maxBlockedBytes) {
            cacheFullFlush();
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

        // Single table flush according to the STREAM_LOAD_MAX_BYTES
        if (buffer.getBufferSizeBytes() >= STREAM_LOAD_MAX_BYTES) {
            bufferFullFlush(bufferKey);
        }
    }

    public void bufferFullFlush(String bufferKey) {
        doFlush(bufferKey, false, true);
    }

    public void forceFlush() {
        doFlush(null, true, false);
    }

    public void cacheFullFlush() {
        doFlush(null, true, true);
    }

    private synchronized void doFlush(String bufferKey, boolean waitUtilDone, boolean bufferFull) {
        checkFlushException();
        if (waitUtilDone || bufferFull) {
            flush(bufferKey, waitUtilDone);
        }
    }

    private synchronized void flush(String bufferKey, boolean waitUtilDone) {
        if (!waitUtilDone && bufferMap.isEmpty()) {
            // bufferMap may have been flushed by other threads
            LOG.info("bufferMap is empty, no need to flush");
            return;
        }

        if (null == bufferKey) {
            for (String key : bufferMap.keySet()) {
                if (waitUtilDone) {
                    flushBuffer(key);
                }
            }
        } else if (bufferMap.containsKey(bufferKey)) {
            flushBuffer(bufferKey);
        } else {
            LOG.warn("buffer not found for key: {}, may be already flushed.", bufferKey);
        }
        if (waitUtilDone) {
            waitAsyncLoadFinish();
        }
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
        buffer.setLabelName(UUID.randomUUID().toString().replace("-", ""));
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
        private long jobId;

        public LoadAsyncExecutor(int flushQueueSize, long jobId) {
            this.flushQueueSize = flushQueueSize;
            this.jobId = jobId;
        }

        @Override
        public void run() {
            LOG.info("LoadAsyncExecutor start for jobId {}", jobId);
            loadThreadAlive = true;
            loadThreadStarted.countDown();
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
            LOG.info("LoadAsyncExecutor stop for jobId {}", jobId);
            loadThreadAlive = false;
        }

        /** execute stream load. */
        public void load(String label, BatchRecordBuffer buffer) throws IOException {
            BatchBufferHttpEntity entity = new BatchBufferHttpEntity(buffer);
            HttpPutBuilder putBuilder = new HttpPutBuilder();

            String loadUrl = String.format(LOAD_URL_PATTERN, hostPort, targetDb, buffer.getTable());
            String finalLabel = String.format("%s_%s_%s", jobId, currentTaskId, label);
            putBuilder
                    .setUrl(loadUrl)
                    .addProperties(loadProps)
                    .addTokenAuth(token)
                    .setLabel(finalLabel)
                    .formatJson()
                    .addCommonHeader()
                    .addHiddenColumns(true)
                    .setEntity(entity);

            Throwable resEx = new Throwable();
            int retry = 0;
            while (retry <= RETRY) {
                LOG.info("stream load started for {} on host {}", putBuilder.getLabel(), hostPort);
                try (CloseableHttpClient httpClient = HttpUtil.getHttpClient()) {
                    try (CloseableHttpResponse response = httpClient.execute(putBuilder.build())) {
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
                                loadStatistic.add(respContent);
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

    public void resetTaskId() {
        this.currentTaskId = null;
    }

    /** commit offfset to frontends. */
    public void commitOffset(
            String taskId,
            List<Map<String, String>> meta,
            long scannedRows,
            LoadStatistic loadStatistic) {
        try {
            String url = String.format(COMMIT_URL_PATTERN, frontendAddress, targetDb);
            CommitOffsetRequest commitRequest =
                    CommitOffsetRequest.builder()
                            .offset(OBJECT_MAPPER.writeValueAsString(meta))
                            .jobId(jobId)
                            .taskId(Long.parseLong(taskId))
                            .scannedRows(scannedRows)
                            .filteredRows(loadStatistic.getFilteredRows())
                            .loadedRows(loadStatistic.getLoadedRows())
                            .loadBytes(loadStatistic.getLoadBytes())
                            .build();
            String param = OBJECT_MAPPER.writeValueAsString(commitRequest);

            HttpPutBuilder builder =
                    new HttpPutBuilder()
                            .addCommonHeader()
                            .addBodyContentType()
                            .addTokenAuth(token)
                            .setUrl(url)
                            .commit()
                            .setEntity(new StringEntity(param));

            LOG.info("commit offset for jobId {} taskId {}, params {}", jobId, taskId, param);
            Throwable resEx = null;
            int retry = 0;
            while (retry <= RETRY) {
                try (CloseableHttpClient httpClient = HttpUtil.getHttpClient()) {
                    try (CloseableHttpResponse httpResponse = httpClient.execute(builder.build())) {
                        int statusCode = httpResponse.getStatusLine().getStatusCode();
                        String reason = httpResponse.getStatusLine().toString();
                        String responseBody =
                                httpResponse.getEntity() != null
                                        ? EntityUtils.toString(httpResponse.getEntity())
                                        : "";
                        LOG.info("commit result {}", responseBody);
                        if (statusCode == 200) {
                            LOG.info("commit offset for jobId {} taskId {}", jobId, taskId);
                            // A 200 response indicates that the request was successful, and
                            // information such as offset and statistics may have already been
                            // updated. Retrying may result in repeated updates.
                            return;
                        }
                        LOG.error(
                                "commit offset failed with {}, reason {}, to retry",
                                hostPort,
                                reason);
                        if (retry == RETRY) {
                            resEx = new StreamLoadException("commit offset failed with: " + reason);
                        }
                    } catch (Exception ex) {
                        resEx = ex;
                        LOG.error("commit offset error with {}, to retry, cause by", hostPort, ex);
                    }
                }
                retry++;
                if (retry <= RETRY) {
                    try {
                        Thread.sleep(retry * 1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }
            }

            if (retry > RETRY) {
                throw new StreamLoadException(
                        "commit offset error: "
                                + (resEx != null ? resEx.getMessage() : "unknown error"),
                        resEx);
            }
        } catch (Exception ex) {
            LOG.error("Failed to commit offset, jobId={}", jobId, ex);
            throw new StreamLoadException("Failed to commit offset", ex);
        }
    }
}
