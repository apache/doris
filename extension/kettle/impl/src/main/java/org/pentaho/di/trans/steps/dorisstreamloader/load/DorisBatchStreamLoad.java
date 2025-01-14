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

package org.pentaho.di.trans.steps.dorisstreamloader.load;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.entity.GzipCompressingEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.pentaho.di.core.logging.LogChannelInterface;

import java.io.IOException;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.pentaho.di.trans.steps.dorisstreamloader.load.LoadConstants.ARROW;
import static org.pentaho.di.trans.steps.dorisstreamloader.load.LoadConstants.COMPRESS_TYPE;
import static org.pentaho.di.trans.steps.dorisstreamloader.load.LoadConstants.COMPRESS_TYPE_GZ;
import static org.pentaho.di.trans.steps.dorisstreamloader.load.LoadConstants.CSV;
import static org.pentaho.di.trans.steps.dorisstreamloader.load.LoadConstants.FORMAT_KEY;
import static org.pentaho.di.trans.steps.dorisstreamloader.load.LoadConstants.GROUP_COMMIT;
import static org.pentaho.di.trans.steps.dorisstreamloader.load.LoadConstants.GROUP_COMMIT_OFF_MODE;
import static org.pentaho.di.trans.steps.dorisstreamloader.load.LoadConstants.LINE_DELIMITER_DEFAULT;
import static org.pentaho.di.trans.steps.dorisstreamloader.load.LoadConstants.LINE_DELIMITER_KEY;
import static org.pentaho.di.trans.steps.dorisstreamloader.load.LoadStatus.PUBLISH_TIMEOUT;
import static org.pentaho.di.trans.steps.dorisstreamloader.load.LoadStatus.SUCCESS;

/** async stream load. */
public class DorisBatchStreamLoad implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final List<String> DORIS_SUCCESS_STATUS =
            new ArrayList<>(Arrays.asList(SUCCESS, PUBLISH_TIMEOUT));
    private static final long STREAM_LOAD_MAX_BYTES = 10 * 1024 * 1024 * 1024L; // 10 GB
    private static final long STREAM_LOAD_MAX_ROWS = Integer.MAX_VALUE;
    private final byte[] lineDelimiter;
    private static final String LOAD_URL_PATTERN = "http://%s/api/%s/%s/_stream_load";
    private String loadUrl;
    private String hostPort;
    private final String username;
    private final String password;
    private final Properties loadProps;
    private Map<String, BatchRecordBuffer> bufferMap = new ConcurrentHashMap<>();
    private ExecutorService loadExecutorService;
    private LoadAsyncExecutor loadAsyncExecutor;
    private BlockingQueue<BatchRecordBuffer> flushQueue;
    private final AtomicBoolean started;
    private volatile boolean loadThreadAlive = false;
    private AtomicReference<Throwable> exception = new AtomicReference<>(null);
    private HttpClientBuilder httpClientBuilder = new HttpUtil().getHttpClientBuilderForBatch();
    private boolean enableGroupCommit;
    private boolean enableGzCompress;
    private long maxBlockedBytes;
    private final AtomicLong currentCacheBytes = new AtomicLong(0L);
    private final Lock lock = new ReentrantLock();
    private final Condition block = lock.newCondition();
    private final int FLUSH_QUEUE_SIZE = 2;
    private DorisOptions options;
    private LogChannelInterface log;

    public DorisBatchStreamLoad(DorisOptions options, LogChannelInterface log) {
        this.log = log;
        this.options = options;
        this.hostPort = getAvailableHost(options.getFenodes());
        this.username = options.getUsername();
        this.password = options.getPassword();
        this.loadProps = options.getStreamLoadProp();
        if (loadProps.getProperty(FORMAT_KEY, CSV).equals(ARROW)) {
            this.lineDelimiter = null;
        } else {
            this.lineDelimiter =
                    EscapeHandler.escapeString(
                                    loadProps.getProperty(
                                            LINE_DELIMITER_KEY, LINE_DELIMITER_DEFAULT))
                            .getBytes();
        }
        this.enableGroupCommit =
                loadProps.containsKey(GROUP_COMMIT)
                        && !loadProps
                                .getProperty(GROUP_COMMIT)
                                .equalsIgnoreCase(GROUP_COMMIT_OFF_MODE);
        this.enableGzCompress = loadProps.getProperty(COMPRESS_TYPE, "").equals(COMPRESS_TYPE_GZ);
        this.flushQueue = new LinkedBlockingDeque<>(FLUSH_QUEUE_SIZE);
        // maxBlockedBytes ensures that a buffer can be written even if the queue is full
        this.maxBlockedBytes = options.getBufferFlushMaxBytes() * (FLUSH_QUEUE_SIZE + 1);
        this.loadUrl = String.format(LOAD_URL_PATTERN, hostPort, options.getDatabase(), options.getTable());
        this.loadAsyncExecutor = new LoadAsyncExecutor(FLUSH_QUEUE_SIZE);
        this.loadExecutorService =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(1),
                        new DefaultThreadFactory("streamload-executor"),
                        new ThreadPoolExecutor.AbortPolicy());
        this.started = new AtomicBoolean(true);
        this.loadExecutorService.execute(loadAsyncExecutor);
    }

    private String getAvailableHost(String fenodes) {
        List<String> nodes =
                Arrays.stream(fenodes.split(",")).map(String::trim).collect(Collectors.toList());
        Collections.shuffle(nodes);
        for (String node : nodes) {
            if (tryHttpConnection(node)) {
                return node;
            }
        }
        String errMsg = "No Doris FE is available, please check configuration";
        log.logError(errMsg);
        throw new DorisRuntimeException(errMsg);
    }

    public boolean tryHttpConnection(String host) {
        try {
            log.logDebug("try to connect host " +  host);
            host = "http://" + host;
            URL url = new URL(host);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(60000);
            connection.setReadTimeout(60000);
            int responseCode = connection.getResponseCode();
            String responseMessage = connection.getResponseMessage();
            connection.disconnect();
            if (responseCode < 500) {
                // code greater than 500 means a server-side exception.
                return true;
            }
            log.logDebug(
                    String.format("Failed to connect host %s, responseCode=%s, msg=%s",
                    host,
                    responseCode,
                    responseMessage));
            return false;
        } catch (Exception ex) {
            log.logDebug("Failed to connect to host:" + host, ex);
            return false;
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
        BatchRecordBuffer buffer =
                bufferMap.computeIfAbsent(
                        bufferKey,
                        k ->
                                new BatchRecordBuffer(
                                        database,
                                        table,
                                        this.lineDelimiter,
                                        1000));

        int bytes = buffer.insert(record);
        currentCacheBytes.addAndGet(bytes);
        if (currentCacheBytes.get() > maxBlockedBytes) {
            lock.lock();
            try {
                while (currentCacheBytes.get() >= maxBlockedBytes) {
                    checkFlushException();
                    log.logDetailed(
                            "Cache full, waiting for flush, currentBytes: " + currentCacheBytes.get()
                                    + ", maxBlockedBytes: " + maxBlockedBytes);
                    block.await(1, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                this.exception.set(e);
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }
        }

        // queue has space, flush according to the bufferMaxRows/bufferMaxBytes
        if (flushQueue.size() < FLUSH_QUEUE_SIZE
                && (buffer.getBufferSizeBytes() >= options.getBufferFlushMaxBytes()
                        || buffer.getNumOfRecords() >= options.getBufferFlushMaxRows())) {
            boolean flush = bufferFullFlush(bufferKey);
            log.logDetailed("trigger flush by buffer full, flush: " + flush);

        } else if (buffer.getBufferSizeBytes() >= STREAM_LOAD_MAX_BYTES
                || buffer.getNumOfRecords() >= STREAM_LOAD_MAX_ROWS) {
            // The buffer capacity exceeds the stream load limit, flush
            boolean flush = bufferFullFlush(bufferKey);
            log.logDetailed("trigger flush by buffer exceeding the limit, flush: " + flush);
        }
    }

    public boolean bufferFullFlush(String bufferKey) {
        return doFlush(bufferKey, false, true);
    }

    /**
     * Force flush and wait for success.
     * @return
     */
    public  boolean forceFlush() {
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
        if (bufferMap.isEmpty()) {
            // bufferMap may have been flushed by other threads
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
            log.logDetailed("buffer not found for key: {}, may be already flushed.", bufferKey);
        }
        if (waitUtilDone) {
            waitAsyncLoadFinish();
        }
        return true;
    }

    private synchronized void flushBuffer(String bufferKey) {
        BatchRecordBuffer buffer = bufferMap.get(bufferKey);
        String label = String.format("%s_%s_%s", "kettle", buffer.getTable(), UUID.randomUUID());
        buffer.setLabelName(label);
        log.logDetailed("Flush buffer, table " + bufferKey + ", records " + buffer.getNumOfRecords());
        putRecordToFlushQueue(buffer);
        bufferMap.remove(bufferKey);
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
    }

    private void checkFlushException() {
        if (exception.get() != null) {
            throw new DorisRuntimeException(exception.get());
        }
    }

    private void waitAsyncLoadFinish() {
        // Because the flush thread will drainTo once after polling is completed
        // if queue_size is 2, at least 4 empty queues must be consumed to ensure that flush has been completed
        for (int i = 0; i < FLUSH_QUEUE_SIZE * 2; i++) {
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
        // clear buffer
        this.flushQueue.clear();
    }

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
                log.logDetailed(
                        String.format("merge %s buffer to one stream load, result bufferBytes %s",
                                recordList.size(),
                                buffer.getBufferSizeBytes()));
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

    class LoadAsyncExecutor implements Runnable {

        private int flushQueueSize;

        public LoadAsyncExecutor(int flushQueueSize) {
            this.flushQueueSize = flushQueueSize;
        }

        @Override
        public void run() {
            log.logDetailed("LoadAsyncExecutor start");
            loadThreadAlive = true;
            List<BatchRecordBuffer> recordList = new ArrayList<>(flushQueueSize);
            while (started.get()) {
                recordList.clear();
                try {
                    BatchRecordBuffer buffer = flushQueue.poll(2000L, TimeUnit.MILLISECONDS);
                    if (buffer == null || buffer.getLabelName() == null) {
                        // label is empty and does not need to load. It is the flag of waitUtilDone
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
                                continue;
                            }
                            load(bf.getLabelName(), bf);
                        }
                    }
                } catch (Exception e) {
                    log.logError("worker running error", e);
                    exception.set(e);
                    // clear queue to avoid writer thread blocking
                    flushQueue.clear();
                    break;
                }
            }
            log.logDetailed("LoadAsyncExecutor stop");
            loadThreadAlive = false;
        }

        /** execute stream load. */
        public void load(String label, BatchRecordBuffer buffer) throws IOException {
            if (enableGroupCommit) {
                label = null;
            }
            refreshLoadUrl(buffer.getDatabase(), buffer.getTable());

            BatchBufferHttpEntity entity = new BatchBufferHttpEntity(buffer);
            HttpPutBuilder putBuilder = new HttpPutBuilder();
            putBuilder
                    .setUrl(loadUrl)
                    .baseAuth(username, password)
                    .setLabel(label)
                    .addCommonHeader()
                    .setEntity(entity)
                    .addHiddenColumns(options.isDeletable())
                    .addProperties(options.getStreamLoadProp());

            if (enableGzCompress) {
                putBuilder.setEntity(new GzipCompressingEntity(entity));
            }
            Throwable resEx = new Throwable();
            int retry = 0;
            while (retry <= options.getMaxRetries()) {
                if (enableGroupCommit) {
                    log.logDetailed("stream load started with group commit on host " + hostPort);
                } else {
                    log.logDetailed("stream load started for " + putBuilder.getLabel() + " on host " + hostPort);
                }

                try (CloseableHttpClient httpClient = httpClientBuilder.build()) {
                    try (CloseableHttpResponse response = httpClient.execute(putBuilder.build())) {
                        int statusCode = response.getStatusLine().getStatusCode();
                        String reason = response.getStatusLine().toString();
                        if (statusCode == 200 && response.getEntity() != null) {
                            String loadResult = EntityUtils.toString(response.getEntity());
                            log.logDetailed("load Result " + loadResult);
                            RespContent respContent =
                                    OBJECT_MAPPER.readValue(loadResult, RespContent.class);
                            if (DORIS_SUCCESS_STATUS.contains(respContent.getStatus())) {
                                long cacheByteBeforeFlush =
                                        currentCacheBytes.getAndAdd(-respContent.getLoadBytes());
                                log.logDetailed("load success, cacheBeforeFlushBytes: " + cacheByteBeforeFlush + ", currentCacheBytes : " + currentCacheBytes.get());
                                lock.lock();
                                try {
                                    block.signal();
                                } finally {
                                    lock.unlock();
                                }
                                return;
                            } else if (LoadStatus.LABEL_ALREADY_EXIST.equals(
                                    respContent.getStatus())) {
                                // todo: need to abort transaction when JobStatus not finished
                                putBuilder.setLabel(label + "_" + retry);
                                reason = respContent.getMessage();
                            } else {
                                String errMsg = null;
                                if (StringUtils.isBlank(respContent.getMessage())
                                    && StringUtils.isBlank(respContent.getErrorURL())) {
                                    // sometimes stream load will not return message
                                    errMsg =
                                        String.format(
                                            "stream load error, response is %s",
                                            loadResult);
                                    throw new DorisRuntimeException(errMsg);
                                } else {
                                    errMsg =
                                        String.format(
                                            "stream load error: %s, see more in %s",
                                            respContent.getMessage(),
                                            respContent.getErrorURL());
                                }
                                throw new DorisRuntimeException(errMsg);
                            }
                        }
                        log.logError(
                                String.format("stream load failed with %s, reason %s, to retry",
                                        hostPort,
                                        reason));
                        if (retry == options.getMaxRetries()) {
                            resEx = new DorisRuntimeException("stream load failed with: " + reason);
                        }
                    } catch (Exception ex) {
                        resEx = ex;
                        log.logError("stream load error with " + hostPort + ", to retry, cause by", ex);
                    }
                }
                retry++;
                // get available backend retry
                refreshLoadUrl(buffer.getDatabase(), buffer.getTable());
                putBuilder.setUrl(loadUrl);
            }
            buffer.clear();
            buffer = null;

            if (retry >= options.getMaxRetries()) {
                throw new DorisRuntimeException(
                        "stream load error: " + resEx.getMessage(), resEx);
            }
        }

        private void refreshLoadUrl(String database, String table) {
            hostPort = getAvailableHost(options.getFenodes());
            loadUrl = String.format(LOAD_URL_PATTERN, hostPort, database, table);
        }
    }

    public void setHttpClientBuilder(HttpClientBuilder httpClientBuilder) {
        this.httpClientBuilder = httpClientBuilder;
    }

    public AtomicReference<Throwable> getException() {
        return exception;
    }

    public boolean isLoadThreadAlive() {
        return loadThreadAlive;
    }
}
