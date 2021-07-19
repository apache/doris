/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pulsar.io.doris;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A simple abstract class for Doris sink
 */
@Slf4j
public abstract class DorisAbstractSink<T> implements Sink<T> {

    // for httpclient
    protected String loadUrl = "";
    protected CloseableHttpClient client;
    protected HttpClientBuilder httpClientBuilder;
    protected HttpPut httpPut;

    // for job failover
    protected int job_failure_retries = 2;
    protected int job_label_repeat_retries = 3;

    // for flush
    private List<Record<T>> inComingRecordList;
    private List<Record<T>> swapRecordList;
    private AtomicBoolean isFlushing;
    private int batchSize;
    private ScheduledExecutorService flushExecutor;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        DorisSinkConfig dorisSinkConfig = DorisSinkConfig.load(config);

        String doris_host = dorisSinkConfig.getDoris_host();
        String doris_db = dorisSinkConfig.getDoris_db();
        String doris_table = dorisSinkConfig.getDoris_table();
        String doris_user = dorisSinkConfig.getDoris_user();
        String doris_password = dorisSinkConfig.getDoris_password();
        String doris_http_port = dorisSinkConfig.getDoris_http_port();
        job_failure_retries = Integer.parseInt(dorisSinkConfig.getJob_failure_retries());
        job_label_repeat_retries = Integer.parseInt(dorisSinkConfig.getJob_label_repeat_retries());
        Objects.requireNonNull(doris_host, "Doris Host is not set");
        Objects.requireNonNull(doris_db, "Doris Database is not set");
        Objects.requireNonNull(doris_table, "Doris Table is not set");
        Objects.requireNonNull(doris_user, "Doris User is not set");
        Objects.requireNonNull(doris_password, "Doris Password is not set");
        Objects.requireNonNull(doris_http_port, "Doris HTTP Port is not set");

        loadUrl = String.format("http://%s:%s/api/%s/%s/_stream_load",
                doris_host,
                doris_http_port,
                doris_db,
                doris_table);

        ServiceUnavailableRetryStrategy serviceUnavailableRetryStrategy = new MyServiceUnavailableRetryStrategy
                .Builder()
                .executionCount(3)
                .retryInterval(100)
                .build();
        httpClientBuilder = HttpClients
                .custom()
                .setRedirectStrategy(new DefaultRedirectStrategy() {
                    @Override
                    protected boolean isRedirectable(String method) {
                        return true;
                    }
                })
                .setServiceUnavailableRetryStrategy(serviceUnavailableRetryStrategy);
        client = httpClientBuilder.build();

        httpPut = new HttpPut(loadUrl);
        httpPut.setHeader(HttpHeaders.EXPECT, "100-continue");
        httpPut.setHeader(HttpHeaders.CONTENT_TYPE, "application/json;charset=UTF-8");
        httpPut.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(doris_user, doris_password));
        httpPut.setHeader("format", "json");
        httpPut.setHeader("strip_outer_array", "true");

        int timeout = dorisSinkConfig.getTimeout();
        batchSize = dorisSinkConfig.getBatchSize();
        isFlushing = new AtomicBoolean(false);
        inComingRecordList = Lists.newArrayList();
        swapRecordList = Lists.newArrayList();

        flushExecutor = Executors.newScheduledThreadPool(1);
        flushExecutor.scheduleAtFixedRate(() -> flush(), timeout, timeout, TimeUnit.MILLISECONDS);
    }

    private static String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded, StandardCharsets.UTF_8);
    }

    @Override
    public void write(Record<T> record) {
        int inComingRecordListSize;
        synchronized (this) {
            if (record != null) {
                inComingRecordList.add(record);
            }
            inComingRecordListSize = inComingRecordList.size();
        }
        if (inComingRecordListSize == batchSize) {
            flushExecutor.submit(() -> flush());
        }
    }

    private void flush() {
        // If not in flushing state, Exchange the data in inComingRecordList and swapRecordList, else return;
        synchronized (this) {
            if (inComingRecordList.size() > 0 && isFlushing.compareAndSet(false, true)) {
                List<Record<T>> tmpList;
                swapRecordList.clear();
                tmpList = swapRecordList;
                swapRecordList = inComingRecordList;
                inComingRecordList = tmpList;
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Already in flushing state, will not flush, queue size: {}", inComingRecordList.size());
                }
                return;
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("Starting flush, queue size: {}", swapRecordList.size());
        }

        int failJobRetryCount = 0;
        int jobLabelRepeatRetryCount = 0;
        try {
            sendData(swapRecordList, failJobRetryCount, jobLabelRepeatRetryCount);
        } catch (Exception e) {
            log.error("Doris put data exception ", e);
        }

        if (log.isDebugEnabled()) {
            log.debug("Finish flush, queue size: {}", swapRecordList.size());
        }
        swapRecordList.clear();
        isFlushing.compareAndSet(true, false);
    }

    @Override
    public void close() {
        if (client != null) {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        flushExecutor.shutdown();
    }

    public abstract void sendData(List<Record<T>> swapRecordList,
                                  int failJobRetryCount,
                                  int jobLabelRepeatRetryCount) throws Exception;

    public abstract void processLoadJobResult(String content,
                                              List<Record<T>> swapRecordList,
                                              CloseableHttpResponse response,
                                              Map dorisLoadResultMap,
                                              int failJobRetryCount,
                                              int jobLabelRepeatRetryCount) throws Exception;

}

class MyServiceUnavailableRetryStrategy implements ServiceUnavailableRetryStrategy {

    private int executionCount;
    private long retryInterval;

    MyServiceUnavailableRetryStrategy(Builder builder) {
        this.executionCount = builder.executionCount;
        this.retryInterval = builder.retryInterval;
    }

    @Override
    public boolean retryRequest(HttpResponse response,
                                int executionCount,
                                HttpContext context) {
        if (response.getStatusLine().getStatusCode() != 200 && executionCount < this.executionCount)
            return true;
        else
            return false;
    }

    @Override
    public long getRetryInterval() {
        return this.retryInterval;
    }

    public static final class Builder {
        private int executionCount;
        private long retryInterval;

        public Builder() {
            executionCount = 3;
            retryInterval = 1000;
        }

        public Builder executionCount(int executionCount) {
            this.executionCount = executionCount;
            return this;
        }

        public Builder retryInterval(long retryInterval) {
            this.retryInterval = retryInterval;
            return this;
        }

        public MyServiceUnavailableRetryStrategy build() {
            return new MyServiceUnavailableRetryStrategy(this);
        }
    }
}