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

package org.apache.doris.metric.collector;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.Config;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Master of fe use this class to start a thread to get metric data from each node
 * and write it to the bdbje periodically.
 */
public class MetricCollector {
    private static final Logger LOG = LogManager.getLogger(MetricCollector.class);

    private BDBJEMetricHandler bdbjeMetricHandler;
    private volatile boolean running = true;
    private volatile boolean started = false;
    private Thread writeMetricThread;

    private long beDisksDataUsedCapacity;
    private long beDisksTotalCapacity;

    public MetricCollector(BDBJEMetricHandler bdbjeMetricHandler) {
        this.bdbjeMetricHandler = bdbjeMetricHandler;
        init();
    }

    private void init() {
        writeMetricThread = new Thread(() -> {
            // Write metric data when current timestamp is a multiple of the WRITE_INTERVAL_MS.
            // If not, sleep until the timestamp is a multiple of the WRITE_INTERVAL_MS.
            while (running) {
                long currentTime = System.currentTimeMillis();
                // writeMetricTime is a multiple of writeIntervalMilliSeconds upward.
                long writeMetricTime =
                        currentTime + BDBJEMetricUtils.WRITE_INTERVAL_MS - currentTime % BDBJEMetricUtils.WRITE_INTERVAL_MS;
                try {
                    TimeUnit.MILLISECONDS.sleep(writeMetricTime - currentTime);
                } catch (InterruptedException e) {
                    // No need to deal with.
                }
                try {
                    writeMetric(writeMetricTime);
                } catch (Exception e) {
                    LOG.error("write metric into bdbje error.", e);
                }
            }
            LOG.info("the write metric bdbje thread exit.");
        });
    }

    public void startWriteMetric() {
        if (!started) {
            running = true;
            writeMetricThread.start();
            started = true;
            LOG.info("start the write metric bdbje thread.");
        }
    }

    // get metric data from each node and write it to the bdbje.
    private void writeMetric(long timestamp) {
        List<Frontend> frontends = Catalog.getCurrentCatalog().getFrontends(null);
        for (Frontend frontend : frontends) {
            if (frontend.isAlive()) {
                String url = "http://" + frontend.getHost() + ":" + Config.http_port + "/metrics?type=json";
                Reader feMetricsReader = requestMetrics(url);
                if (feMetricsReader == null) {
                    continue;
                }
                parseFeMetricJsonAndWriteMetric(frontend.getHost(), Config.http_port, timestamp, feMetricsReader);
            }
        }

        beDisksDataUsedCapacity = 0;
        beDisksTotalCapacity = 0;
        SystemInfoService systemInfoService = Catalog.getCurrentSystemInfo();
        for (long beId : systemInfoService.getBackendIds(true)) {
            Backend be = systemInfoService.getBackend(beId);
            String url = "http://" + be.getHost() + ":" + be.getHttpPort() + "/metrics?type=json";
            Reader metricsJson = requestMetrics(url);
            if (metricsJson == null) {
                continue;
            }
            parseBeMetricJsonAndWriteMetric(be.getHost(), be.getHttpPort(), timestamp, metricsJson);
        }
        // over write
        bdbjeMetricHandler.writeLong(BDBJEMetricUtils.BE_DISKS_DATA_USED_CAPACITY, beDisksDataUsedCapacity);
        bdbjeMetricHandler.writeLong(BDBJEMetricUtils.BE_DISKS_TOTAL_CAPACITY, beDisksTotalCapacity);
    }

    // request metrics json data form node by http server.
    private Reader requestMetrics(String urlString) {
        try {
            URL url = new URL(urlString);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            if (conn.getResponseCode() != 200) {
                LOG.error("failed to get metric from {}. response code: {}", urlString, conn.getResponseCode());
                return null;
            }
            return new InputStreamReader(conn.getInputStream());
        } catch (IOException e) {
            LOG.warn("failed to get metric from {}. {}", urlString, e.getMessage());
        }
        return null;
    }

    private void parseBeMetricJsonAndWriteMetric(String host, int port, long timestamp, Reader metricsJson) {
        /**
         * The json of metrics is as follows:
         * [{
         *     "tags": {
         *          "metric": "cpu",
         *          "mode": "idle"
         *     },
         *     "unit": "percent",
         *     "value": 545076644889
         * }]
         */
        try {
            JsonArray jsonMetrics = JsonParser.parseReader(metricsJson).getAsJsonArray();
            long cpuTotalTime = 0L;
            for (JsonElement element : jsonMetrics) {
                JsonObject metric = element.getAsJsonObject();
                String metricName = metric.get(BDBJEMetricUtils.TAGS).getAsJsonObject().get(BDBJEMetricUtils.METRIC)
                        .getAsString();
                JsonElement value = metric.get(BDBJEMetricUtils.VALUE);
                switch (metricName) {
                    case BDBJEMetricUtils.METRIC_CPU:
                        cpuTotalTime += value.getAsLong();
                        if (metric.get(BDBJEMetricUtils.TAGS).getAsJsonObject().get("mode").getAsString().equals("idle")) {
                            // cpu_idle
                            bdbjeMetricHandler.writeLong(BDBJEMetricUtils.concatBdbKey(host, port,
                                    BDBJEMetricUtils.CPU_IDLE, timestamp), value.getAsLong());
                        }
                        break;
                    case BDBJEMetricUtils.METRIC_MEMORY_ALLOCATED_BYTES:
                        bdbjeMetricHandler.writeLong(BDBJEMetricUtils.concatBdbKey(host, port,
                                BDBJEMetricUtils.METRIC_MEMORY_ALLOCATED_BYTES, timestamp), value.getAsLong());
                        break;
                    case BDBJEMetricUtils.METRIC_MAX_DISK_IO_UTIL_PERCENT:
                        bdbjeMetricHandler.writeLong(BDBJEMetricUtils.concatBdbKey(host, port,
                                BDBJEMetricUtils.METRIC_MAX_DISK_IO_UTIL_PERCENT, timestamp), value.getAsLong());
                        break;
                    case BDBJEMetricUtils.DISKS_DATA_USED_CAPACITY:
                        beDisksDataUsedCapacity += value.getAsLong();
                        break;
                    case BDBJEMetricUtils.DISKS_TOTAL_CAPACITY:
                        beDisksTotalCapacity += value.getAsLong();
                        break;
                    case BDBJEMetricUtils.BASE_COMPACTION_SCORE:
                        bdbjeMetricHandler.writeLong(BDBJEMetricUtils.concatBdbKey(host, port,
                                BDBJEMetricUtils.BASE_COMPACTION_SCORE, timestamp), value.getAsLong());
                        break;
                    case BDBJEMetricUtils.CUMU_COMPACTION_SCORE:
                        bdbjeMetricHandler.writeLong(BDBJEMetricUtils.concatBdbKey(host, port,
                                BDBJEMetricUtils.CUMU_COMPACTION_SCORE, timestamp), value.getAsLong());
                        break;
                }
            }
            bdbjeMetricHandler.writeLong(BDBJEMetricUtils.concatBdbKey(host, port, BDBJEMetricUtils.CPU_TOTAL, timestamp),
                    cpuTotalTime);
        } catch (Exception e) {
            LOG.warn("failed to parse be metric: {}:{}", host, port, e);
            return;
        }
    }

    private void parseFeMetricJsonAndWriteMetric(String host, int port, long timestamp, Reader metricsJson) {
        try {
            JsonArray jsonMetrics = JsonParser.parseReader(metricsJson).getAsJsonArray();
            for (JsonElement element : jsonMetrics) {
                JsonObject metric = element.getAsJsonObject();
                String metricName = metric.get(BDBJEMetricUtils.TAGS).getAsJsonObject().get(BDBJEMetricUtils.METRIC)
                        .getAsString();
                JsonElement value = metric.get(BDBJEMetricUtils.VALUE);
                switch (metricName) {
                    case BDBJEMetricUtils.QPS:
                        bdbjeMetricHandler.writeDouble(BDBJEMetricUtils.concatBdbKey(host, port, BDBJEMetricUtils.QPS,
                                timestamp), value.getAsDouble());
                        break;
                    case BDBJEMetricUtils.QUERY_ERR_RATE:
                        bdbjeMetricHandler.writeDouble(BDBJEMetricUtils.concatBdbKey(host, port,
                                BDBJEMetricUtils.QUERY_ERR_RATE, timestamp), value.getAsDouble());
                        break;
                    case BDBJEMetricUtils.CONNECTION_TOTAL:
                        bdbjeMetricHandler.writeLong(BDBJEMetricUtils.concatBdbKey(host, port,
                                BDBJEMetricUtils.CONNECTION_TOTAL, timestamp), value.getAsLong());
                        break;
                    case BDBJEMetricUtils.TXN_BEGIN:
                        if (isMaster(host)) {
                            bdbjeMetricHandler.writeLong(BDBJEMetricUtils.concatBdbKey(BDBJEMetricUtils.TXN_BEGIN,
                                    timestamp), value.getAsLong());
                        }
                        break;
                    case BDBJEMetricUtils.TXN_SUCCESS:
                        if (isMaster(host)) {
                            bdbjeMetricHandler.writeLong(BDBJEMetricUtils.concatBdbKey(BDBJEMetricUtils.TXN_SUCCESS,
                                    timestamp), value.getAsLong());
                        }
                        break;
                    case BDBJEMetricUtils.TXN_FAILED:
                        if (isMaster(host)) {
                            bdbjeMetricHandler.writeLong(BDBJEMetricUtils.concatBdbKey(BDBJEMetricUtils.TXN_FAILED,
                                    timestamp), value.getAsLong());
                        }
                        break;
                    case BDBJEMetricUtils.QUERY_LATENCY_MS:
                        String quantile = metric.get(BDBJEMetricUtils.TAGS).getAsJsonObject().get(BDBJEMetricUtils.QUANLITE)
                                .getAsString();
                        switch (quantile) {
                            case BDBJEMetricUtils.PERCENTILE_75TH:
                                bdbjeMetricHandler.writeDouble(BDBJEMetricUtils.concatBdbKey(host, port,
                                        BDBJEMetricUtils.QUANLITE.concat(BDBJEMetricUtils.PERCENTILE_75TH), timestamp),
                                        value.getAsDouble());
                                break;
                            case BDBJEMetricUtils.PERCENTILE_95TH:
                                bdbjeMetricHandler.writeDouble(BDBJEMetricUtils.concatBdbKey(host, port,
                                        BDBJEMetricUtils.QUANLITE.concat(BDBJEMetricUtils.PERCENTILE_95TH), timestamp),
                                        value.getAsDouble());
                                break;
                            case BDBJEMetricUtils.PERCENTILE_98TH:
                                bdbjeMetricHandler.writeDouble(BDBJEMetricUtils.concatBdbKey(host, port,
                                        BDBJEMetricUtils.QUANLITE.concat(BDBJEMetricUtils.PERCENTILE_98TH), timestamp),
                                        value.getAsDouble());
                                break;
                            case BDBJEMetricUtils.PERCENTILE_99TH:
                                bdbjeMetricHandler.writeDouble(BDBJEMetricUtils.concatBdbKey(host, port,
                                        BDBJEMetricUtils.QUANLITE.concat(BDBJEMetricUtils.PERCENTILE_99TH), timestamp),
                                        value.getAsDouble());
                                break;
                            case BDBJEMetricUtils.PERCENTILE_999TH:
                                bdbjeMetricHandler.writeDouble(BDBJEMetricUtils.concatBdbKey(host, port,
                                        BDBJEMetricUtils.QUANLITE.concat(BDBJEMetricUtils.PERCENTILE_999TH), timestamp),
                                        value.getAsDouble());
                                break;
                        }
                        break;
                    default:
                        break;
                }
            }
        } catch (Exception e) {
            LOG.warn("failed to parse fe metrics: {}:{}", host, port, e);
        }
    }

    private boolean isMaster(String host) {
        return Catalog.getCurrentCatalog().getMasterIp().equals(host);
    }

    public void close() {
        running = false;
    }
}