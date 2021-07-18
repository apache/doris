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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.Getter;
import lombok.Setter;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.system.SystemInfoService;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

/*
 * This class provides monitoring data for time serial chart.
 * Json data format:
 *  {
 *     	"x_value": [
 *     		timestamp,
 *     		timestamp
 *     	],
 *     	"y_value": {
 *     		"host:http_port": [
 *     			value,
 *     			value
 *     		]
 *      }
 *  }
 */
public class Monitor {
    private static final int DEFAULT_MONITOR_POINTS = 100;
    static final String NODES = "nodes";
    static final String POINT_NUM = "point_num";

    public enum MonitorType {
        QPS,
        QUERY_LATENCY,
        QUERY_ERR_RATE,
        CONN_TOTAL,
        TXN_STATUS,
        BE_CPU_IDLE,
        BE_MEM,
        BE_DISK_IO,
        BE_BASE_COMPACTION_SCORE,
        BE_CUMU_COMPACTION_SCORE
    }

    // read metric from bdbje and calculate monitor.
    public static Object monitoring(long startTime, long endTime, String parameterJson, MonitorType monitorType)
            throws DdlException {
        if (!Config.enable_monitor) {
            throw new DdlException("no enable monitor server.");
        }
        JsonObject parameterJsonObject = JsonParser.parseString(parameterJson).getAsJsonObject();

        int pointNum = DEFAULT_MONITOR_POINTS;
        try {
            pointNum = parameterJsonObject.get(POINT_NUM).getAsInt();
        } catch (Exception ignored) {
            // POINT_NUM is an optional parameter. If the user does not set it, an exception will be thrown.
        }

        // The timestamp for writing metric is a multiple of the WRITE_INTERVAL_MS,
        // so read timestamps and readInterval must be multiples of WRITE_INTERVAL_MS.
        int readInterval = computeReadInterval(endTime - startTime, pointNum);
        long endReadTime = endTime - endTime % BDBJEMetricUtils.WRITE_INTERVAL_MS;
        long startReadTime = computeStartReadTime(endReadTime - startTime, endReadTime,
                readInterval);
        List<Long> timestampsOfReadingMetric = Lists.newArrayList();
        for (long time = startReadTime; time <= endReadTime; time += readInterval) {
            timestampsOfReadingMetric.add(time);
        }

        List<String> nodes;
        try {
            nodes = GsonUtils.GSON.fromJson(parameterJsonObject.get(NODES).getAsJsonArray(), List.class);
        } catch (Exception e) {
            nodes = Lists.newArrayList();
        }

        switch (monitorType) {
            case QPS:
                return qps(nodes, timestampsOfReadingMetric);
            case QUERY_LATENCY:
                String quanlite;
                try {
                    quanlite = parameterJsonObject.get(BDBJEMetricUtils.QUANLITE).getAsString();
                } catch (Exception e) {
                    throw new DdlException("request query_latency is missing parameter quanlite.");
                }
                return queryLatency(nodes, timestampsOfReadingMetric, quanlite);
            case QUERY_ERR_RATE:
                return queryErrRate(nodes, timestampsOfReadingMetric);
            case CONN_TOTAL:
                return connectionTotal(nodes, timestampsOfReadingMetric);
            case TXN_STATUS:
                return transactionStatus(timestampsOfReadingMetric, readInterval);
            case BE_CPU_IDLE:
                return beCpuIdle(startReadTime - readInterval, nodes, timestampsOfReadingMetric);
            case BE_MEM:
                return beMem(nodes, timestampsOfReadingMetric);
            case BE_DISK_IO:
                return diskIoUtil(nodes, timestampsOfReadingMetric);
            case BE_BASE_COMPACTION_SCORE:
                return beBaseCompactionScore(nodes, timestampsOfReadingMetric);
            case BE_CUMU_COMPACTION_SCORE:
                return beCumuCompactionScore(nodes, timestampsOfReadingMetric);
            default:
                return "";
        }
    }

    // return readInterval, and readInterval is a multiple of WRITE_INTERVAL_MS.
    private static int computeReadInterval(long totalTime, int points) {
        long multiple = totalTime / points / BDBJEMetricUtils.WRITE_INTERVAL_MS;
        return (int) (multiple <= 0 ? 1 : multiple) * BDBJEMetricUtils.WRITE_INTERVAL_MS;
    }

    // StartReadTime should be greater than startTime,
    // and (StartReadTime - endReadTime) is a multiple of readInterval.
    private static long computeStartReadTime(long totalTime, long endReadTime,
                                             long readInterval) {
        return endReadTime - (totalTime - (totalTime
                + readInterval /*This is to make the remainder positive, because totalTime may be negative*/)
                % readInterval);
    }

    private static ChartData<Double> qps(List<String> nodes, List<Long> timestampsOfReadingMetric) {
        emptyAddAllFe(nodes);
        return readDataDouble(BDBJEMetricUtils.QPS, nodes, timestampsOfReadingMetric);
    }

    private static ChartData<Double> queryLatency(List<String> nodes, List<Long> timestampsOfReadingMetric, String quanlite) {
        emptyAddAllFe(nodes);
        return readDataDouble(BDBJEMetricUtils.QUANLITE.concat(quanlite), nodes, timestampsOfReadingMetric);
    }

    private static ChartData<Double> queryErrRate(List<String> nodes, List<Long> timestampsOfReadingMetric) {
        emptyAddAllFe(nodes);
        return readDataDouble(BDBJEMetricUtils.QUERY_ERR_RATE, nodes, timestampsOfReadingMetric);
    }

    private static ChartData<Long> connectionTotal(List<String> nodes, List<Long> timestampsOfReadingMetric) {
        emptyAddAllFe(nodes);
        return readDataLong(BDBJEMetricUtils.CONNECTION_TOTAL, nodes, timestampsOfReadingMetric);
    }

    /*
     * json data format：
     *
     * {
     *     	"x_value": [
     *     		timestamp,
     *     		timestamp
     *     	],
     *     	"y_value": {
     *     		"fe_host1:http_port": {
     *     			"begin" [
     *     				value,
     *     				value
     *     			]
     *     			"success": [
     *     				value,
     *     				value
     *     			],
     *     			"failed": [
     *     				value,
     *     				value
     *     			]
     *         }
     *     }
     * }
     *
     */
    private static ChartDataTxn<Double> transactionStatus(List<Long> timestampsOfReadingMetric, int readInterval) {
        Map<String, Map<String, List<Double>>> nodeToData = Maps.newHashMap();
        // Only master of fe has this monitoring data.
        InetSocketAddress master = Catalog.getCurrentCatalog().getHaProtocol().getLeader();
        Pair<String, Integer> ipPort = new Pair<>(master.getAddress().getHostAddress(), Config.http_port);
        Map<String, List<Double>> txnData = Maps.newHashMap();
        txnData.put(BDBJEMetricUtils.BEGIN, getTransactionStatusData(BDBJEMetricUtils.TXN_BEGIN, readInterval,
                timestampsOfReadingMetric));
        txnData.put(BDBJEMetricUtils.SUCCESS, getTransactionStatusData(BDBJEMetricUtils.TXN_SUCCESS, readInterval,
                timestampsOfReadingMetric));
        txnData.put(BDBJEMetricUtils.FAILED, getTransactionStatusData(BDBJEMetricUtils.TXN_FAILED, readInterval,
                timestampsOfReadingMetric));
        nodeToData.put(NetUtils.getHostnameByIp(ipPort.first) + ":" + ipPort.second, txnData);
        return new ChartDataTxn<>(timestampsOfReadingMetric, nodeToData);
    }

    private static List<Double> getTransactionStatusData(String metricName, int readInterval,
                                                         List<Long> timestampsOfReadingMetric) {
        List<Double> values = Lists.newArrayList();
        if (timestampsOfReadingMetric.size() == 0) {
            return values;
        }
        BDBJEMetricHandler bdbjeMetricHandler = Catalog.getCurrentCatalog().getBDBJEMetricHandler();
        Long firstValue = bdbjeMetricHandler.readLong(BDBJEMetricUtils.concatBdbKey(
                metricName, timestampsOfReadingMetric.get(0) - readInterval));
        for (long time : timestampsOfReadingMetric) {
            Long secondValue = bdbjeMetricHandler.readLong(BDBJEMetricUtils.concatBdbKey(
                    metricName, time));
            values.add(calculateFrequency(firstValue, secondValue, readInterval));
            firstValue = secondValue;
        }
        return values;
    }

    // times per second.
    private static Double calculateFrequency(Long firstValue, Long secondValue, int readInterval) {
        if (firstValue == null || secondValue == null) {
            return null;
        }
        return (secondValue - firstValue) * 1000.0 / readInterval;
    }

    // cpuIdle is the total cpu idle time since boot, and cpuTotal is the total cpu time since boot.
    // Average cpuIdlePercent in readInterval = (cpuIdle2 - cpuIdle1) / (cpuTotal2 - cpuIdle1) * 100%
    // The average cpuIdlePercent is used to represent the cpuIdlePercent.
    private static ChartData<Long> beCpuIdle(long lastTimestamp, List<String> nodes, List<Long> timestampsOfReadingMetric) {
        Map<String, List<Long>> nodeToData = Maps.newHashMap();
        emptyAddAllBe(nodes);
        BDBJEMetricHandler bdbjeMetricHandler = Catalog.getCurrentCatalog().getBDBJEMetricHandler();
        for (String node : nodes) {
            Pair<String, Integer> ipPort;
            try {
                ipPort = SystemInfoService.validateHostAndPort(node);
            } catch (Exception ignored) {
                // This rarely happens. If this exception occurs, discard the node.
                continue;
            }
            List<Long> values = Lists.newArrayList();
            Long firstCpuIdle = bdbjeMetricHandler.readLong(BDBJEMetricUtils.concatBdbKey(ipPort.first, ipPort.second,
                    BDBJEMetricUtils.CPU_IDLE, lastTimestamp));
            Long firstCpuTotal = bdbjeMetricHandler.readLong(BDBJEMetricUtils.concatBdbKey(ipPort.first, ipPort.second,
                    BDBJEMetricUtils.CPU_TOTAL, lastTimestamp));
            for (long time : timestampsOfReadingMetric) {
                Long secondCpuIdle = bdbjeMetricHandler.readLong(BDBJEMetricUtils.concatBdbKey(ipPort.first,
                        ipPort.second, BDBJEMetricUtils.CPU_IDLE, time));
                Long secondCpuTotal = bdbjeMetricHandler.readLong(BDBJEMetricUtils.concatBdbKey(ipPort.first,
                        ipPort.second, BDBJEMetricUtils.CPU_TOTAL, time));
                values.add(calculateCpuIdlePercent(firstCpuIdle, firstCpuTotal, secondCpuIdle, secondCpuTotal));
                firstCpuIdle = secondCpuIdle;
                firstCpuTotal = secondCpuTotal;
            }
            nodeToData.put(node, values);
        }
        return new ChartData<>(timestampsOfReadingMetric, nodeToData);
    }

    private static Long calculateCpuIdlePercent(Long firstCpuIdle, Long firstCpuTotal, Long secondCpuIdle,
                                                Long secondCpuTotal) {
        if (firstCpuIdle == null || firstCpuTotal == null || secondCpuIdle == null || secondCpuTotal == null
                || firstCpuTotal.equals(secondCpuTotal)) {
            return null;
        }
        return (secondCpuIdle - firstCpuIdle) * 100 / (secondCpuTotal - firstCpuTotal);
    }

    private static ChartData<Long> beMem(List<String> nodes, List<Long> timestampsOfReadingMetric) {
        emptyAddAllBe(nodes);
        return readDataLong(BDBJEMetricUtils.METRIC_MEMORY_ALLOCATED_BYTES, nodes, timestampsOfReadingMetric);
    }

    private static ChartData<Long> diskIoUtil(List<String> nodes, List<Long> timestampsOfReadingMetric) {
        emptyAddAllBe(nodes);
        return readDataLong(BDBJEMetricUtils.METRIC_MAX_DISK_IO_UTIL_PERCENT, nodes, timestampsOfReadingMetric);
    }

    private static ChartData<Long> beBaseCompactionScore(List<String> nodes, List<Long> timestampsOfReadingMetric) {
        emptyAddAllBe(nodes);
        return readDataLong(BDBJEMetricUtils.BASE_COMPACTION_SCORE, nodes, timestampsOfReadingMetric);
    }

    private static ChartData<Long> beCumuCompactionScore(List<String> nodes, List<Long> timestampsOfReadingMetric) {
        emptyAddAllBe(nodes);
        return readDataLong(BDBJEMetricUtils.CUMU_COMPACTION_SCORE, nodes, timestampsOfReadingMetric);
    }

    private static void emptyAddAllFe(List<String> nodes) {
        if (nodes.isEmpty()) {
            nodes.addAll(ClusterInfo.feList());
        }
    }

    private static void emptyAddAllBe(List<String> nodes) {
        if (nodes.isEmpty()) {
            nodes.addAll(ClusterInfo.beList());
        }
    }

    private static ChartData<Long> readDataLong(String metricName, List<String> nodes,
                                                List<Long> timestampsOfReadingMetric) {
        Map<String, List<Long>> nodeToData = Maps.newHashMap();
        BDBJEMetricHandler bdbjeMetricHandler = Catalog.getCurrentCatalog().getBDBJEMetricHandler();
        for (String node : nodes) {
            Pair<String, Integer> ipPort;
            try {
                ipPort = SystemInfoService.validateHostAndPort(node);
            } catch (Exception ignored) {
                // This rarely happens. If this exception occurs, discard the node.
                continue;
            }
            List<Long> values = Lists.newArrayList();
            for (long time : timestampsOfReadingMetric) {
                values.add(bdbjeMetricHandler.readLong(BDBJEMetricUtils.concatBdbKey(ipPort.first, ipPort.second,
                        metricName, time)));
            }
            nodeToData.put(node, values);
        }
        return new ChartData<>(timestampsOfReadingMetric, nodeToData);
    }

    private static ChartData<Double> readDataDouble(String metricName,
                                                    List<String> nodes, List<Long> timestampsOfReadingMetric) {
        Map<String, List<Double>> nodeToData = Maps.newHashMap();
        BDBJEMetricHandler bdbjeMetricHandler = Catalog.getCurrentCatalog().getBDBJEMetricHandler();
        for (String node : nodes) {
            Pair<String, Integer> ipPort;
            try {
                ipPort = SystemInfoService.validateHostAndPort(node);
            } catch (Exception ignored) {
                // This rarely happens. If this exception occurs, discard the node.
                continue;
            }
            List<Double> values = Lists.newArrayList();
            for (long time : timestampsOfReadingMetric) {
                values.add(bdbjeMetricHandler.readDouble(BDBJEMetricUtils.concatBdbKey(ipPort.first, ipPort.second,
                        metricName, time)));
            }
            nodeToData.put(node, values);
        }
        return new ChartData<>(timestampsOfReadingMetric, nodeToData);
    }

    @Getter
    @Setter
    public static class ChartData<T> {
        public List<Long> x_value;
        public Map<String, List<T>> y_value;

        public ChartData(List<Long> x_value, Map<String, List<T>> y_value) {
            this.x_value = x_value;
            this.y_value = y_value;
        }
    }

    @Getter
    @Setter
    public static class ChartDataTxn<T> {
        public List<Long> x_value;
        public Map<String, Map<String, List<T>>> y_value;

        public ChartDataTxn(List<Long> x_value, Map<String, Map<String, List<T>>> y_value) {
            this.x_value = x_value;
            this.y_value = y_value;
        }
    }
}
