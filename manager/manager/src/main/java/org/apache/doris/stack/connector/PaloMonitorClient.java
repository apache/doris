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

package org.apache.doris.stack.connector;

import com.alibaba.fastjson.JSON;
import org.apache.doris.stack.model.palo.ClusterMonitorInfo;
import org.apache.doris.stack.model.palo.PaloResponseEntity;
import org.apache.doris.stack.exception.PaloRequestException;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Slf4j
public class PaloMonitorClient extends PaloClient {

    protected HttpClientPoolManager poolManager;

    @Autowired
    public PaloMonitorClient(HttpClientPoolManager poolManager) {
        this.poolManager = poolManager;
    }

    private static final String NODE_NUM = "/rest/v2/monitor/cluster_info/node_num";

    private static final String DISKS_CAPACITY = "/rest/v2/monitor/cluster_info/disks_capacity";

    private static final String FE_LIST = "/rest/v2/monitor/cluster_info/fe_list";

    private static final String BE_LIST = "/rest/v2/monitor/cluster_info/be_list";

    private static final String QPS = "/rest/v2/monitor/timeserial/qps";

    private static final String QUERY_LATENCY = "/rest/v2/monitor/timeserial/query_latency";

    private static final String QUERY_ERR_RATE = "/rest/v2/monitor/timeserial/query_err_rate";

    private static final String CONN_TOTAL = "/rest/v2/monitor/timeserial/conn_total";

    private static final String TXN_STATUS = "/rest/v2/monitor/timeserial/txn_status";

    private static final String BE_CPU_IDLE = "/rest/v2/monitor/timeserial/be_cpu_idle";

    private static final String BE_MEM = "/rest/v2/monitor/timeserial/be_mem";

    private static final String BE_DISK_IO = "/rest/v2/monitor/timeserial/be_disk_io";

    private static final String BE_BASE_COMPACTION_SCORE = "/rest/v2/monitor/timeserial/be_base_compaction_score";

    private static final String BE_CUMU_COMPACTION_SCORE = "/rest/v2/monitor/timeserial/be_cumu_compaction_score";

    public Object getQPS(ClusterMonitorInfo info) throws Exception {
        String url = getHostUrl(info.getHost(), info.getHttpPort());
        StringBuffer buffer = new StringBuffer();
        buffer.append(url);
        buffer.append(QPS);
        buffer.append("?");
        buffer.append("start=").append(info.getStart());
        buffer.append("&end=").append(info.getEnd());

        url = buffer.toString();
        log.debug("get QPS info request, url is {}.", url);

        Map<String, String> headers = Maps.newHashMap();
        setHeaders(headers);

        Map<String, Object> requestBody = Maps.newHashMap();
        requestBody.put("nodes", info.getNodes());
        if (null != info.getPointNum()) {
            requestBody.put("point_num", info.getPointNum());
        }

        PaloResponseEntity response = poolManager.doGet(url, headers, requestBody);
        if (response.getCode() != REQUEST_SUCCESS_CODE) {
            throw new PaloRequestException("Get QPS info exception:" + response.getData());
        }
        return JSON.parse(response.getData());
    }

    public Object getQueryLatency(ClusterMonitorInfo info) throws Exception {
        String url = getHostUrl(info.getHost(), info.getHttpPort());
        StringBuffer buffer = new StringBuffer();
        buffer.append(url);
        buffer.append(QUERY_LATENCY);
        buffer.append("?");
        buffer.append("start=").append(info.getStart());
        buffer.append("&end=").append(info.getEnd());

        url = buffer.toString();
        log.debug("get query latency info request, url is {}.", url);

        Map<String, String> headers = Maps.newHashMap();
        setHeaders(headers);

        Map<String, Object> requestBody = Maps.newHashMap();
        requestBody.put("nodes", info.getNodes());
        if (null != info.getPointNum()) {
            requestBody.put("point_num", info.getPointNum());
        }
        if (!StringUtils.isEmpty(info.getQuantile())) {
            requestBody.put("quantile", info.getQuantile());
        }

        PaloResponseEntity response = poolManager.doGet(url, headers, requestBody);
        if (response.getCode() != REQUEST_SUCCESS_CODE) {
            throw new PaloRequestException("Get query latency info exception:" + response.getData());
        }

        return JSON.parse(response.getData());
    }

    public Object getQueryErrRate(ClusterMonitorInfo info) throws Exception {
        String url = getHostUrl(info.getHost(), info.getHttpPort());
        StringBuffer buffer = new StringBuffer();
        buffer.append(url);
        buffer.append(QUERY_ERR_RATE);
        buffer.append("?");
        buffer.append("start=").append(info.getStart());
        buffer.append("&end=").append(info.getEnd());

        url = buffer.toString();
        log.debug("get query error rate info request, url is {}.", url);

        Map<String, String> headers = Maps.newHashMap();
        setHeaders(headers);

        Map<String, Object> requestBody = Maps.newHashMap();
        requestBody.put("nodes", info.getNodes());
        if (null != info.getPointNum()) {
            requestBody.put("point_num", info.getPointNum());
        }

        PaloResponseEntity response = poolManager.doGet(url, headers, requestBody);
        if (response.getCode() != REQUEST_SUCCESS_CODE) {
            throw new PaloRequestException("Get query error rate info exception:" + response.getData());
        }

        return JSON.parse(response.getData());
    }

    public Object getConnTotal(ClusterMonitorInfo info) throws Exception {
        String url = getHostUrl(info.getHost(), info.getHttpPort());
        StringBuffer buffer = new StringBuffer();
        buffer.append(url);
        buffer.append(CONN_TOTAL);
        buffer.append("?");
        buffer.append("start=").append(info.getStart());
        buffer.append("&end=").append(info.getEnd());

        url = buffer.toString();
        log.debug("get connection total rate info request, url is {}.", url);

        Map<String, String> headers = Maps.newHashMap();
        setHeaders(headers);

        Map<String, Object> requestBody = Maps.newHashMap();
        requestBody.put("nodes", info.getNodes());
        if (null != info.getPointNum()) {
            requestBody.put("point_num", info.getPointNum());
        }

        PaloResponseEntity response = poolManager.doGet(url, headers, requestBody);
        if (response.getCode() != REQUEST_SUCCESS_CODE) {
            throw new PaloRequestException("Get connection total info exception:" + response.getData());
        }

        return JSON.parse(response.getData());
    }

    public Object getTxnStatus(ClusterMonitorInfo info) throws Exception {
        String url = getHostUrl(info.getHost(), info.getHttpPort());
        StringBuffer buffer = new StringBuffer();
        buffer.append(url);
        buffer.append(TXN_STATUS);
        buffer.append("?");
        buffer.append("start=").append(info.getStart());
        buffer.append("&end=").append(info.getEnd());

        url = buffer.toString();
        log.debug("get transaction status info request, url is {}.", url);

        Map<String, String> headers = Maps.newHashMap();
        setHeaders(headers);

        Map<String, Object> requestBody = Maps.newHashMap();
        requestBody.put("nodes", info.getNodes());
        if (null != info.getPointNum()) {
            requestBody.put("point_num", info.getPointNum());
        }

        PaloResponseEntity response = poolManager.doGet(url, headers, requestBody);
        if (response.getCode() != REQUEST_SUCCESS_CODE) {
            throw new PaloRequestException("Get transaction status info exception:" + response.getData());
        }

        return JSON.parse(response.getData());
    }

    public Object getBeCpuIdle(ClusterMonitorInfo info) throws Exception {
        String url = getHostUrl(info.getHost(), info.getHttpPort());
        StringBuffer buffer = new StringBuffer();
        buffer.append(url);
        buffer.append(BE_CPU_IDLE);
        buffer.append("?");
        buffer.append("start=").append(info.getStart());
        buffer.append("&end=").append(info.getEnd());

        url = buffer.toString();
        log.debug("get be cpu idle info request, url is {}.", url);

        Map<String, String> headers = Maps.newHashMap();
        setHeaders(headers);

        Map<String, Object> requestBody = Maps.newHashMap();
        requestBody.put("nodes", info.getNodes());
        if (null != info.getPointNum()) {
            requestBody.put("point_num", info.getPointNum());
        }

        PaloResponseEntity response = poolManager.doGet(url, headers, requestBody);
        if (response.getCode() != REQUEST_SUCCESS_CODE) {
            throw new PaloRequestException("Get be cpu idle info exception:" + response.getData());
        }

        return JSON.parse(response.getData());
    }

    public Object getBeMem(ClusterMonitorInfo info) throws Exception {
        String url = getHostUrl(info.getHost(), info.getHttpPort());
        StringBuffer buffer = new StringBuffer();
        buffer.append(url);
        buffer.append(BE_MEM);
        buffer.append("?");
        buffer.append("start=").append(info.getStart());
        buffer.append("&end=").append(info.getEnd());

        url = buffer.toString();
        log.debug("get be memory info request, url is {}.", url);

        Map<String, String> headers = Maps.newHashMap();
        setHeaders(headers);

        Map<String, Object> requestBody = Maps.newHashMap();
        requestBody.put("nodes", info.getNodes());
        if (null != info.getPointNum()) {
            requestBody.put("point_num", info.getPointNum());
        }

        PaloResponseEntity response = poolManager.doGet(url, headers, requestBody);
        if (response.getCode() != REQUEST_SUCCESS_CODE) {
            throw new PaloRequestException("Get be memory info exception:" + response.getData());
        }
        return JSON.parse(response.getData());
    }

    public Object getBeDiskIO(ClusterMonitorInfo info) throws Exception {
        String url = getHostUrl(info.getHost(), info.getHttpPort());
        StringBuffer buffer = new StringBuffer();
        buffer.append(url);
        buffer.append(BE_DISK_IO);
        buffer.append("?");
        buffer.append("start=").append(info.getStart());
        buffer.append("&end=").append(info.getEnd());

        url = buffer.toString();
        log.debug("get be disk IO info request, url is {}.", url);

        Map<String, String> headers = Maps.newHashMap();
        setHeaders(headers);

        Map<String, Object> requestBody = Maps.newHashMap();
        requestBody.put("nodes", info.getNodes());
        if (null != info.getPointNum()) {
            requestBody.put("point_num", info.getPointNum());
        }

        PaloResponseEntity response = poolManager.doGet(url, headers, requestBody);
        if (response.getCode() != REQUEST_SUCCESS_CODE) {
            throw new PaloRequestException("Get be disk IO info exception:" + response.getData());
        }

        return JSON.parse(response.getData());
    }

    public Object getBeBaseCompactionScore(ClusterMonitorInfo info) throws Exception {
        String url = getHostUrl(info.getHost(), info.getHttpPort());
        StringBuffer buffer = new StringBuffer();
        buffer.append(url);
        buffer.append(BE_BASE_COMPACTION_SCORE);
        buffer.append("?");
        buffer.append("start=").append(info.getStart());
        buffer.append("&end=").append(info.getEnd());

        url = buffer.toString();
        log.debug("get be base compaction score info request, url is {}.", url);

        Map<String, String> headers = Maps.newHashMap();
        setHeaders(headers);

        Map<String, Object> requestBody = Maps.newHashMap();
        requestBody.put("nodes", info.getNodes());
        if (null != info.getPointNum()) {
            requestBody.put("point_num", info.getPointNum());
        }

        PaloResponseEntity response = poolManager.doGet(url, headers, requestBody);
        if (response.getCode() != REQUEST_SUCCESS_CODE) {
            throw new PaloRequestException("Get be base compaction score IO info exception:" + response.getData());
        }

        return JSON.parse(response.getData());
    }

    public Object getBeCumuCompactionScore(ClusterMonitorInfo info) throws Exception {
        String url = getHostUrl(info.getHost(), info.getHttpPort());
        StringBuffer buffer = new StringBuffer();
        buffer.append(url);
        buffer.append(BE_CUMU_COMPACTION_SCORE);
        buffer.append("?");
        buffer.append("start=").append(info.getStart());
        buffer.append("&end=").append(info.getEnd());

        url = buffer.toString();
        log.debug("get be cumu compaction score info request, url is {}.", url);

        Map<String, String> headers = Maps.newHashMap();
        setHeaders(headers);

        Map<String, Object> requestBody = Maps.newHashMap();
        requestBody.put("nodes", info.getNodes());
        if (null != info.getPointNum()) {
            requestBody.put("point_num", info.getPointNum());
        }

        PaloResponseEntity response = poolManager.doGet(url, headers, requestBody);
        if (response.getCode() != REQUEST_SUCCESS_CODE) {
            throw new PaloRequestException("Get be cumu compaction score IO info exception:" + response.getData());
        }

        return JSON.parse(response.getData());
    }

    public Object getNodeNum(ClusterMonitorInfo info) throws Exception {
        String url = getHostUrl(info.getHost(), info.getHttpPort());
        StringBuffer buffer = new StringBuffer();
        buffer.append(url);
        buffer.append(NODE_NUM);

        url = buffer.toString();
        log.debug("get node num info request, url is {}.", url);

        Map<String, String> headers = Maps.newHashMap();
        setHeaders(headers);

        PaloResponseEntity response = poolManager.doGet(url, headers);
        if (response.getCode() != REQUEST_SUCCESS_CODE) {
            throw new PaloRequestException("Get node num info exception:" + response.getData());
        }

        return JSON.parse(response.getData());
    }

    public Object getDisksCapacity(ClusterMonitorInfo info) throws Exception {
        String url = getHostUrl(info.getHost(), info.getHttpPort());
        StringBuffer buffer = new StringBuffer();
        buffer.append(url);
        buffer.append(DISKS_CAPACITY);

        url = buffer.toString();
        log.debug("get disk capacity info request, url is {}.", url);

        Map<String, String> headers = Maps.newHashMap();
        setHeaders(headers);

        PaloResponseEntity response = poolManager.doGet(url, headers);
        if (response.getCode() != REQUEST_SUCCESS_CODE) {
            throw new PaloRequestException("Get disk capacity info exception:" + response.getData());
        }

        return JSON.parse(response.getData());
    }

    public Object getFeList(ClusterMonitorInfo info) throws Exception {
        String url = getHostUrl(info.getHost(), info.getHttpPort());
        StringBuffer buffer = new StringBuffer();
        buffer.append(url);
        buffer.append(FE_LIST);

        url = buffer.toString();
        log.debug("get fe list info request, url is {}.", url);

        Map<String, String> headers = Maps.newHashMap();
        setHeaders(headers);

        PaloResponseEntity response = poolManager.doGet(url, headers);
        if (response.getCode() != REQUEST_SUCCESS_CODE) {
            throw new PaloRequestException("Get fe list info exception:" + response.getData());
        }

        return JSON.parse(response.getData());
    }

    public Object getBeList(ClusterMonitorInfo info) throws Exception {
        String url = getHostUrl(info.getHost(), info.getHttpPort());
        StringBuffer buffer = new StringBuffer();
        buffer.append(url);
        buffer.append(BE_LIST);

        url = buffer.toString();
        log.debug("get be list info request, url is {}.", url);

        Map<String, String> headers = Maps.newHashMap();
        setHeaders(headers);

        PaloResponseEntity response = poolManager.doGet(url, headers);
        if (response.getCode() != REQUEST_SUCCESS_CODE) {
            throw new PaloRequestException("Get be list info exception:" + response.getData());
        }

        return JSON.parse(response.getData());
    }
}
