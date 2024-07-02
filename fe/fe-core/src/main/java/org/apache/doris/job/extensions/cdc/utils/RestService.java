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

package org.apache.doris.job.extensions.cdc.utils;

import org.apache.doris.common.Pair;
import org.apache.doris.httpv2.rest.manager.HttpUtils;
import org.apache.doris.job.extensions.cdc.CdcDatabaseJob;
import org.apache.doris.job.extensions.cdc.state.AbstractSourceSplit;
import org.apache.doris.job.extensions.cdc.state.BinlogSplit;
import org.apache.doris.job.extensions.cdc.state.SnapshotSplit;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RestService {
    private static final Logger LOG = LogManager.getLogger(RestService.class);
    private static final String SHUTDOWN = "/api/shutdown";
    private static final String FETCH_RECORDS = "/api/fetchRecords";
    private static final String FETCH_SPLITS = "/api/fetchSplits";
    private static final String CLOSE_RESOURCE = "/api/close/";
    private static Map<String, String> empty = new HashMap<>();

    private static ObjectMapper objectMapper = new ObjectMapper();

    //    public static boolean isStarted(Pair<String, Integer> ipPort, Long jobId) throws IOException {
    //        Map<String, String> params = new HashMap<>();
    //        params.put("jobId", jobId.toString());
    //        String restUrl = HttpUtils.concatUrl(ipPort, CHECK_CDC_STATUS, params);
    //        String response = HttpUtils.doGet(restUrl, empty);
    //        String data = HttpUtils.parseResponse(response);
    //        return "true".equals(data);
    //    }
    //
    //    public static boolean sendHeartbeat(Pair<String, Integer> ipPort, Long jobId){
    //        Map<String, String> params = new HashMap<>();
    //        params.put("jobId", jobId.toString());
    //        String requestUrl = HttpUtils.concatUrl(ipPort, HEARTBEAT_URL, params);
    //        try {
    //            String response = HttpUtils.doPost(requestUrl, empty, null);
    //            ResponseBody responseEntity = GsonUtils.GSON.fromJson(response, new TypeToken<ResponseBody>() {}.getType());
    //            return responseEntity.getCode() == 0;
    //        } catch (Exception ex) {
    //            LOG.warn("send heartbeat request error: {}", ex.getMessage());
    //            return false;
    //        }
    //    }

    //    public static void stopCdcProcess(Pair<String, Integer> ipPort, Long jobId) {
    //        Map<String, String> params = new HashMap<>();
    //        params.put("jobId", jobId.toString());
    //        String requestUrl = HttpUtils.concatUrl(ipPort, SHUTDOWN, params);
    //        try {
    //            String response = HttpUtils.doPost(requestUrl, empty, null);
    //            ResponseBody responseEntity = GsonUtils.GSON.fromJson(response, new TypeToken<ResponseBody>() {}.getType());
    //            if(responseEntity.getCode() == 0){
    //                LOG.info("shutdown cdc process success");
    //            }else {
    //                LOG.info("shutdown cdc process error, code {} , msg {}", responseEntity.getCode(), responseEntity.getMsg());
    //            }
    //        } catch (Exception ex) {
    //            LOG.warn("shutdown cdc process error: {}", ex.getMessage());
    //        }
    //    }

    public static List<? extends AbstractSourceSplit> getSplits(Pair<String, Integer> ipPort, Long jobId,
            Map<String, String> config) {
        List<? extends AbstractSourceSplit> responseList;
        Map<String, Object> params = new HashMap<>();
        params.put("jobId", jobId);
        params.put("config", config);
        String requestUrl = HttpUtils.concatUrl(ipPort, FETCH_SPLITS, empty);
        try {
            Map<String, String> header = new HashMap<>();
            header.put("content-type", "application/json");
            String response = HttpUtils.parseResponse(HttpUtils.doPost(requestUrl, header, params));
            List<Map<String, Object>> mapList = new ObjectMapper().readValue(response,
                    new TypeReference<List<Map<String, Object>>>() {
                    });
            Map<String, Object> split = mapList.get(0);
            String splitId = split.get(CdcDatabaseJob.SPLIT_ID).toString();
            if (CdcDatabaseJob.BINLOG_SPLIT_ID.equals(splitId)) {
                responseList = objectMapper.convertValue(mapList, new TypeReference<List<BinlogSplit>>() {
                });
            } else {
                responseList = objectMapper.convertValue(mapList, new TypeReference<List<SnapshotSplit>>() {
                });
            }
            return responseList;
        } catch (IOException e) {
            LOG.error("Get states error: ", e);
            throw new RuntimeException("Get states error");
        }
    }

    public static String fetchRecords(Pair<String, Integer> ipPort, Long jobId, Map<String, String> meta,
            Map<String, String> config) {
        try {
            Map<String, Object> params = new HashMap<>();
            params.put("jobId", jobId);
            params.put("fetchSize", 100);
            params.put("meta", meta);
            params.put("schedule", true);
            params.put("config", config);
            String requestUrl = HttpUtils.concatUrl(ipPort, FETCH_RECORDS, empty);
            Map<String, String> header = new HashMap<>();
            header.put("content-type", "application/json");
            String s = HttpUtils.doPost(requestUrl, header, params);
            String response = HttpUtils.parseResponse(s);
            return response;
        } catch (Exception ex) {
            LOG.error("Failed to get record", ex);
            throw new RuntimeException("Failed to get record");
        }
    }

    public static void closeResource(Pair<String, Integer> ipPort, Long jobId) {
        try {
            String requestUrl = HttpUtils.concatUrl(ipPort, CLOSE_RESOURCE + jobId, empty);
            Map<String, String> header = new HashMap<>();
            String response = HttpUtils.doPost(requestUrl, header, null);
            System.out.println(response);
        } catch (Exception ex) {
            LOG.error("Failed to close resource", ex);
        }
    }
}
