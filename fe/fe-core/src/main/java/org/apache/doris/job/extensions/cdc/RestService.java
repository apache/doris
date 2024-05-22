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

package org.apache.doris.job.extensions.cdc;

import com.google.gson.reflect.TypeToken;
import org.apache.doris.common.Pair;
import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.httpv2.rest.manager.HttpUtils;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RestService {
    private static final Logger LOG = LogManager.getLogger(RestService.class);
    private static final String CHECK_CDC_STATUS = "/api/status";
    private static final String HEARTBEAT_URL = "/api/job/cdc/heartbeat";
    private static final String SHUTDOWN = "/api/shutdown";
    private static Map<String, String> empty = new HashMap<>();

    public static boolean isStarted(Pair<String, Integer> ipPort, Long jobId) throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("jobId", jobId.toString());
        String restUrl = HttpUtils.concatUrl(ipPort, CHECK_CDC_STATUS, params);
        String response = HttpUtils.doGet(restUrl, empty);
        String data = HttpUtils.parseResponse(response);
        return "true".equals(data);
    }

    public static void getSnapshotSplits(Pair<String, Integer> ipPort, Long jobId) {
        System.out.println("save snapshot splits to fe===");
    }

    public static boolean sendHeartbeat(Pair<String, Integer> ipPort, Long jobId){
        Map<String, String> params = new HashMap<>();
        params.put("jobId", jobId.toString());
        String requestUrl = HttpUtils.concatUrl(ipPort, HEARTBEAT_URL, params);
        try {
            String response = HttpUtils.doPost(requestUrl, empty, null);
            ResponseBody responseEntity = GsonUtils.GSON.fromJson(response, new TypeToken<ResponseBody>() {}.getType());
            return responseEntity.getCode() == 0;
        } catch (Exception ex) {
            LOG.warn("send heartbeat request error: {}", ex.getMessage());
            return false;
        }
    }

    public static void stopCdcProcess(Pair<String, Integer> ipPort, Long jobId) {
        Map<String, String> params = new HashMap<>();
        params.put("jobId", jobId.toString());
        String requestUrl = HttpUtils.concatUrl(ipPort, SHUTDOWN, params);
        try {
            String response = HttpUtils.doPost(requestUrl, empty, null);
            ResponseBody responseEntity = GsonUtils.GSON.fromJson(response, new TypeToken<ResponseBody>() {}.getType());
            if(responseEntity.getCode() == 0){
                LOG.info("shutdown cdc process success");
            }else {
                LOG.info("shutdown cdc process error, code {} , msg {}", responseEntity.getCode(), responseEntity.getMsg());
            }
        } catch (Exception ex) {
            LOG.warn("shutdown cdc process error: {}", ex.getMessage());
        }
    }
}
