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

package org.apache.doris.httpv2.rest;

import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.metric.collector.ClusterInfo;
import org.apache.doris.metric.collector.Monitor;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * This class is used to get Doris monitoring data by http get method.
 */
@RestController
@RequestMapping("/rest/v2")
public class MonitorAction {
    private static final String START_TIMESTAMP = "start";
    private static final String END_TIMESTAMP = "end";

    /**
     * url: http://fe_host:http_port/rest/v2/monitor/timeserial/{@link Monitor.MonitorType}?start=startTimestamp&end
     * =endTimestamp,
     * and the get method must has a body containing a json map of nodes, like:
     * {"nodes":["host1:http_port", "host2:http_port"]}.
     */
    @RequestMapping(path = "/monitor/timeserial/{type}", method = RequestMethod.GET)
    public Object timeSerial(@PathVariable("type") String type,
                             @RequestParam(value = START_TIMESTAMP) long start,
                             @RequestParam(value = END_TIMESTAMP) long end,
                             @RequestBody String nodesJson) {
        Object data;
        try {
            Monitor.MonitorType monitorType = Monitor.MonitorType.valueOf(type.toUpperCase());
            data = Monitor.monitoring(start, end, nodesJson, monitorType);
        } catch (IllegalArgumentException e) {
            return ResponseEntityBuilder.notFound("No such monitor type: " + type);
        } catch (Exception e) {
            return ResponseEntityBuilder.badRequest(e.getMessage());
        }
        return ResponseEntityBuilder.ok(data);
    }

    /**
     * url: http://fe_host:http_port/rest/v2/monitor/cluster_info/{@link ClusterInfo.ClusterInfoType}
     */
    @RequestMapping(path = "/monitor/cluster_info/{type}", method = RequestMethod.GET)
    public Object clusterInfo(@PathVariable("type") String type) {
        try {
            ClusterInfo.ClusterInfoType clusterInfoType = ClusterInfo.ClusterInfoType.valueOf(type.toUpperCase());
            return ResponseEntityBuilder.ok(ClusterInfo.getClusterInfo(clusterInfoType));
        } catch (IllegalArgumentException e) {
            return ResponseEntityBuilder.notFound("No such monitor type: " + type);
        }
    }
}