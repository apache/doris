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

package org.apache.doris.stack.controller.monitor;

import org.apache.doris.stack.model.request.monitor.ClusterMonitorReq;
import org.apache.doris.stack.controller.BaseController;
import org.apache.doris.stack.rest.ResponseEntityBuilder;
import org.apache.doris.stack.service.monitor.PaloMonitorService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Api(tags = "Doris Cluster monitoring routing interface")
@RestController
@RequestMapping(value = "api/rest/v2/monitor/")
@Slf4j
public class PaloMonitorController extends BaseController {

    @Autowired
    private PaloMonitorService monitorService;

    @ApiOperation(value = "be/fe Number of nodes")
    @GetMapping(value = "cluster_info/node_num")
    public Object nodeNum(ClusterMonitorReq info, HttpServletRequest request, HttpServletResponse response) throws Exception {
        return ResponseEntityBuilder.ok(monitorService.nodeNum(info, request, response));
    }

    @ApiOperation(value = "Disk space usage")
    @GetMapping(value = "cluster_info/disks_capacity")
    public Object diskCapacity(ClusterMonitorReq info, HttpServletRequest request, HttpServletResponse response) throws Exception {
        return ResponseEntityBuilder.ok(monitorService.disksCapacity(info, request, response));
    }

    @ApiOperation(value = "fe node information")
    @GetMapping(value = "cluster_info/fe_list")
    public Object feList(ClusterMonitorReq info, HttpServletRequest request, HttpServletResponse response) throws Exception {
        return ResponseEntityBuilder.ok(monitorService.feList(info, request, response));
    }

    @ApiOperation(value = "be node information")
    @GetMapping(value = "cluster_info/be_list")
    public Object beList(ClusterMonitorReq info, HttpServletRequest request, HttpServletResponse response) throws Exception {
        return ResponseEntityBuilder.ok(monitorService.beList(info, request, response));
    }

    @ApiOperation(value = "qps information")
    @GetMapping(value = "timeserial/qps")
    public Object qps(ClusterMonitorReq info, HttpServletRequest request, HttpServletResponse response) throws Exception {
        return ResponseEntityBuilder.ok(monitorService.qps(info, request, response));
    }

    @ApiOperation(value = "Percentile delay information")
    @GetMapping(value = "timeserial/query_latency")
    public Object queryLatency(ClusterMonitorReq info, HttpServletRequest request, HttpServletResponse response) throws Exception {
        return ResponseEntityBuilder.ok(monitorService.queryLatency(info, request, response));
    }

    @ApiOperation(value = "Query error rate information")
    @GetMapping(value = "timeserial/query_err_rate")
    public Object queryErrRate(ClusterMonitorReq info, HttpServletRequest request, HttpServletResponse response) throws Exception {
        return ResponseEntityBuilder.ok(monitorService.queryErrRate(info, request, response));
    }

    @ApiOperation(value = "Number of connections information")
    @GetMapping(value = "timeserial/conn_total")
    public Object connTotal(ClusterMonitorReq info, HttpServletRequest request, HttpServletResponse response) throws Exception {
        return ResponseEntityBuilder.ok(monitorService.connTotal(info, request, response));
    }

    @ApiOperation(value = "Import error submission success / failure frequency")
    @GetMapping(value = "timeserial/txn_status")
    public Object txnStatus(ClusterMonitorReq info, HttpServletRequest request, HttpServletResponse response) throws Exception {
        return ResponseEntityBuilder.ok(monitorService.txnStatus(info, request, response));
    }

    @ApiOperation(value = "be node CPU idle rate")
    @GetMapping(value = "timeserial/be_cpu_idle")
    public Object beCpuIdle(ClusterMonitorReq info, HttpServletRequest request, HttpServletResponse response) throws Exception {
        return ResponseEntityBuilder.ok(monitorService.beCpuIdle(info, request, response));
    }

    @ApiOperation(value = "be node memory")
    @GetMapping(value = "timeserial/be_mem")
    public Object beMem(ClusterMonitorReq info, HttpServletRequest request, HttpServletResponse response) throws Exception {
        return ResponseEntityBuilder.ok(monitorService.beMem(info, request, response));
    }

    @ApiOperation(value = "be node IO")
    @GetMapping(value = "timeserial/be_disk_io")
    public Object beDiskIO(ClusterMonitorReq info, HttpServletRequest request, HttpServletResponse response) throws Exception {
        return ResponseEntityBuilder.ok(monitorService.beDiskIO(info, request, response));
    }

    @ApiOperation(value = "be base compaction score")
    @GetMapping(value = "timeserial/be_base_compaction_score")
    public Object beBaseCompactionScore(ClusterMonitorReq info, HttpServletRequest request, HttpServletResponse response) throws Exception {
        return ResponseEntityBuilder.ok(monitorService.beBaseCompactionScore(info, request, response));
    }

    @ApiOperation(value = "be cumu compaction score")
    @GetMapping(value = "timeserial/be_cumu_compaction_score")
    public Object beCumuCompactionScore(ClusterMonitorReq info, HttpServletRequest request, HttpServletResponse response) throws Exception {
        return ResponseEntityBuilder.ok(monitorService.beCumuCompactionScore(info, request, response));
    }
}
