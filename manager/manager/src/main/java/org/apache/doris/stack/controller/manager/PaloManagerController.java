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

package org.apache.doris.stack.controller.manager;

import org.apache.doris.stack.controller.BaseController;
import org.apache.doris.stack.service.manager.PaloManagerService;
import com.google.common.base.Strings;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Api(tags = "Doris Cluster routing interface")
@RestController
@RequestMapping(value = "/api")
@Slf4j
public class PaloManagerController extends BaseController {
    private static final String START_TIMESTAMP = "start";

    private static final String END_TIMESTAMP = "end";

    private static final String TYPE = "type";

    private static final String QUERY_ID = "query_id";

    private static final String SEARCH = "search";

    private static final String FRAGMENT_ID = "fragment_id";

    private static final String INSTANCE_ID = "instance_id";

    private static final String CONN_INFO = "/rest/v2/manager/cluster/cluster_info/conn_info";

    private static final String MONITOR_VALUE = "/rest/v2/manager/monitor/value/";

    private static final String MONITOR_TIMESERIAL = "/rest/v2/manager/monitor/timeserial/";

    private static final String FRONTENDS = "/rest/v2/manager/node/frontends";

    private static final String BACKENDS = "/rest/v2/manager/node/backends";

    private static final String BROKERS = "/rest/v2/manager/node/brokers";

    private static final String CONFIGURATION_NAME = "/rest/v2/manager/node/configuration_name";

    private static final String NODE_LIST = "/rest/v2/manager/node/node_list";

    private static final String CONFIGURATION_INFO = "/rest/v2/manager/node/configuration_info";

    private static final String UPDATE_FE_CONFIG = "/rest/v2/manager/node/set_config/fe";

    private static final String UPDATE_BE_CONFIG = "/rest/v2/manager/node/set_config/be";

    private static final String QUERY_INFO = "/rest/v2/manager/query/query_info";

    private static final String SQL = "/rest/v2/manager/query/sql/";

    private static final String PROFILE_TEXT = "/rest/v2/manager/query/profile/text/";

    private static final String PROFILE_FRAGMENTS = "/rest/v2/manager/query/profile/fragments/";

    private static final String PROFILE_GRAPH = "/rest/v2/manager/query/profile/graph/";

    @Autowired
    private PaloManagerService paloManagerService;

    @ApiOperation(value = "Doris connection infomation")
    @GetMapping(value = CONN_INFO)
    public Object ConnectionInfo(HttpServletRequest request, HttpServletResponse response) throws Exception {
        return paloManagerService.get(request, response, CONN_INFO);
    }

    @ApiOperation(value = "Monitoring chart value type")
    @GetMapping(value = MONITOR_VALUE + "{type}")
    public Object nodeNum(HttpServletRequest request, HttpServletResponse response,
                          @PathVariable("type") String type) throws Exception {
        return paloManagerService.get(request, response, MONITOR_VALUE + type);
    }

    @ApiOperation(value = "Monitoring time series diagram")
    @PostMapping(value = MONITOR_TIMESERIAL + "{type}")
    public Object qps(HttpServletRequest request, HttpServletResponse response,
                      @PathVariable("type") String type,
                      @RequestParam(value = START_TIMESTAMP) long start,
                      @RequestParam(value = END_TIMESTAMP) long end,
                      @RequestBody(required = false) String requestBody) throws Exception {
        return paloManagerService.post(request, response, requestPath(type, start, end), requestBody);
    }

    private String requestPath(String type, long start, long end) {
        StringBuffer sb = new StringBuffer();
        sb.append(MONITOR_TIMESERIAL).append(type)
                .append("?").append(START_TIMESTAMP).append("=").append(start)
                .append("&").append(END_TIMESTAMP).append("=").append(end);
        return sb.toString();
    }

    @ApiOperation(value = "Fe node information")
    @GetMapping(value = FRONTENDS)
    public Object frontends(HttpServletRequest request, HttpServletResponse response) throws Exception {
        return paloManagerService.get(request, response, FRONTENDS);
    }

    @ApiOperation(value = "Be node information")
    @GetMapping(value = BACKENDS)
    public Object backends(HttpServletRequest request, HttpServletResponse response) throws Exception {
        return paloManagerService.get(request, response, BACKENDS);
    }

    @ApiOperation(value = "broker information")
    @GetMapping(value = BROKERS)
    public Object brokers(HttpServletRequest request, HttpServletResponse response) throws Exception {
        return paloManagerService.get(request, response, BROKERS);
    }

    @ApiOperation(value = "Configuration item drop-down box")
    @GetMapping(value = CONFIGURATION_NAME)
    public Object configurationNames(HttpServletRequest request, HttpServletResponse response) throws Exception {
        return paloManagerService.get(request, response, CONFIGURATION_NAME);
    }

    @ApiOperation(value = "Configure node drop-down box")
    @GetMapping(value = NODE_LIST)
    public Object nodeList(HttpServletRequest request, HttpServletResponse response) throws Exception {
        return paloManagerService.get(request, response, NODE_LIST);
    }

    @ApiOperation(value = "Configuration item list")
    @PostMapping(value = CONFIGURATION_INFO)
    public Object configurationInfo(HttpServletRequest request, HttpServletResponse response,
                                    @RequestParam(value = TYPE) String type,
                                    @RequestBody(required = false) String requestBody) throws Exception {
        return paloManagerService.post(request, response,
                CONFIGURATION_INFO + "?" + TYPE + "=" + type, requestBody);
    }

    @ApiOperation(value = "Modify Fe configuration")
    @PostMapping(value = UPDATE_FE_CONFIG)
    public Object updateFeConfiguration(HttpServletRequest request, HttpServletResponse response,
                                        @RequestBody(required = false) String requestBody) throws Exception {
        return paloManagerService.post(request, response, UPDATE_FE_CONFIG, requestBody);
    }

    @ApiOperation(value = "Modify Be configuration")
    @PostMapping(value = UPDATE_BE_CONFIG)
    public Object updateBeConfiguration(HttpServletRequest request, HttpServletResponse response,
                                        @RequestBody(required = false) String requestBody) throws Exception {
        return paloManagerService.post(request, response, UPDATE_BE_CONFIG, requestBody);
    }

    @ApiOperation(value = "query list")
    @GetMapping(value = QUERY_INFO)
    public Object queryInfo(HttpServletRequest request, HttpServletResponse response,
                            @RequestParam(value = QUERY_ID, required = false) String queryId,
                            @RequestParam(value = SEARCH, required = false) String search) throws Exception {
        String requestPath = QUERY_INFO;
        if (queryId != null || search != null) {
            requestPath += "?";
            if (queryId != null && search != null) {
                requestPath += QUERY_ID + "=" + queryId + "&" + SEARCH + "=" + search;
            } else if (queryId != null) {
                requestPath += QUERY_ID + "=" + queryId;
            } else {
                requestPath += SEARCH + "=" + search;
            }
        }
        return paloManagerService.get(request, response, requestPath);
    }

    @ApiOperation(value = "query sql")
    @GetMapping(value = SQL + "{queryId}")
    public Object querySql(HttpServletRequest request, HttpServletResponse response,
                           @PathVariable("queryId") String queryId) throws Exception {
        return paloManagerService.get(request, response, SQL + queryId);
    }

    @ApiOperation(value = "profile text")
    @GetMapping(value = PROFILE_TEXT + "{queryId}")
    public Object profileText(HttpServletRequest request, HttpServletResponse response,
                              @PathVariable("queryId") String queryId) throws Exception {
        return paloManagerService.get(request, response, PROFILE_TEXT + queryId);
    }

    @ApiOperation(value = "profile fragments")
    @GetMapping(value = PROFILE_FRAGMENTS + "{queryId}")
    public Object profileFragments(HttpServletRequest request, HttpServletResponse response,
                                   @PathVariable("queryId") String queryId) throws Exception {
        return paloManagerService.get(request, response, PROFILE_FRAGMENTS + queryId);
    }

    @ApiOperation(value = "profile Graphical")
    @GetMapping(value = PROFILE_GRAPH + "{queryId}")
    public Object profileGraph(HttpServletRequest request, HttpServletResponse response,
                               @PathVariable("queryId") String queryId,
                               @RequestParam(value = FRAGMENT_ID, required = false) String fragment,
                               @RequestParam(value = INSTANCE_ID, required = false) String instanceId) throws Exception {
        StringBuffer sb = new StringBuffer();
        sb.append(PROFILE_GRAPH).append(queryId);
        if (!Strings.isNullOrEmpty(fragment) && !Strings.isNullOrEmpty(instanceId)) {
            sb.append("?").append(FRAGMENT_ID).append("=").append(fragment)
                    .append("&").append(INSTANCE_ID).append("=").append(instanceId);
        }
        return paloManagerService.get(request, response, sb.toString());
    }
}
