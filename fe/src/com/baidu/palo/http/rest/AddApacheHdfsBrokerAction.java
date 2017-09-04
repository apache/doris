// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.http.rest;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.common.Pair;
import com.baidu.palo.http.ActionController;
import com.baidu.palo.http.BaseRequest;
import com.baidu.palo.http.BaseResponse;
import com.baidu.palo.http.IllegalArgException;
import com.baidu.palo.system.SystemInfoService;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.util.List;

import io.netty.handler.codec.http.HttpMethod;

/*
 * fe_host:fe_http_port/api/add_apache_hdfs_broker?name=broker_name\&host_ports=host:port,host2:port2...
 * return:
 * {"status":"OK","msg":"Success"}
 * {"status":"FAILED","msg":"err info..."}
 */
public class AddApacheHdfsBrokerAction extends RestBaseAction {
    public static final String NAME = "name";
    public static final String HOST_PORTS = "host_ports";

    public AddApacheHdfsBrokerAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, 
                "/api/add_apache_hdfs_broker",
                new AddApacheHdfsBrokerAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) throws DdlException {
        String name = request.getSingleParameter(NAME);
        if (Strings.isNullOrEmpty(name)) {
            throw new DdlException("No broker name specified.");
        }
        String hostPorts = request.getSingleParameter(HOST_PORTS);
        if (Strings.isNullOrEmpty(hostPorts)) {
            throw new DdlException("No host:port specified.");
        }

        String[] hostPortArr = hostPorts.split(",");
        if (hostPortArr.length == 0) {
            throw new DdlException("No host:port specified.");
        }

        List<Pair<String, Integer>> hostPortPairs = Lists.newArrayList();
        for (String hostPort : hostPortArr) {
            Pair<String, Integer> pair;
            try {
                pair = SystemInfoService.validateHostAndPort(hostPort);
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
            hostPortPairs.add(pair);
        }
        
        Catalog.getInstance().getBrokerMgr().addBrokers(name, hostPortPairs);

        // to json response
        RestBaseResult result = new RestBaseResult();

        // send result
        response.setContentType("application/json");
        response.getContent().append(result.toJson());
        sendResult(request, response);
    }
}
