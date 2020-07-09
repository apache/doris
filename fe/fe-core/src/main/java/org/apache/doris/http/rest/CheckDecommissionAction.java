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

package org.apache.doris.http.rest;


import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.doris.alter.SystemHandler;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.http.entity.HttpStatus;
import org.apache.doris.http.entity.ResponseEntity;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;

/*
 * calc row count from replica to table
 * fe_host:fe_http_port/api/check_decommission?host_ports=host:port,host2:port2...
 * return:
 * {"status":"OK","msg":"Success"}
 * {"status":"FAILED","msg":"err info..."}
 */
@RestController
public class CheckDecommissionAction extends RestBaseController {

    public static final String HOST_PORTS = "host_ports";


    @RequestMapping(path = "/api/check_decommission",method = RequestMethod.GET)
    public Object execute(HttpServletRequest request, HttpServletResponse response)
            throws DdlException {
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build("Success");
        //check user auth
        executeCheckPassword(request,response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.OPERATOR);
        String hostPorts = request.getParameter(HOST_PORTS);
        if (Strings.isNullOrEmpty(hostPorts)) {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            entity.setMsg("No host:port specified");
            return  entity;
        }

        String[] hostPortArr = hostPorts.split(",");
        if (hostPortArr.length == 0) {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            entity.setMsg("No host:port specified");
            return  entity;
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

        SystemHandler.checkDecommission(hostPortPairs);
        return entity;
    }
}
