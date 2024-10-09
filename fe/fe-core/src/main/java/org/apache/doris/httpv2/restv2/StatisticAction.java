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

package org.apache.doris.httpv2.restv2;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.rest.RestBaseController;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
@RequestMapping("/rest/v2")
public class StatisticAction extends RestBaseController {

    private static final Logger LOG = LogManager.getLogger(StatisticAction.class);

    @RequestMapping(path = "/api/cluster_overview", method = RequestMethod.GET)
    public Object clusterOverview(HttpServletRequest request, HttpServletResponse response) {
        if (Config.enable_all_http_auth) {
            executeCheckPassword(request, response);
        }

        try {
            if (!Env.getCurrentEnv().isMaster()) {
                return redirectToMasterOrException(request, response);
            }
        } catch (Exception e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }

        Map<String, Object> resultMap = Maps.newHashMap();
        Env env = Env.getCurrentEnv();
        SystemInfoService infoService = Env.getCurrentSystemInfo();

        resultMap.put("dbCount", env.getInternalCatalog().getDbIds().size());
        resultMap.put("tblCount", getTblCount(env));
        resultMap.put("diskOccupancy", getDiskOccupancy(infoService));
        resultMap.put("beCount", infoService.getAllBackendIds().size());
        resultMap.put("feCount", env.getFrontends(null).size());
        resultMap.put("remainDisk", getRemainDisk(infoService));

        return ResponseEntityBuilder.ok(resultMap);
    }

    private int getTblCount(Env env) {
        InternalCatalog catalog = env.getInternalCatalog();
        return catalog.getDbIds().stream().map(catalog::getDbNullable).filter(Objects::nonNull)
                .map(db -> db.getTables().size()).reduce(Integer::sum).orElse(0);
    }

    private long getDiskOccupancy(SystemInfoService infoService) {
        long diskOccupancy = 0;
        List<Backend> backends;
        try {
            backends = infoService.getAllBackendsByAllCluster().values().asList();
        } catch (UserException e) {
            LOG.warn("failed to get backends by current cluster", e);
            return 0;
        }
        for (Backend be : backends) {
            diskOccupancy += be.getDataUsedCapacityB();
        }
        return diskOccupancy;
    }

    private long getRemainDisk(SystemInfoService infoService) {
        long remainDisk = 0;
        List<Backend> backends;
        try {
            backends = infoService.getAllBackendsByAllCluster().values().asList();
        } catch (UserException e) {
            LOG.warn("failed to get backends by current cluster", e);
            return 0;
        }
        for (Backend be : backends) {
            remainDisk += be.getAvailableCapacityB();
        }
        return remainDisk;
    }
}
