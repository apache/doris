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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.rest.RestBaseController;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.Maps;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@RestController
@RequestMapping("/rest/v2")
public class StatisticAction extends RestBaseController {

    private static final Logger LOG = LogManager.getLogger(StatisticAction.class);

    @RequestMapping(path = "/api/cluster_overview", method = RequestMethod.GET)
    public Object clusterOverview(HttpServletRequest request, HttpServletResponse response) {
        if (!Catalog.getCurrentCatalog().isMaster()) {
            return redirectToMaster(request, response);
        }
        Map<String, Object> resultMap = Maps.newHashMap();
        Catalog catalog = Catalog.getCurrentCatalog();
        SystemInfoService infoService = Catalog.getCurrentSystemInfo();

        resultMap.put("dbCount", catalog.getDbIds().size());
        resultMap.put("tblCount", getTblCount(catalog));
        resultMap.put("diskOccupancy", getDiskOccupancy(infoService));
        resultMap.put("beCount", infoService.getClusterBackendIds(SystemInfoService.DEFAULT_CLUSTER).size());
        resultMap.put("feCount", catalog.getFrontends(null).size());
        resultMap.put("remainDisk", getRemainDisk(infoService));

        return ResponseEntityBuilder.ok(resultMap);
    }

    private int getTblCount(Catalog catalog) {
        return catalog.getDbIds().stream().map(catalog::getDbNullable).filter(Objects::nonNull)
                .map(db -> db.getTables().size()).reduce(Integer::sum).orElse(0);
    }

    private long getDiskOccupancy(SystemInfoService infoService) {
        long diskOccupancy = 0;
        List<Backend> backends = infoService.getClusterBackends(SystemInfoService.DEFAULT_CLUSTER);
        for (Backend be : backends) {
            diskOccupancy += be.getDataUsedCapacityB();
        }
        return diskOccupancy;
    }

    private long getRemainDisk(SystemInfoService infoService) {
        long remainDisk = 0;
        List<Backend> backends = infoService.getClusterBackends(SystemInfoService.DEFAULT_CLUSTER);
        for (Backend be : backends) {
            remainDisk += be.getAvailableCapacityB();
        }
        return remainDisk;
    }
}
