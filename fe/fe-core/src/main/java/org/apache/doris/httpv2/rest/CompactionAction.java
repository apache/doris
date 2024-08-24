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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.entity.RestBaseResult;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * used to trigger full compaction by table id or tablet id.
 * eg:
 * fe_host:http_port/api/compaction/run?tablet_id={int}&compact_type={enum}
 * fe_host:http_port/api/compaction/run?table_id={int}&compact_type={enum}
 */
@RestController
public class CompactionAction extends RestBaseController {
    public static final String COMPACT_TYPE = "compact_type";
    public static final String TABLET_ID = "tablet_id";
    public static final String TABLE_ID = "table_id";
    private static final Logger LOG = LogManager.getLogger(SetConfigAction.class);

    @RequestMapping(path = "/api/compaction/run", method = RequestMethod.POST)
    protected Object compaction(HttpServletRequest request, HttpServletResponse response) {
        LOG.info("Compaction action, path info: {}", request.getPathInfo());
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        String tableId = request.getParameter(TABLE_ID);
        String tabletId = request.getParameter(TABLET_ID);
        String compactType = request.getParameter(COMPACT_TYPE);
        if (Strings.isNullOrEmpty(compactType)) {
            return ResponseEntityBuilder.badRequest("compact_type need to be set.");
        } else if (!compactType.equals("base") && !compactType.equals("cumulative") && !compactType.equals("full")) {
            return ResponseEntityBuilder.badRequest("tablet id and table id can not be empty at the same time!");
        }

        if (Strings.isNullOrEmpty(tabletId)) {
            if (Strings.isNullOrEmpty(tableId)) {
                // both tablet id and table id are empty, return error.
                return ResponseEntityBuilder.badRequest("tablet id and table id can not be empty at the same time!");
            } else {
                OlapTable olapTable = (OlapTable) Env.getCurrentEnv().getInternalCatalog()
                        .getTableByTableId(Long.valueOf(tableId));
                if (olapTable == null) {
                    return new RestBaseResult("Table not found. Table id: " + tableId);
                }
                List<Tablet> tabletList = olapTable.getAllTablets();
                for (Tablet tablet : tabletList) {
                    List<Replica> replicaList = tablet.getReplicas();
                    for (Replica replica : replicaList) {
                        Backend backend = Env.getCurrentSystemInfo().getBackend(replica.getBackendId());
                        redirectTo(request, new TNetworkAddress(backend.getHost(), backend.getHttpPort()), true);
                    }
                }
            }
        } else {
            if (!Strings.isNullOrEmpty(tableId)) {
                // both tablet id and table id are not empty, return err.
                return ResponseEntityBuilder.badRequest("tablet id and table id can not be set at the same time!");
            } else {
                Tablet tablet = Env.getCurrentEnv().getInternalCatalog().getTabletByTabletId(Long.valueOf(tabletId));
                if (tablet == null) {
                    return new RestBaseResult("Tablet not found. Tablet id: " + tabletId);
                }
                List<Replica> replicaList = tablet.getReplicas();
                for (Replica replica : replicaList) {
                    Backend backend = Env.getCurrentSystemInfo().getBackend(replica.getBackendId());
                    redirectTo(request, new TNetworkAddress(backend.getHost(), backend.getHttpPort()), true);
                }
            }
        }
        return ResponseEntityBuilder.ok("");
    }

}
