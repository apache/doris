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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
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
    public static final Logger LOG = LogManager.getLogger(CompactionAction.class);

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
            return ResponseEntityBuilder.badRequest("compact_type need to be base, cumulative or full.");
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
                        sendRequestToBe(request, backend, tablet.getId());
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
                    sendRequestToBe(request, backend, tablet.getId());
                }
            }
        }
        return ResponseEntityBuilder.ok("");
    }

    private void sendRequestToBe(HttpServletRequest request, Backend backend, long tabletId) {
        HttpURLConnection connection = null;
        URL url = null;
        try {
            String compactType = request.getParameter("compact_type");

            String urlString = String.format("http://%s:%d/api/compaction/run?tablet_id=%d&compact_type=%s",
                    backend.getHost(), backend.getHttpPort(), tabletId, compactType);

            url = new URL(urlString);
            connection = (HttpURLConnection) url.openConnection();

            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setConnectTimeout(10000);
            connection.setReadTimeout(10000);

            ActionAuthorizationInfo authInfo = getAuthorizationInfo(request);
            String auth = authInfo.fullUserName + ":" + authInfo.password;
            String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
            connection.setRequestProperty("Authorization", "Basic " + encodedAuth);

            try (OutputStream os = connection.getOutputStream()) {
                os.write(0);
                os.flush();
            }

            LOG.info("Send request to BE, URL:{}", url);
            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                try (InputStream in = connection.getInputStream();
                        ByteArrayOutputStream out = new ByteArrayOutputStream()) {

                    byte[] buffer = new byte[1024];
                    int bytesRead;

                    bytesRead = in.read(buffer);
                    out.write(buffer, 0, bytesRead);

                    String response = out.toString("UTF-8");
                    LOG.info("Success. Url:{}, Response code:{}, Response body:{} ", urlString, responseCode,
                            response);
                }
            } else {
                LOG.warn("Failed. Url:{}, Response code:{}, Response Message: {}", urlString, responseCode,
                        connection.getResponseMessage());
            }
        } catch (Exception e) {
            if (url != null) {
                LOG.warn("Exception happens for url {}:{}", url, e.getMessage());
            }
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }
}
