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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;

public class LoadAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(LoadAction.class);

    public static final String SUB_LABEL_NAME_PARAM = "sub_label";

    private ExecuteEnv execEnv;
    private boolean isStreamLoad = false;

    public LoadAction(ActionController controller, ExecuteEnv execEnv) {
        this(controller, execEnv, false);
    }

    public LoadAction(ActionController controller, ExecuteEnv execEnv, boolean isStreamLoad) {
        super(controller);
        this.execEnv = execEnv;
        this.isStreamLoad = isStreamLoad;
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        ExecuteEnv execEnv = ExecuteEnv.getInstance();
        LoadAction action = new LoadAction(controller, execEnv);
        controller.registerHandler(HttpMethod.PUT,
                                   "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_load", action);

        controller.registerHandler(HttpMethod.PUT,
                                   "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_stream_load",
                                   new LoadAction(controller, execEnv, true));
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException {

        // A 'Load' request must have 100-continue header
        if (!request.getRequest().headers().contains(HttpHeaders.Names.EXPECT)) {
            throw new DdlException("There is no 100-continue header");
        }

        final String clusterName = ConnectContext.get().getClusterName();
        if (Strings.isNullOrEmpty(clusterName)) {
            throw new DdlException("No cluster selected.");
        }

        String dbName = request.getSingleParameter(DB_KEY);
        if (Strings.isNullOrEmpty(dbName)) {
            throw new DdlException("No database selected.");
        }

        String tableName = request.getSingleParameter(TABLE_KEY);
        if (Strings.isNullOrEmpty(tableName)) {
            throw new DdlException("No table selected.");
        }
        
        String fullDbName = ClusterNamespace.getFullName(clusterName, dbName);

        String label = request.getSingleParameter(LABEL_KEY);
        if (!isStreamLoad) {
            if (Strings.isNullOrEmpty(label)) {
                throw new DdlException("No label selected.");
            }
        } else {
            label = request.getRequest().headers().get(LABEL_KEY);
        }
 
        // check auth
        checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, tableName, PrivPredicate.LOAD);

        TNetworkAddress redirectAddr;
        if (!isStreamLoad && !Strings.isNullOrEmpty(request.getSingleParameter(SUB_LABEL_NAME_PARAM))) {
            // only multi mini load need to redirect to Master, because only Master has the info of table to
            // the Backend which the file exists.
            if (redirectToMaster(request, response)) {
                return;
            }
            redirectAddr = execEnv.getMultiLoadMgr().redirectAddr(fullDbName, label);
        } else {
            // Choose a backend sequentially.
            List<Long> backendIds = Catalog.getCurrentSystemInfo().seqChooseBackendIdsByStorageMediumAndTag(
                    1, true, false, clusterName, null, null);
            if (backendIds == null) {
                throw new DdlException("No backend alive.");
            }

            Backend backend = Catalog.getCurrentSystemInfo().getBackend(backendIds.get(0));
            if (backend == null) {
                throw new DdlException("No backend alive.");
            }

            redirectAddr = new TNetworkAddress(backend.getHost(), backend.getHttpPort());
        }

        LOG.info("redirect load action to destination={}, stream: {}, db: {}, tbl: {}, label: {}",
                redirectAddr.toString(), isStreamLoad, dbName, tableName, label);
        redirectTo(request, response, redirectAddr);
    }
}

