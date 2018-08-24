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
import com.baidu.palo.cluster.ClusterNamespace;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.http.ActionController;
import com.baidu.palo.http.BaseRequest;
import com.baidu.palo.http.BaseResponse;
import com.baidu.palo.http.IllegalArgException;
import com.baidu.palo.mysql.privilege.PrivPredicate;
import com.baidu.palo.service.ExecuteEnv;
import com.baidu.palo.system.Backend;
import com.baidu.palo.thrift.TNetworkAddress;

import com.google.common.base.Strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;

public class LoadAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(LoadAction.class);

    public static final String CLUSTER_NAME_PARAM = "cluster";
    public static final String DB_NAME_PARAM = "db";
    public static final String TABLE_NAME_PARAM = "table";
    public static final String LABEL_NAME_PARAM = "label";
    public static final String SUB_LABEL_NAME_PARAM = "sub_label";

    private ExecuteEnv execEnv;

    public LoadAction(ActionController controller, ExecuteEnv execEnv) {
        super(controller);
        this.execEnv = execEnv;
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        ExecuteEnv execEnv = ExecuteEnv.getInstance();
        LoadAction action = new LoadAction(controller, execEnv);
        controller.registerHandler(HttpMethod.PUT,
                "/api/{" + DB_NAME_PARAM + "}/{" + TABLE_NAME_PARAM + "}/_load", action);
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) throws DdlException {
        // A 'Load' request must have 100-continue header
        if (!request.getRequest().headers().contains(HttpHeaders.Names.EXPECT)) {
            throw new DdlException("There is no 100-continue header");
        }

        final AuthorizationInfo authInfo = getAuthorizationInfo(request);
        if (authInfo == null) {
            throw new DdlException("Authorize failed");
        } 

        final String clusterName = authInfo.cluster;
        if (Strings.isNullOrEmpty(clusterName)) {
            throw new DdlException("No cluster selected.");
        }

        String dbName = request.getSingleParameter(DB_NAME_PARAM);
        if (Strings.isNullOrEmpty(dbName)) {
            throw new DdlException("No database selected.");
        }

        String tableName = request.getSingleParameter(TABLE_NAME_PARAM);
        if (Strings.isNullOrEmpty(dbName)) {
            throw new DdlException("No table selected.");
        }
        
        String fullDbName = ClusterNamespace.getFullName(authInfo.cluster, dbName);
        String label = request.getSingleParameter(LABEL_NAME_PARAM);
        String subLabel = request.getSingleParameter(SUB_LABEL_NAME_PARAM);
        if (Strings.isNullOrEmpty(label)) {
            throw new DdlException("No label selected.");
        }
 
        // check auth
        checkTblAuth(authInfo, fullDbName, tableName, PrivPredicate.LOAD);

        // Try to redirect to master
        if (redirectToMaster(request, response)) {
            return;
        }

        // Choose a backend sequentially.
        List<Long> backendIds = Catalog.getCurrentSystemInfo().seqChooseBackendIds(1, true, false, clusterName);
        if (backendIds == null) {
            throw new DdlException("No backend alive.");
        }

        Backend backend = Catalog.getCurrentSystemInfo().getBackend(backendIds.get(0));
        if (backend == null) {
            throw new DdlException("No backend alive.");
        }

        TNetworkAddress redirectAddr = new TNetworkAddress(backend.getHost(), backend.getHttpPort());
        if (!Strings.isNullOrEmpty(subLabel)) {
            redirectAddr = execEnv.getMultiLoadMgr().redirectAddr(fullDbName, label, tableName, redirectAddr);
        }
        LOG.info("mini load redirect to backend: {}, label: {}", redirectAddr.toString(), label);

        redirectTo(request, response, redirectAddr);
    }
}
