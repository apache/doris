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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TStorageType;

import com.google.common.base.Strings;

import org.json.JSONObject;

import java.util.List;
import java.util.Map;

import io.netty.handler.codec.http.HttpMethod;

public class StorageTypeCheckAction extends RestBaseAction {
    public StorageTypeCheckAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        StorageTypeCheckAction action = new StorageTypeCheckAction(controller);
        controller.registerHandler(HttpMethod.GET, "/api/_check_storagetype", action);
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException {
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        String dbName = request.getSingleParameter(DB_KEY);
        if (Strings.isNullOrEmpty(dbName)) {
            throw new DdlException("Parameter db is missing");
        }

        String fullDbName = ClusterNamespace.getFullName(ConnectContext.get().getClusterName(), dbName);
        Database db = catalog.getDb(fullDbName);
        if (db == null) {
            throw new DdlException("Database " + dbName + " does not exist");
        }

        JSONObject root = new JSONObject();
        List<Table> tableList = null;
        db.readLock();
        try {
            tableList = db.getTables();
        } finally {
            db.readUnlock();
        }

        for (Table tbl : tableList) {
            if (tbl.getType() != TableType.OLAP) {
                continue;
            }

            OlapTable olapTbl = (OlapTable) tbl;
            olapTbl.readLock();
            try {
                JSONObject indexObj = new JSONObject();
                for (Map.Entry<Long, MaterializedIndexMeta> entry : olapTbl.getIndexIdToMeta().entrySet()) {
                    MaterializedIndexMeta indexMeta = entry.getValue();
                    if (indexMeta.getStorageType() == TStorageType.ROW) {
                        indexObj.put(olapTbl.getIndexNameById(entry.getKey()), indexMeta.getStorageType().name());
                    }
                }
                root.put(tbl.getName(), indexObj);
            } finally {
                olapTbl.readUnlock();
            }
        }

        // to json response
        String result = root.toString();

        // send result
        response.setContentType("application/json");
        response.getContent().append(result);
        sendResult(request, response);
    }
}
