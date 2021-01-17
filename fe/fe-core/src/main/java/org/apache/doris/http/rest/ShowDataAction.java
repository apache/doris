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

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;

import io.netty.handler.codec.http.HttpMethod;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ShowDataAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(ShowDataAction.class);

    public ShowDataAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction (ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/show_data", new ShowDataAction(controller));
    }

    public long getDataSizeOfDatabase(Database db) {
        long totalSize = 0;
        long tableSize = 0;
        // sort by table name
        List<Table> tables = db.getTables();
        for (Table table : tables) {
            if (table.getType() != TableType.OLAP) {
                continue;
            }
            table.readLock();
            try {
                tableSize = ((OlapTable)table).getDataSize();
            } finally {
                table.readUnlock();
            }
            totalSize += tableSize;
        } // end for tables
        return totalSize;
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        String dbName = request.getSingleParameter("db");
        ConcurrentHashMap<String, Database> fullNameToDb = Catalog.getCurrentCatalog().getFullNameToDb();
        long totalSize = 0;
        if (dbName != null) {
            Database db = fullNameToDb.get("default_cluster:"+dbName);
            if (db == null) {
                response.getContent().append("database " + dbName + " not found.");
                sendResult(request, response, HttpResponseStatus.NOT_FOUND);
                return;
            }
            totalSize = getDataSizeOfDatabase(db);    
        } else {
            for (Database db : fullNameToDb.values()) {
                LOG.info("database name: {}", db.getFullName());
                totalSize += getDataSizeOfDatabase(db);
            }
        }
        response.getContent().append(String.valueOf(totalSize));
        sendResult(request, response);
    }
}
