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

package org.apache.doris.datasource.deltalake;

import org.apache.doris.catalog.external.HMSExternalDatabase;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.datasource.HMSExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;
import java.util.Map;

public class DeltaLakeExternalCatalog extends HMSExternalCatalog {

    public DeltaLakeExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
                String comment) {
        super(catalogId, name, resource, props, comment, InitCatalogLog.Type.DELTALAKE);
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        HMSExternalDatabase hmsExternalDatabase = (HMSExternalDatabase) idToDb.get(dbNameToId.get(dbName));
        if (hmsExternalDatabase != null && hmsExternalDatabase.isInitialized()) {
            List<String> names = Lists.newArrayList();
            for (HMSExternalTable table : hmsExternalDatabase.getTables()) {
                String tableName = table.getName();
                Table tableDetails = client.getTable(dbName, tableName);
                Map<String, String> parameters = tableDetails.getParameters();
                String provider = parameters.get("spark.sql.sources.provider");
                if ("delta".equalsIgnoreCase(provider)) {
                    names.add(tableName);
                }
            }
            return names;
        } else {
            List<String> allTableNames = client.getAllTables(getRealTableName(dbName));
            List<String> deltaTableNames = Lists.newArrayList();
            for (String tableName : allTableNames) {
                Table tableDetails = client.getTable(dbName, tableName);
                Map<String, String> parameters = tableDetails.getParameters();
                String provider = parameters.get("spark.sql.sources.provider");
                if ("delta".equalsIgnoreCase(provider)) {
                    deltaTableNames.add(tableName);
                }
            }
            return deltaTableNames;
        }
    }
}
