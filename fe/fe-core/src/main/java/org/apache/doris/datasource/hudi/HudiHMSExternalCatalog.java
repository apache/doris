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

package org.apache.doris.datasource.hudi;

import org.apache.doris.catalog.external.HudiExternalDatabase;
import org.apache.doris.datasource.HMSExternalCatalog;
import org.apache.doris.datasource.SessionContext;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

public class HudiHMSExternalCatalog extends HMSExternalCatalog {


    public static final String HUDI_CATALOG_TYPE = "hudi.catalog.type";
    public static final String HUDI_HMS = "hms";

    /**
     * Default constructor for HMSExternalCatalog.
     *
     * @param catalogId
     * @param name
     * @param resource
     * @param props
     */
    public HudiHMSExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
            String comment) {
        super(catalogId, name, resource, props, comment);
    }


    public String getHudiCatalogType() {
        makeSureInitialized();
        return HUDI_HMS;
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        HudiExternalDatabase hudiExternalDatabase = (HudiExternalDatabase) idToDb.get(dbNameToId.get(dbName));
        if (hudiExternalDatabase != null && hudiExternalDatabase.isInitialized()) {
            List<String> names = Lists.newArrayList();
            hudiExternalDatabase.getTables().forEach(table -> names.add(table.getName()));
            return names;
        } else {
            return client.getAllTables(getRealTableName(dbName));
        }
    }

}

