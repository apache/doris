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

package org.apache.doris.datasource.lakesoul;

import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.property.PropertyConverter;

import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

public class LakeSoulExternalCatalog extends ExternalCatalog {

    private DBManager dbManager;

    private final Map<String, String> props;

    public LakeSoulExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
                                   String comment) {
        super(catalogId, name, InitCatalogLog.Type.LAKESOUL, comment);
        this.props = PropertyConverter.convertToMetaProperties(props);
        catalogProperty = new CatalogProperty(resource, props);
        initLocalObjectsImpl();
    }

    @Override
    protected List<String> listDatabaseNames() {
        initLocalObjectsImpl();
        return dbManager.listNamespaces();
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        List<TableInfo> tifs = dbManager.getTableInfosByNamespace(dbName);
        List<String> tableNames = Lists.newArrayList();
        for (TableInfo item : tifs) {
            tableNames.add(item.getTableName());
        }
        return tableNames;
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        makeSureInitialized();
        TableInfo tableInfo = dbManager.getTableInfoByNameAndNamespace(dbName, tblName);

        return null != tableInfo;
    }

    @Override
    protected void initLocalObjectsImpl() {
        if (dbManager == null) {
            if (props != null) {
                if (props.containsKey(DBUtil.urlKey)) {
                    System.setProperty(DBUtil.urlKey, props.get(DBUtil.urlKey));
                }
                if (props.containsKey(DBUtil.usernameKey)) {
                    System.setProperty(DBUtil.usernameKey, props.get(DBUtil.usernameKey));
                }
                if (props.containsKey(DBUtil.passwordKey)) {
                    System.setProperty(DBUtil.passwordKey, props.get(DBUtil.passwordKey));
                }
            }
            dbManager = new DBManager();
        }
    }

    public TableInfo getLakeSoulTable(String dbName, String tblName) {
        makeSureInitialized();
        return dbManager.getTableInfoByNameAndNamespace(tblName, dbName);
    }
}

