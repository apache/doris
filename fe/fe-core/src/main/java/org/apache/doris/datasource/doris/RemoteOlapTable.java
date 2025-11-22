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

package org.apache.doris.datasource.doris;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.system.Backend;

import com.google.common.collect.ImmutableMap;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public class RemoteOlapTable extends OlapTable {
    private RemoteDorisExternalCatalog catalog;
    private RemoteDorisExternalDatabase database;

    public RemoteDorisExternalCatalog getCatalog() {
        return catalog;
    }

    public void setCatalog(RemoteDorisExternalCatalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public RemoteDorisExternalDatabase getDatabase() {
        return database;
    }

    public void setDatabase(RemoteDorisExternalDatabase database) {
        this.database = database;
    }

    public static RemoteOlapTable fromOlapTable(OlapTable olapTable) {
        try {
            RemoteOlapTable externalOlapTable = new RemoteOlapTable();
            Class<?> currentClass = olapTable.getClass();
            while (currentClass != null) {
                for (Field field : currentClass.getDeclaredFields()) {
                    if (Modifier.isStatic(field.getModifiers())) {
                        continue;
                    }
                    field.setAccessible(true);
                    field.set(externalOlapTable, field.get(olapTable));
                }
                currentClass = currentClass.getSuperclass();
            }
            return externalOlapTable;
        } catch (Exception e) {
            throw new RuntimeException("failed to initial external olap table", e);
        }
    }

    @Override
    public long getCatalogId() {
        return catalog.getId();
    }

    public ImmutableMap<Long, Backend> getAllBackendsByAllCluster() throws AnalysisException {
        return catalog.getBackends();
    }

    @Override
    public boolean isInternal() {
        return false;
    }
}
