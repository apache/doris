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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.MetaNotFoundException;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * DataSourceMgr will loaded all data sources at FE startup,
 * and save them in maps mapping with id and name.
 */
public class DataSourceMgr {

    private Map<Long, ExternalDataSource> idToDataSource = Maps.newConcurrentMap();
    private Map<String, ExternalDataSource> nameToDataSource = Maps.newConcurrentMap();

    public DataSourceMgr() {
        loadDataSources();
    }

    public void loadDataSources() {
        // TODO: Actually, we should initialize the data source object where user executing "create catalog" cmd,
        //       not loaded from config file.
    }

    private void registerDataSource(ExternalDataSource ds) {
        ds.setId(Catalog.getCurrentCatalog().getNextId());
        idToDataSource.put(ds.getId(), ds);
        nameToDataSource.put(ds.getName(), ds);
    }

    public <E extends MetaNotFoundException> ExternalDataSource getDataSourceOrException(long id, java.util.function.Function<Long, E> e) throws E {
        ExternalDataSource ds = idToDataSource.get(id);
        if (ds == null) {
            throw e.apply(id);
        }
        return ds;
    }

    public <E extends MetaNotFoundException> ExternalDataSource getDataSourceOrException(String name, java.util.function.Function<String, E> e) throws E {
        ExternalDataSource ds = nameToDataSource.get(name);
        if (ds == null) {
            throw e.apply(name);
        }
        return ds;
    }

    public boolean hasDataSource(String name) {
        return nameToDataSource.containsKey(name);
    }
}
