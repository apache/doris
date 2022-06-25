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


import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * External data source for elasticsearch
 */
public class EsExternalDataSource extends ExternalDataSource {
    /**
     * Default constructor for EsExternalDataSource.
     */
    public EsExternalDataSource(String name, Map<String, String> props) {
        setName(name);
        getDsProperty().setProperties(props);
        setType("es");
    }

    @Override
    public List<String> listDatabaseNames(SessionContext ctx) {
        return null;
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        return null;
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        return false;
    }

    @Override
    public List<Long> getDbIds() {
        // TODO: implement it
        return Lists.newArrayList();
    }
}
