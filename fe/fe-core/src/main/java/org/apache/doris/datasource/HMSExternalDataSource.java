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

import org.apache.doris.catalog.Column;
import org.apache.doris.external.ExternalScanRange;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * External data source for hive metastore compatible data sources.
 */
public class HMSExternalDataSource extends ExternalDataSource {
    private static final Logger LOG = LogManager.getLogger(HMSExternalDataSource.class);
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";

    public HMSExternalDataSource() {
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
    public List<Column> getSchema(SessionContext ctx, String dbName, String tblName) {
        return null;
    }

    @Override
    public List<ExternalScanRange> getExternalScanRanges(SessionContext ctx) {
        return null;
    }
}
