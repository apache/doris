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

package org.apache.doris.datasource.iceberg.dlf;

import org.apache.doris.datasource.iceberg.HiveCompatibleCatalog;
import org.apache.doris.datasource.iceberg.dlf.client.DLFCachedClientPool;

import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.Map;

public class DLFCatalog extends HiveCompatibleCatalog {

    @Override
    public void initialize(String name, Map<String, String> properties) {
        super.initialize(name, initializeFileIO(properties, conf), new DLFCachedClientPool(this.conf, properties));
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
        String dbName = tableIdentifier.namespace().level(0);
        String tableName = tableIdentifier.name();
        return new DLFTableOperations(this.conf, this.clients, this.fileIO, this.uid, dbName, tableName);
    }
}
