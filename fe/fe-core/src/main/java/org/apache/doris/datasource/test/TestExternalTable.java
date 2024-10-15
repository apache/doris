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

package org.apache.doris.datasource.test;

import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

/**
 * TestExternalTable is a table for unit test.
 */
public class TestExternalTable extends ExternalTable {
    private static final Logger LOG = LogManager.getLogger(TestExternalTable.class);

    public TestExternalTable(long id, String name, String dbName, TestExternalCatalog catalog) {
        super(id, name, catalog, dbName, TableType.TEST_EXTERNAL_TABLE);
    }

    @Override
    public synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        this.objectCreated = true;
    }

    @Override
    public TTableDescriptor toThrift() {
        makeSureInitialized();
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.TEST_EXTERNAL_TABLE,
                getFullSchema().size(),
                0, getName(), "");
        return tTableDescriptor;
    }

    @Override
    public Optional<SchemaCacheValue> initSchema() {
        return Optional.of(new SchemaCacheValue(((TestExternalCatalog) catalog).mockedSchema(dbName, name)));
    }
}
