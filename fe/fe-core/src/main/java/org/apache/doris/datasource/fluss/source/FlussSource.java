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

package org.apache.doris.datasource.fluss.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.fluss.FlussExternalCatalog;
import org.apache.doris.datasource.fluss.FlussExternalTable;
import org.apache.doris.datasource.fluss.FlussUtils;

import org.apache.fluss.client.table.Table;

public class FlussSource {
    private final FlussExternalTable targetTable;
    private final FlussExternalCatalog catalog;
    private Table flussTable;

    public FlussSource(TupleDescriptor desc) {
        ExternalTable table = (ExternalTable) desc.getTable();
        if (!(table instanceof FlussExternalTable)) {
            throw new IllegalArgumentException("Table must be FlussExternalTable");
        }
        this.targetTable = (FlussExternalTable) table;
        this.catalog = (FlussExternalCatalog) targetTable.getCatalog();
    }

    public FlussExternalTable getTargetTable() {
        return targetTable;
    }

    public FlussExternalCatalog getCatalog() {
        return catalog;
    }

    public Table getFlussTable() {
        if (flussTable == null) {
            flussTable = FlussUtils.getFlussTable(targetTable);
        }
        return flussTable;
    }
}

