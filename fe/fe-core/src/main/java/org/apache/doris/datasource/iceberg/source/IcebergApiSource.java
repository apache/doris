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

package org.apache.doris.datasource.iceberg.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.planner.ColumnRange;

import org.apache.iceberg.Table;

import java.util.Map;

/**
 * Get metadata from iceberg api (all iceberg table like hive, rest, glue...)
 */
public class IcebergApiSource implements IcebergSource {

    private final IcebergExternalTable icebergExtTable;
    private final Table originTable;

    private final TupleDescriptor desc;

    public IcebergApiSource(IcebergExternalTable table, TupleDescriptor desc,
                            Map<String, ColumnRange> columnNameToRange) {
        // Theoretically, the IcebergScanNode is responsible for scanning data from physical tables.
        // Views should not reach this point.
        // By adding this validation, we aim to ensure that if a view query does end up here, it indicates a bug.
        // This helps us identify issues promptly.

        // when use legacy planner, query an iceberg view will enter this
        // we should set enable_fallback_to_original_planner=false
        // so that it will throw exception by first planner
        if (table.isView()) {
            throw new UnsupportedOperationException("IcebergApiSource does not support view");
        }
        this.icebergExtTable = table;

        this.originTable = Env.getCurrentEnv().getExtMetaCacheMgr().getIcebergMetadataCache().getIcebergTable(
                icebergExtTable);

        this.desc = desc;
    }

    @Override
    public TupleDescriptor getDesc() {
        return desc;
    }

    @Override
    public String getFileFormat() {
        return IcebergUtils.getFileFormat(originTable).name();
    }

    @Override
    public Table getIcebergTable() throws MetaNotFoundException {
        return originTable;
    }

    @Override
    public TableIf getTargetTable() {
        return icebergExtTable;
    }

    @Override
    public ExternalCatalog getCatalog() {
        return icebergExtTable.getCatalog();
    }

}
