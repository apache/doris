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

package org.apache.doris.planner.external.iceberg;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.IcebergExternalTable;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.IcebergHMSExternalCatalog;
import org.apache.doris.planner.ColumnRange;
import org.apache.doris.planner.external.ExternalFileScanNode;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;

import java.util.Map;

public class IcebergApiSourceProvider implements IcebergSourceProvider {

    private IcebergExternalTable icebergExtTable;
    private Table originTable;

    public IcebergApiSourceProvider(IcebergExternalTable table, Map<String, ColumnRange> columnNameToRange) {
        this.icebergExtTable = table;
        this.originTable = ((IcebergHMSExternalCatalog) icebergExtTable.getCatalog())
                .getIcebergTable(icebergExtTable.getDbName(), icebergExtTable.getName());
    }

    @Override
    public String getFileFormat() {
        return originTable.properties()
            .getOrDefault(TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
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
    public ExternalFileScanNode.ParamCreateContext createContext() throws UserException {
        return null;
    }

    @Override
    public TFileAttributes getFileAttributes() throws UserException {
        return null;
    }

    @Override
    public ExternalCatalog getCatalog() {
        return icebergExtTable.getCatalog();
    }

}
