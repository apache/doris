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

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.HiveMetaStoreClientHelper;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.planner.ColumnRange;
import org.apache.doris.planner.external.ExternalFileScanNode;
import org.apache.doris.planner.external.HiveScanProvider;
import org.apache.doris.thrift.TFileAttributes;

import org.apache.iceberg.TableProperties;

import java.util.Map;

public class IcebergHMSSource implements IcebergSource {

    private final HMSExternalTable hmsTable;
    private final HiveScanProvider hiveScanProvider;

    private final TupleDescriptor desc;

    public IcebergHMSSource(HMSExternalTable hmsTable, TupleDescriptor desc,
                            Map<String, ColumnRange> columnNameToRange) {
        this.hiveScanProvider = new HiveScanProvider(hmsTable, desc, columnNameToRange);
        this.hmsTable = hmsTable;
        this.desc = desc;
    }

    @Override
    public TupleDescriptor getDesc() {
        return desc;
    }

    @Override
    public String getFileFormat() throws DdlException, MetaNotFoundException {
        return hiveScanProvider.getRemoteHiveTable().getParameters()
                .getOrDefault(TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    }

    public org.apache.iceberg.Table getIcebergTable() throws MetaNotFoundException {
        return HiveMetaStoreClientHelper.getIcebergTable(hmsTable);
    }

    @Override
    public ExternalFileScanNode.ParamCreateContext createContext() throws UserException {
        return hiveScanProvider.createContext(null);
    }

    @Override
    public void updateRequiredSlots(ExternalFileScanNode.ParamCreateContext context) throws UserException {
        hiveScanProvider.updateRequiredSlots(context);
    }

    @Override
    public TableIf getTargetTable() {
        return hiveScanProvider.getTargetTable();
    }

    @Override
    public TFileAttributes getFileAttributes() throws UserException {
        return hiveScanProvider.getFileAttributes();
    }

    @Override
    public ExternalCatalog getCatalog() {
        return hmsTable.getCatalog();
    }
}
