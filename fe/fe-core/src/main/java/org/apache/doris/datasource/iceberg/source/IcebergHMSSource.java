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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.source.HiveScanNode;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.planner.ColumnRange;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileTextScanRangeParams;

import java.util.Map;

public class IcebergHMSSource implements IcebergSource {

    private final HMSExternalTable hmsTable;
    private final TupleDescriptor desc;
    private final Map<String, ColumnRange> columnNameToRange;
    private final org.apache.iceberg.Table icebergTable;

    public IcebergHMSSource(HMSExternalTable hmsTable, TupleDescriptor desc,
            Map<String, ColumnRange> columnNameToRange) {
        this.hmsTable = hmsTable;
        this.desc = desc;
        this.columnNameToRange = columnNameToRange;
        this.icebergTable =
                Env.getCurrentEnv().getExtMetaCacheMgr().getIcebergMetadataCache()
                        .getIcebergTable(hmsTable.getCatalog(),
                                hmsTable.getDbName(), hmsTable.getName());
    }

    @Override
    public TupleDescriptor getDesc() {
        return desc;
    }

    @Override
    public String getFileFormat() throws DdlException, MetaNotFoundException {
        return IcebergUtils.getFileFormat(icebergTable).name();
    }

    public org.apache.iceberg.Table getIcebergTable() throws MetaNotFoundException {
        return icebergTable;
    }

    @Override
    public TableIf getTargetTable() {
        return hmsTable;
    }

    @Override
    public TFileAttributes getFileAttributes() throws UserException {
        TFileTextScanRangeParams textParams = new TFileTextScanRangeParams();
        textParams.setColumnSeparator(hmsTable.getRemoteTable().getSd().getSerdeInfo().getParameters()
                .getOrDefault(HiveScanNode.PROP_FIELD_DELIMITER, HiveScanNode.DEFAULT_FIELD_DELIMITER));
        textParams.setLineDelimiter(HiveScanNode.DEFAULT_LINE_DELIMITER);
        TFileAttributes fileAttributes = new TFileAttributes();
        fileAttributes.setTextParams(textParams);
        fileAttributes.setHeaderType("");
        return fileAttributes;
    }

    @Override
    public ExternalCatalog getCatalog() {
        return hmsTable.getCatalog();
    }
}
