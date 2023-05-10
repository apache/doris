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

package org.apache.doris.planner.external.paimon;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.ExternalTable;
import org.apache.doris.catalog.external.PaimonExternalTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.external.FileQueryScanNode;
import org.apache.doris.planner.external.TableFormatType;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TPaimonFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import avro.shaded.com.google.common.base.Preconditions;
import org.apache.paimon.hive.mapred.PaimonInputSplit;
import org.apache.paimon.table.AbstractFileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataField;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PaimonScanNode extends FileQueryScanNode {
    private static PaimonSource source = null;

    public PaimonScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv) {
        super(id, desc, "PAIMON_SCAN_NODE", StatisticalType.PAIMON_SCAN_NODE, needCheckColumnPriv);
    }

    @Override
    protected void doInitialize() throws UserException {
        ExternalTable table = (ExternalTable) desc.getTable();
        if (table.isView()) {
            throw new AnalysisException(
                String.format("Querying external view '%s.%s' is not supported", table.getDbName(), table.getName()));
        }
        computeColumnFilter();
        initBackendPolicy();
        source = new PaimonSource((PaimonExternalTable) table, desc, columnNameToRange);
        Preconditions.checkNotNull(source);
        initSchemaParams();
    }

    public static void setPaimonParams(TFileRangeDesc rangeDesc, PaimonSplit paimonSplit) {
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(paimonSplit.getTableFormatType().value());
        TPaimonFileDesc fileDesc = new TPaimonFileDesc();
        fileDesc.setPaimonSplit(paimonSplit.getSerializableSplit());
        StringBuilder columnNamesBuilder = new StringBuilder();
        StringBuilder columnTypesBuilder = new StringBuilder();
        StringBuilder columnIdsBuilder = new StringBuilder();
        boolean isFirst = true;
        for (DataField field : ((AbstractFileStoreTable) source.getPaimonTable()).schema().fields()) {
            if (!isFirst) {
                columnNamesBuilder.append(",");
                columnTypesBuilder.append(",");
                columnIdsBuilder.append(",");
            }
            columnNamesBuilder.append(field.name());
            columnTypesBuilder.append(field.type().toString());
            columnIdsBuilder.append(field.id());
            isFirst = false;
        }
        fileDesc.setPaimonColumnIds(columnIdsBuilder.toString());
        fileDesc.setPaimonColumnNames(columnNamesBuilder.toString());
        fileDesc.setPaimonColumnTypes(columnTypesBuilder.toString());
        tableFormatFileDesc.setPaimonParams(fileDesc);
        Map<String, String> map = ((AbstractFileStoreTable) source.getPaimonTable()).schema().options();
        map.entrySet();
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }

    @Override
    public List<Split> getSplits() throws UserException {
        List<Split> splits = new ArrayList<>();
        ReadBuilder readBuilder = source.getPaimonTable().newReadBuilder();
        List<org.apache.paimon.table.source.Split> paimonSplits = readBuilder.newScan().plan().splits();
        for (org.apache.paimon.table.source.Split split : paimonSplits) {
            PaimonInputSplit inputSplit = new PaimonInputSplit(
                    "tempDir",
                          (DataSplit) split
            );
            PaimonSplit paimonSplit = new PaimonSplit(inputSplit,
                    ((AbstractFileStoreTable) source.getPaimonTable()).location().toString());
            paimonSplit.setTableFormatType(TableFormatType.PAIMON);
            splits.add(paimonSplit);
        }
        return splits;
    }

    @Override
    public TFileType getLocationType() throws DdlException, MetaNotFoundException {
        String location = ((AbstractFileStoreTable) source.getPaimonTable()).location().toString();
        if (location != null && !location.isEmpty()) {
            if (location.startsWith(FeConstants.FS_PREFIX_S3)
                    || location.startsWith(FeConstants.FS_PREFIX_S3A)
                    || location.startsWith(FeConstants.FS_PREFIX_S3N)
                    || location.startsWith(FeConstants.FS_PREFIX_BOS)
                    || location.startsWith(FeConstants.FS_PREFIX_COS)
                    || location.startsWith(FeConstants.FS_PREFIX_OSS)
                    || location.startsWith(FeConstants.FS_PREFIX_OBS)) {
                return TFileType.FILE_S3;
            } else if (location.startsWith(FeConstants.FS_PREFIX_HDFS)) {
                return TFileType.FILE_HDFS;
            } else if (location.startsWith(FeConstants.FS_PREFIX_FILE)) {
                return TFileType.FILE_LOCAL;
            }
        }
        throw new DdlException("Unknown file location " + location
            + " for hms table " + source.getPaimonTable().name());
    }

    @Override
    public TFileFormatType getFileFormatType() throws DdlException, MetaNotFoundException {
        return TFileFormatType.FORMAT_JNI;
    }

    @Override
    public List<String> getPathPartitionKeys() throws DdlException, MetaNotFoundException {
        return new ArrayList<>(source.getPaimonTable().partitionKeys());
    }

    @Override
    public TFileAttributes getFileAttributes() throws UserException {
        return source.getFileAttributes();
    }

    @Override
    public TableIf getTargetTable() {
        return source.getTargetTable();
    }

    @Override
    public Map<String, String> getLocationProperties() throws MetaNotFoundException, DdlException {
        return source.getCatalog().getProperties();
    }
}
