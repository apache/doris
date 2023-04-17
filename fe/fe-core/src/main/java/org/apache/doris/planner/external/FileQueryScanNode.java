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

package org.apache.doris.planner.external;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FunctionGenTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.catalog.external.IcebergExternalTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.external.iceberg.IcebergApiSource;
import org.apache.doris.planner.external.iceberg.IcebergHMSSource;
import org.apache.doris.planner.external.iceberg.IcebergScanProvider;
import org.apache.doris.planner.external.iceberg.IcebergSource;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.tablefunction.ExternalFileTableValuedFunction;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileScanSlotInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * FileQueryScanNode for querying the file access type of catalog, now only support
 * hive,hudi and iceberg.
 */
public class FileQueryScanNode extends FileScanNode {
    private static final Logger LOG = LogManager.getLogger(FileQueryScanNode.class);

    // For query, there is only one FileScanProvider.
    private FileScanProviderIf scanProvider;

    private Map<String, SlotDescriptor> destSlotDescByName;
    private TFileScanRangeParams params;

    /**
     * External file scan node for Query hms table
     * needCheckColumnPriv: Some of ExternalFileScanNode do not need to check column priv
     * eg: s3 tvf
     * These scan nodes do not have corresponding catalog/database/table info, so no need to do priv check
     */
    public FileQueryScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv) {
        super(id, desc, "FILE_QUERY_SCAN_NODE", StatisticalType.FILE_SCAN_NODE, needCheckColumnPriv);
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);

        computeColumnFilter();
        if (this.desc.getTable() instanceof HMSExternalTable) {
            HMSExternalTable hmsTable = (HMSExternalTable) this.desc.getTable();
            initHMSExternalTable(hmsTable);
        } else if (this.desc.getTable() instanceof FunctionGenTable) {
            FunctionGenTable table = (FunctionGenTable) this.desc.getTable();
            initFunctionGenTable(table, (ExternalFileTableValuedFunction) table.getTvf());
        } else if (this.desc.getTable() instanceof IcebergExternalTable) {
            IcebergExternalTable table = (IcebergExternalTable) this.desc.getTable();
            initIcebergExternalTable(table);
        }
        backendPolicy.init();
        numNodes = backendPolicy.numBackends();
        initScanRangeParams();
    }

    /**
     * Init ExternalFileScanNode, ONLY used for Nereids. Should NOT use this function in anywhere else.
     */
    public void init() throws UserException {
        // prepare for partition prune
        // computeColumnFilter();
        if (this.desc.getTable() instanceof HMSExternalTable) {
            HMSExternalTable hmsTable = (HMSExternalTable) this.desc.getTable();
            initHMSExternalTable(hmsTable);
        } else if (this.desc.getTable() instanceof FunctionGenTable) {
            FunctionGenTable table = (FunctionGenTable) this.desc.getTable();
            initFunctionGenTable(table, (ExternalFileTableValuedFunction) table.getTvf());
        } else if (this.desc.getTable() instanceof IcebergExternalTable) {
            IcebergExternalTable table = (IcebergExternalTable) this.desc.getTable();
            initIcebergExternalTable(table);
        }

        backendPolicy.init();
        numNodes = backendPolicy.numBackends();
        initScanRangeParams();
    }

    /**
     * Reset required_slots in contexts. This is called after Nereids planner do the projection.
     * In the projection process, some slots may be removed. So call this to update the slots info.
     */
    @Override
    public void updateRequiredSlots(PlanTranslatorContext planTranslatorContext,
            Set<SlotId> requiredByProjectSlotIdSet) throws UserException {
        params.unsetRequiredSlots();
        for (SlotDescriptor slot : desc.getSlots()) {
            if (!slot.isMaterialized()) {
                continue;
            }

            TFileScanSlotInfo slotInfo = new TFileScanSlotInfo();
            slotInfo.setSlotId(slot.getId().asInt());
            slotInfo.setIsFileSlot(!scanProvider.getPathPartitionKeys().contains(slot.getColumn().getName()));
            params.addToRequiredSlots(slotInfo);
        }
    }

    private void initHMSExternalTable(HMSExternalTable hmsTable) throws UserException {
        Preconditions.checkNotNull(hmsTable);

        if (hmsTable.isView()) {
            throw new AnalysisException(
                    String.format("Querying external view '[%s].%s.%s' is not supported", hmsTable.getDlaType(),
                            hmsTable.getDbName(), hmsTable.getName()));
        }

        switch (hmsTable.getDlaType()) {
            case HUDI:
                scanProvider = new HudiScanProvider(hmsTable, desc, columnNameToRange);
                break;
            case ICEBERG:
                IcebergSource hmsSource = new IcebergHMSSource(hmsTable, desc, columnNameToRange);
                scanProvider = new IcebergScanProvider(hmsSource, analyzer);
                break;
            case HIVE:
                String inputFormat = hmsTable.getRemoteTable().getSd().getInputFormat();
                if (inputFormat.contains("TextInputFormat")) {
                    for (SlotDescriptor slot : desc.getSlots()) {
                        if (!slot.getType().isScalarType()) {
                            throw new UserException("For column `" + slot.getColumn().getName()
                                    + "`, The column types ARRAY/MAP/STRUCT are not supported yet"
                                    + " for text input format of Hive. ");
                        }
                    }
                }
                scanProvider = new HiveScanProvider(hmsTable, desc, columnNameToRange);
                break;
            default:
                throw new UserException("Unknown table type: " + hmsTable.getDlaType());
        }
    }

    private void initIcebergExternalTable(IcebergExternalTable icebergTable) throws UserException {
        Preconditions.checkNotNull(icebergTable);
        if (icebergTable.isView()) {
            throw new AnalysisException(
                String.format("Querying external view '%s.%s' is not supported", icebergTable.getDbName(),
                        icebergTable.getName()));
        }

        String catalogType = icebergTable.getIcebergCatalogType();
        switch (catalogType) {
            case IcebergExternalCatalog.ICEBERG_HMS:
            case IcebergExternalCatalog.ICEBERG_REST:
            case IcebergExternalCatalog.ICEBERG_DLF:
            case IcebergExternalCatalog.ICEBERG_GLUE:
                IcebergSource icebergSource = new IcebergApiSource(
                        icebergTable, desc, columnNameToRange);
                scanProvider = new IcebergScanProvider(icebergSource, analyzer);
                break;
            default:
                throw new UserException("Unknown iceberg catalog type: " + catalogType);
        }
    }

    private void initFunctionGenTable(FunctionGenTable table, ExternalFileTableValuedFunction tvf) {
        Preconditions.checkNotNull(table);
        scanProvider = new TVFScanProvider(table, desc, tvf);
    }

    // Create a corresponding TFileScanRangeParams
    private void initScanRangeParams() throws UserException {
        Preconditions.checkNotNull(desc);
        destSlotDescByName = Maps.newHashMap();
        for (SlotDescriptor slot : desc.getSlots()) {
            destSlotDescByName.put(slot.getColumn().getName(), slot);
        }
        params = new TFileScanRangeParams();
        params.setDestTupleId(desc.getId().asInt());
        List<String> partitionKeys = scanProvider.getPathPartitionKeys();
        List<Column> columns = desc.getTable().getBaseSchema(false);
        params.setNumOfColumnsFromFile(columns.size() - partitionKeys.size());
        for (SlotDescriptor slot : desc.getSlots()) {
            if (!slot.isMaterialized()) {
                continue;
            }
            TFileScanSlotInfo slotInfo = new TFileScanSlotInfo();
            slotInfo.setSlotId(slot.getId().asInt());
            slotInfo.setIsFileSlot(!partitionKeys.contains(slot.getColumn().getName()));
            params.addToRequiredSlots(slotInfo);
        }
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        setDefaultValueExprs();
        setColumnPositionMappingForTextFile();
        params.setSrcTupleId(-1);
        createScanRangeLocations(conjuncts, params, scanProvider);
        this.inputSplitsNum += scanProvider.getInputSplitNum();
        this.totalFileSize += scanProvider.getInputFileSize();
        TableIf table = desc.getTable();
        if (table instanceof HMSExternalTable) {
            if (((HMSExternalTable) table).getDlaType().equals(HMSExternalTable.DLAType.HIVE)) {
                genSlotToSchemaIdMap();
            }
        }
        if (scanProvider instanceof HiveScanProvider) {
            this.totalPartitionNum = ((HiveScanProvider) scanProvider).getTotalPartitionNum();
            this.readPartitionNum = ((HiveScanProvider) scanProvider).getReadPartitionNum();
        }
    }

    @Override
    public void finalizeForNereids() throws UserException {
        setDefaultValueExprs();
        setColumnPositionMappingForTextFile();
        params.setSrcTupleId(-1);
        createScanRangeLocations(conjuncts, params, scanProvider);
        this.inputSplitsNum += scanProvider.getInputSplitNum();
        this.totalFileSize += scanProvider.getInputFileSize();
        TableIf table = desc.getTable();
        if (table instanceof HMSExternalTable) {
            if (((HMSExternalTable) table).getDlaType().equals(HMSExternalTable.DLAType.HIVE)) {
                genSlotToSchemaIdMap();
            }
        }
        if (scanProvider instanceof HiveScanProvider) {
            this.totalPartitionNum = ((HiveScanProvider) scanProvider).getTotalPartitionNum();
            this.readPartitionNum = ((HiveScanProvider) scanProvider).getReadPartitionNum();
        }
    }

    private void setColumnPositionMappingForTextFile()
            throws UserException {
        TableIf tbl = scanProvider.getTargetTable();
        List<Integer> columnIdxs = Lists.newArrayList();

        for (TFileScanSlotInfo slot : params.getRequiredSlots()) {
            if (!slot.isIsFileSlot()) {
                continue;
            }
            SlotDescriptor slotDesc = desc.getSlot(slot.getSlotId());
            String colName = slotDesc.getColumn().getName();
            int idx = tbl.getBaseColumnIdxByName(colName);
            if (idx == -1) {
                throw new UserException("Column " + colName + " not found in table " + tbl.getName());
            }
            columnIdxs.add(idx);
        }
        params.setColumnIdxs(columnIdxs);
    }

    protected void setDefaultValueExprs()
            throws UserException {
        TableIf tbl = scanProvider.getTargetTable();
        Preconditions.checkNotNull(tbl);
        TExpr tExpr = new TExpr();
        tExpr.setNodes(Lists.newArrayList());

        for (Column column : tbl.getBaseSchema()) {
            Expr expr;
            if (column.getDefaultValue() != null) {
                if (column.getDefaultValueExprDef() != null) {
                    expr = column.getDefaultValueExpr();
                } else {
                    expr = new StringLiteral(column.getDefaultValue());
                }
            } else {
                if (column.isAllowNull()) {
                    expr = NullLiteral.create(column.getType());
                } else {
                    expr = null;
                }
            }
            SlotDescriptor slotDesc = destSlotDescByName.get(column.getName());
            // if slot desc is null, which mean it is an unrelated slot, just skip.
            // eg:
            // (a, b, c) set (x=a, y=b, z=c)
            // c does not exist in file, the z will be filled with null, even if z has default value.
            // and if z is not nullable, the load will fail.
            if (slotDesc != null) {
                if (expr != null) {
                    expr = castToSlot(slotDesc, expr);
                    params.putToDefaultValueOfSrcSlot(slotDesc.getId().asInt(), expr.treeToThrift());
                } else {
                    params.putToDefaultValueOfSrcSlot(slotDesc.getId().asInt(), tExpr);
                }
            }
        }
    }

    private void genSlotToSchemaIdMap() {
        List<Column> baseSchema = desc.getTable().getBaseSchema();
        Map<String, Integer> columnNameToPosition = Maps.newHashMap();
        for (SlotDescriptor slot : desc.getSlots()) {
            int idx = 0;
            for (Column col : baseSchema) {
                if (col.getName().equals(slot.getColumn().getName())) {
                    columnNameToPosition.put(col.getName(), idx);
                    break;
                }
                idx += 1;
            }
        }
        params.setSlotNameToSchemaPos(columnNameToPosition);
    }
}
