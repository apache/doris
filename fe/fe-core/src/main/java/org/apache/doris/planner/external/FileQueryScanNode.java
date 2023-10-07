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
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TableSample;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.FunctionGenTable;
import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.ExternalTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.common.util.S3Util;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.hive.AcidInfo;
import org.apache.doris.datasource.hive.AcidInfo.DeleteDeltaInfo;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.external.hudi.HudiScanNode;
import org.apache.doris.planner.external.hudi.HudiSplit;
import org.apache.doris.planner.external.iceberg.IcebergScanNode;
import org.apache.doris.planner.external.iceberg.IcebergSplit;
import org.apache.doris.planner.external.paimon.PaimonScanNode;
import org.apache.doris.planner.external.paimon.PaimonSplit;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.system.Backend;
import org.apache.doris.tablefunction.ExternalFileTableValuedFunction;
import org.apache.doris.thrift.TExternalScanRange;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileScanSlotInfo;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.THdfsParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TTableFormatFileDesc;
import org.apache.doris.thrift.TTextSerdeType;
import org.apache.doris.thrift.TTransactionalHiveDeleteDeltaDesc;
import org.apache.doris.thrift.TTransactionalHiveDesc;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * FileQueryScanNode for querying the file access type of catalog, now only support
 * hive, hudi, iceberg and TVF.
 */
public abstract class FileQueryScanNode extends FileScanNode {
    private static final Logger LOG = LogManager.getLogger(FileQueryScanNode.class);

    protected Map<String, SlotDescriptor> destSlotDescByName;
    protected TFileScanRangeParams params;

    protected TableSample tableSample;

    /**
     * External file scan node for Query hms table
     * needCheckColumnPriv: Some of ExternalFileScanNode do not need to check column priv
     * eg: s3 tvf
     * These scan nodes do not have corresponding catalog/database/table info, so no need to do priv check
     */
    public FileQueryScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName,
                             StatisticalType statisticalType, boolean needCheckColumnPriv) {
        super(id, desc, planNodeName, statisticalType, needCheckColumnPriv);
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        if (ConnectContext.get().getExecutor() != null) {
            ConnectContext.get().getExecutor().getSummaryProfile().setInitScanNodeStartTime();
        }
        super.init(analyzer);
        doInitialize();
        if (ConnectContext.get().getExecutor() != null) {
            ConnectContext.get().getExecutor().getSummaryProfile().setInitScanNodeFinishTime();
        }
    }

    /**
     * Init ExternalFileScanNode, ONLY used for Nereids. Should NOT use this function in anywhere else.
     */
    @Override
    public void init() throws UserException {
        if (ConnectContext.get().getExecutor() != null) {
            ConnectContext.get().getExecutor().getSummaryProfile().setInitScanNodeStartTime();
        }
        doInitialize();
        if (ConnectContext.get().getExecutor() != null) {
            ConnectContext.get().getExecutor().getSummaryProfile().setInitScanNodeFinishTime();
        }
    }

    // Init scan provider and schema related params.
    protected void doInitialize() throws UserException {
        Preconditions.checkNotNull(desc);
        if (desc.getTable() instanceof ExternalTable) {
            ExternalTable table = (ExternalTable) desc.getTable();
            if (table.isView()) {
                throw new AnalysisException(
                        String.format("Querying external view '%s.%s' is not supported", table.getDbName(),
                                table.getName()));
            }
        }
        computeColumnsFilter();
        initBackendPolicy();
        initSchemaParams();
    }

    // Init schema (Tuple/Slot) related params.
    protected void initSchemaParams() throws UserException {
        destSlotDescByName = Maps.newHashMap();
        for (SlotDescriptor slot : desc.getSlots()) {
            destSlotDescByName.put(slot.getColumn().getName(), slot);
        }
        params = new TFileScanRangeParams();
        if (this instanceof HiveScanNode) {
            params.setTextSerdeType(TTextSerdeType.HIVE_TEXT_SERDE);
        }
        params.setDestTupleId(desc.getId().asInt());
        List<String> partitionKeys = getPathPartitionKeys();
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
        setDefaultValueExprs(getTargetTable(), destSlotDescByName, params, false);
        setColumnPositionMapping();
        // For query, set src tuple id to -1.
        params.setSrcTupleId(-1);
    }

    /**
     * Reset required_slots in contexts. This is called after Nereids planner do the projection.
     * In the projection process, some slots may be removed. So call this to update the slots info.
     */
    @Override
    public void updateRequiredSlots(PlanTranslatorContext planTranslatorContext,
            Set<SlotId> requiredByProjectSlotIdSet) throws UserException {
        updateRequiredSlots();
    }

    private void updateRequiredSlots() throws UserException {
        params.unsetRequiredSlots();
        for (SlotDescriptor slot : desc.getSlots()) {
            if (!slot.isMaterialized()) {
                continue;
            }

            TFileScanSlotInfo slotInfo = new TFileScanSlotInfo();
            slotInfo.setSlotId(slot.getId().asInt());
            slotInfo.setIsFileSlot(!getPathPartitionKeys().contains(slot.getColumn().getName()));
            params.addToRequiredSlots(slotInfo);
        }
        // Update required slots and column_idxs in scanRangeLocations.
        setColumnPositionMapping();
    }

    public void setTableSample(TableSample tSample) {
        this.tableSample = tSample;
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        doFinalize();
    }

    @Override
    public void finalizeForNereids() throws UserException {
        doFinalize();
    }

    // Create scan range locations and the statistics.
    protected void doFinalize() throws UserException {
        if (ConnectContext.get().getExecutor() != null) {
            ConnectContext.get().getExecutor().getSummaryProfile().setFinalizeScanNodeStartTime();
        }
        createScanRangeLocations();
        updateRequiredSlots();
        if (ConnectContext.get().getExecutor() != null) {
            ConnectContext.get().getExecutor().getSummaryProfile().setFinalizeScanNodeFinishTime();
        }
    }

    private void setColumnPositionMapping()
            throws UserException {
        TableIf tbl = getTargetTable();
        List<Integer> columnIdxs = Lists.newArrayList();
        // avoid null pointer, it maybe has no slots when two tables are joined
        if (params.getRequiredSlots() == null) {
            params.setColumnIdxs(columnIdxs);
            return;
        }
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

    public TFileScanRangeParams getFileScanRangeParams() {
        return params;
    }

    @Override
    public void createScanRangeLocations() throws UserException {
        long start = System.currentTimeMillis();
        if (ConnectContext.get().getExecutor() != null) {
            ConnectContext.get().getExecutor().getSummaryProfile().setGetSplitsStartTime();
        }
        List<Split> inputSplits = getSplits();
        if (ConnectContext.get().getExecutor() != null) {
            ConnectContext.get().getExecutor().getSummaryProfile().setGetSplitsFinishTime();
        }
        this.inputSplitsNum = inputSplits.size();
        if (inputSplits.isEmpty() && !(getLocationType() == TFileType.FILE_STREAM)) {
            return;
        }
        TFileFormatType fileFormatType = getFileFormatType();
        params.setFormatType(fileFormatType);
        boolean isCsvOrJson = Util.isCsvFormat(fileFormatType) || fileFormatType == TFileFormatType.FORMAT_JSON;
        boolean isWal = fileFormatType == TFileFormatType.FORMAT_WAL;
        if (isCsvOrJson || isWal) {
            params.setFileAttributes(getFileAttributes());
            if (getLocationType() == TFileType.FILE_STREAM) {
                params.setFileType(TFileType.FILE_STREAM);
                FunctionGenTable table = (FunctionGenTable) this.desc.getTable();
                ExternalFileTableValuedFunction tableValuedFunction = (ExternalFileTableValuedFunction) table.getTvf();
                params.setCompressType(tableValuedFunction.getTFileCompressType());

                TScanRangeLocations curLocations = newLocations();
                TFileRangeDesc rangeDesc = new TFileRangeDesc();
                rangeDesc.setLoadId(ConnectContext.get().queryId());
                rangeDesc.setSize(-1);
                rangeDesc.setFileSize(-1);
                curLocations.getScanRange().getExtScanRange().getFileScanRange().addToRanges(rangeDesc);
                curLocations.getScanRange().getExtScanRange().getFileScanRange().setParams(params);

                TScanRangeLocation location = new TScanRangeLocation();
                long backendId = ConnectContext.get().getBackendId();
                Backend backend = Env.getCurrentSystemInfo().getIdToBackend().get(backendId);
                location.setBackendId(backendId);
                location.setServer(new TNetworkAddress(backend.getHost(), backend.getBePort()));
                curLocations.addToLocations(location);
                scanRangeLocations.add(curLocations);
                return;
            }
        }

        Map<String, String> locationProperties = getLocationProperties();
        // for JNI, only need to set properties
        if (fileFormatType == TFileFormatType.FORMAT_JNI) {
            params.setProperties(locationProperties);
        }

        boolean enableSqlCache = ConnectContext.get().getSessionVariable().enableFileCache;
        boolean enableShortCircuitRead = HdfsResource.enableShortCircuitRead(locationProperties);
        List<String> pathPartitionKeys = getPathPartitionKeys();
        for (Split split : inputSplits) {
            FileSplit fileSplit = (FileSplit) split;
            TFileType locationType = getLocationType(fileSplit.getPath().toString());
            setLocationPropertiesIfNecessary(locationType, locationProperties);

            TScanRangeLocations curLocations = newLocations();
            // If fileSplit has partition values, use the values collected from hive partitions.
            // Otherwise, use the values in file path.
            boolean isACID = false;
            if (fileSplit instanceof HiveSplit) {
                HiveSplit hiveSplit = (HiveSplit) split;
                isACID = hiveSplit.isACID();
            }
            List<String> partitionValuesFromPath = fileSplit.getPartitionValues() == null
                    ? BrokerUtil.parseColumnsFromPath(fileSplit.getPath().toString(), pathPartitionKeys, false, isACID)
                    : fileSplit.getPartitionValues();

            TFileRangeDesc rangeDesc = createFileRangeDesc(fileSplit, partitionValuesFromPath, pathPartitionKeys,
                    locationType);
            TFileCompressType fileCompressType = getFileCompressType(fileSplit);
            rangeDesc.setCompressType(fileCompressType);
            if (isACID) {
                HiveSplit hiveSplit = (HiveSplit) split;
                hiveSplit.setTableFormatType(TableFormatType.TRANSACTIONAL_HIVE);
                TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
                tableFormatFileDesc.setTableFormatType(hiveSplit.getTableFormatType().value());
                AcidInfo acidInfo = (AcidInfo) hiveSplit.getInfo();
                TTransactionalHiveDesc transactionalHiveDesc = new TTransactionalHiveDesc();
                transactionalHiveDesc.setPartition(acidInfo.getPartitionLocation());
                List<TTransactionalHiveDeleteDeltaDesc> deleteDeltaDescs = new ArrayList<>();
                for (DeleteDeltaInfo deleteDeltaInfo : acidInfo.getDeleteDeltas()) {
                    TTransactionalHiveDeleteDeltaDesc deleteDeltaDesc = new TTransactionalHiveDeleteDeltaDesc();
                    deleteDeltaDesc.setDirectoryLocation(deleteDeltaInfo.getDirectoryLocation());
                    deleteDeltaDesc.setFileNames(deleteDeltaInfo.getFileNames());
                    deleteDeltaDescs.add(deleteDeltaDesc);
                }
                transactionalHiveDesc.setDeleteDeltas(deleteDeltaDescs);
                tableFormatFileDesc.setTransactionalHiveParams(transactionalHiveDesc);
                rangeDesc.setTableFormatParams(tableFormatFileDesc);
            }

            // external data lake table
            if (fileSplit instanceof IcebergSplit) {
                // TODO: extract all data lake split to factory
                IcebergScanNode.setIcebergParams(rangeDesc, (IcebergSplit) fileSplit);
            } else if (fileSplit instanceof PaimonSplit) {
                PaimonScanNode.setPaimonParams(rangeDesc, (PaimonSplit) fileSplit);
            } else if (fileSplit instanceof HudiSplit) {
                HudiScanNode.setHudiParams(rangeDesc, (HudiSplit) fileSplit);
            }

            curLocations.getScanRange().getExtScanRange().getFileScanRange().addToRanges(rangeDesc);
            TScanRangeLocation location = new TScanRangeLocation();
            Backend selectedBackend;
            if (enableSqlCache) {
                // Use consistent hash to assign the same scan range into the same backend among different queries
                selectedBackend = backendPolicy.getNextConsistentBe(curLocations);
            } else if (enableShortCircuitRead) {
                // Try to find a local BE if enable hdfs short circuit read
                selectedBackend = backendPolicy.getNextLocalBe(Arrays.asList(fileSplit.getHosts()));
            } else {
                selectedBackend = backendPolicy.getNextBe();
            }
            location.setBackendId(selectedBackend.getId());
            location.setServer(new TNetworkAddress(selectedBackend.getHost(), selectedBackend.getBePort()));
            curLocations.addToLocations(location);
            LOG.debug("assign to backend {} with table split: {} ({}, {}), location: {}",
                    curLocations.getLocations().get(0).getBackendId(), fileSplit.getPath(), fileSplit.getStart(),
                    fileSplit.getLength(), Joiner.on("|").join(fileSplit.getHosts()));
            scanRangeLocations.add(curLocations);
            this.totalFileSize += fileSplit.getLength();
        }
        if (ConnectContext.get().getExecutor() != null) {
            ConnectContext.get().getExecutor().getSummaryProfile().setCreateScanRangeFinishTime();
        }
        LOG.debug("create #{} ScanRangeLocations cost: {} ms",
                scanRangeLocations.size(), (System.currentTimeMillis() - start));
    }

    private void setLocationPropertiesIfNecessary(TFileType locationType,
            Map<String, String> locationProperties) throws UserException {
        if (locationType == TFileType.FILE_HDFS || locationType == TFileType.FILE_BROKER) {
            if (!params.isSetHdfsParams()) {
                THdfsParams tHdfsParams = HdfsResource.generateHdfsParam(locationProperties);
                // tHdfsParams.setFsName(getFsName(fileSplit));
                params.setHdfsParams(tHdfsParams);
            }

            if (locationType == TFileType.FILE_BROKER) {
                params.setProperties(locationProperties);

                if (!params.isSetBrokerAddresses()) {
                    FsBroker broker = Env.getCurrentEnv().getBrokerMgr().getAnyAliveBroker();
                    if (broker == null) {
                        throw new UserException("No alive broker.");
                    }
                    params.addToBrokerAddresses(new TNetworkAddress(broker.host, broker.port));
                }
            }
        } else if ((locationType == TFileType.FILE_S3 || locationType == TFileType.FILE_LOCAL)
                && !params.isSetProperties()) {
            params.setProperties(locationProperties);
        }

        if (!params.isSetFileType()) {
            params.setFileType(locationType);
        }
    }

    private TScanRangeLocations newLocations() {
        // Generate on file scan range
        TFileScanRange fileScanRange = new TFileScanRange();
        // Scan range
        TExternalScanRange externalScanRange = new TExternalScanRange();
        externalScanRange.setFileScanRange(fileScanRange);
        TScanRange scanRange = new TScanRange();
        scanRange.setExtScanRange(externalScanRange);

        // Locations
        TScanRangeLocations locations = new TScanRangeLocations();
        locations.setScanRange(scanRange);
        return locations;
    }

    private TFileRangeDesc createFileRangeDesc(FileSplit fileSplit, List<String> columnsFromPath,
                                               List<String> columnsFromPathKeys, TFileType locationType)
            throws UserException {
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        rangeDesc.setStartOffset(fileSplit.getStart());
        rangeDesc.setSize(fileSplit.getLength());
        // fileSize only be used when format is orc or parquet and TFileType is broker
        // When TFileType is other type, it is not necessary
        rangeDesc.setFileSize(fileSplit.getFileLength());
        rangeDesc.setColumnsFromPath(columnsFromPath);
        rangeDesc.setColumnsFromPathKeys(columnsFromPathKeys);

        rangeDesc.setFileType(locationType);
        rangeDesc.setPath(fileSplit.getPath().toString());
        if (locationType == TFileType.FILE_HDFS) {
            URI fileUri = fileSplit.getPath().toUri();
            rangeDesc.setFsName(fileUri.getScheme() + "://" + fileUri.getAuthority());
        }
        rangeDesc.setModificationTime(fileSplit.getModificationTime());
        return rangeDesc;
    }

    protected abstract TFileType getLocationType() throws UserException;

    protected abstract TFileType getLocationType(String location) throws UserException;

    protected abstract TFileFormatType getFileFormatType() throws UserException;

    protected TFileCompressType getFileCompressType(FileSplit fileSplit) throws UserException {
        return Util.inferFileCompressTypeByPath(fileSplit.getPath().toString());
    }

    protected TFileAttributes getFileAttributes() throws UserException {
        throw new NotImplementedException("");
    }

    protected abstract List<String> getPathPartitionKeys() throws UserException;

    protected abstract TableIf getTargetTable() throws UserException;

    protected abstract Map<String, String> getLocationProperties() throws UserException;

    protected static Optional<TFileType> getTFileType(String location) {
        if (location != null && !location.isEmpty()) {
            if (S3Util.isObjStorage(location)) {
                if (S3Util.isHdfsOnOssEndpoint(location)) {
                    // if hdfs service is enabled on oss, use hdfs lib to access oss.
                    return Optional.of(TFileType.FILE_HDFS);
                }
                return Optional.of(TFileType.FILE_S3);
            } else if (location.startsWith(FeConstants.FS_PREFIX_HDFS)) {
                return Optional.of(TFileType.FILE_HDFS);
            } else if (location.startsWith(FeConstants.FS_PREFIX_VIEWFS)) {
                return Optional.of(TFileType.FILE_HDFS);
            } else if (location.startsWith(FeConstants.FS_PREFIX_COSN)) {
                return Optional.of(TFileType.FILE_HDFS);
            } else if (location.startsWith(FeConstants.FS_PREFIX_FILE)) {
                return Optional.of(TFileType.FILE_LOCAL);
            } else if (location.startsWith(FeConstants.FS_PREFIX_OFS)) {
                return Optional.of(TFileType.FILE_BROKER);
            } else if (location.startsWith(FeConstants.FS_PREFIX_GFS)) {
                return Optional.of(TFileType.FILE_BROKER);
            } else if (location.startsWith(FeConstants.FS_PREFIX_JFS)) {
                return Optional.of(TFileType.FILE_BROKER);
            }
        }
        return Optional.empty();
    }
}


