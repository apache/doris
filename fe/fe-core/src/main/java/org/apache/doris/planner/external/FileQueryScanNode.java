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
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.ExternalTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.external.iceberg.IcebergScanNode;
import org.apache.doris.planner.external.iceberg.IcebergSplit;
import org.apache.doris.planner.external.paimon.PaimonScanNode;
import org.apache.doris.planner.external.paimon.PaimonSplit;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.system.Backend;
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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * FileQueryScanNode for querying the file access type of catalog, now only support
 * hive,hudi, iceberg and TVF.
 */
public abstract class FileQueryScanNode extends FileScanNode {
    private static final Logger LOG = LogManager.getLogger(FileQueryScanNode.class);

    protected Map<String, SlotDescriptor> destSlotDescByName;
    protected TFileScanRangeParams params;

    protected int inputSplitNum = 0;
    protected long inputFileSize = 0;

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
        super.init(analyzer);
        doInitialize();
    }

    /**
     * Init ExternalFileScanNode, ONLY used for Nereids. Should NOT use this function in anywhere else.
     */
    public void init() throws UserException {
        doInitialize();
    }

    // Init scan provider and schema related params.
    protected void doInitialize() throws UserException {
        Preconditions.checkNotNull(desc);
        ExternalTable table = (ExternalTable) desc.getTable();
        if (table.isView()) {
            throw new AnalysisException(
                String.format("Querying external view '%s.%s' is not supported", table.getDbName(), table.getName()));
        }
        computeColumnFilter();
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
        setColumnPositionMappingForTextFile();
        // For query, set src tuple id to -1.
        params.setSrcTupleId(-1);
    }

    protected void initBackendPolicy() throws UserException {
        backendPolicy.init();
        numNodes = backendPolicy.numBackends();
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
            slotInfo.setIsFileSlot(!getPathPartitionKeys().contains(slot.getColumn().getName()));
            params.addToRequiredSlots(slotInfo);
        }
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
        createScanRangeLocations();
    }

    private void setColumnPositionMappingForTextFile()
            throws UserException {
        TableIf tbl = getTargetTable();
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

    public void createScanRangeLocations() throws UserException {
        long start = System.currentTimeMillis();
        List<Split> inputSplits = getSplits();
        this.inputSplitNum = inputSplits.size();
        if (inputSplits.isEmpty()) {
            return;
        }
        FileSplit inputSplit = (FileSplit) inputSplits.get(0);
        TFileType locationType = getLocationType();
        params.setFileType(locationType);
        TFileFormatType fileFormatType = getFileFormatType();
        params.setFormatType(fileFormatType);
        TFileCompressType fileCompressType = getFileCompressType(inputSplit);
        params.setCompressType(fileCompressType);
        if (Util.isCsvFormat(fileFormatType) || fileFormatType == TFileFormatType.FORMAT_JSON) {
            params.setFileAttributes(getFileAttributes());
        }

        // set hdfs params for hdfs file type.
        Map<String, String> locationProperties = getLocationProperties();
        if (locationType == TFileType.FILE_HDFS || locationType == TFileType.FILE_BROKER) {
            String fsName = getFsName(inputSplit);
            THdfsParams tHdfsParams = HdfsResource.generateHdfsParam(locationProperties);
            tHdfsParams.setFsName(fsName);
            params.setHdfsParams(tHdfsParams);

            if (locationType == TFileType.FILE_BROKER) {
                FsBroker broker = Env.getCurrentEnv().getBrokerMgr().getAnyAliveBroker();
                if (broker == null) {
                    throw new UserException("No alive broker.");
                }
                params.addToBrokerAddresses(new TNetworkAddress(broker.ip, broker.port));
            }
        } else if (locationType == TFileType.FILE_S3) {
            params.setProperties(locationProperties);
        }

        List<String> pathPartitionKeys = getPathPartitionKeys();
        for (Split split : inputSplits) {
            TScanRangeLocations curLocations = newLocations(params, backendPolicy);
            FileSplit fileSplit = (FileSplit) split;

            // If fileSplit has partition values, use the values collected from hive partitions.
            // Otherwise, use the values in file path.
            List<String> partitionValuesFromPath = fileSplit.getPartitionValues() == null
                    ? BrokerUtil.parseColumnsFromPath(fileSplit.getPath().toString(), pathPartitionKeys, false)
                    : fileSplit.getPartitionValues();

            TFileRangeDesc rangeDesc = createFileRangeDesc(fileSplit, partitionValuesFromPath, pathPartitionKeys);
            // external data lake table
            if (fileSplit instanceof IcebergSplit) {
                IcebergScanNode.setIcebergParams(rangeDesc, (IcebergSplit) fileSplit);
            } else if (fileSplit instanceof PaimonSplit) {
                PaimonScanNode.setPaimonParams(rangeDesc, (PaimonSplit) fileSplit);
            }

            curLocations.getScanRange().getExtScanRange().getFileScanRange().addToRanges(rangeDesc);
            LOG.debug("assign to backend {} with table split: {} ({}, {}), location: {}",
                    curLocations.getLocations().get(0).getBackendId(), fileSplit.getPath(), fileSplit.getStart(),
                    fileSplit.getLength(), Joiner.on("|").join(fileSplit.getHosts()));
            scanRangeLocations.add(curLocations);
            this.inputFileSize += fileSplit.getLength();
        }
        LOG.debug("create #{} ScanRangeLocations cost: {} ms",
                scanRangeLocations.size(), (System.currentTimeMillis() - start));
    }

    private TScanRangeLocations newLocations(TFileScanRangeParams params, FederationBackendPolicy backendPolicy) {
        // Generate on file scan range
        TFileScanRange fileScanRange = new TFileScanRange();
        fileScanRange.setParams(params);

        // Scan range
        TExternalScanRange externalScanRange = new TExternalScanRange();
        externalScanRange.setFileScanRange(fileScanRange);
        TScanRange scanRange = new TScanRange();
        scanRange.setExtScanRange(externalScanRange);

        // Locations
        TScanRangeLocations locations = new TScanRangeLocations();
        locations.setScanRange(scanRange);

        TScanRangeLocation location = new TScanRangeLocation();
        Backend selectedBackend = backendPolicy.getNextBe();
        location.setBackendId(selectedBackend.getId());
        location.setServer(new TNetworkAddress(selectedBackend.getHost(), selectedBackend.getBePort()));
        locations.addToLocations(location);

        return locations;
    }

    private TFileRangeDesc createFileRangeDesc(FileSplit fileSplit, List<String> columnsFromPath,
                                               List<String> columnsFromPathKeys)
            throws UserException {
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        rangeDesc.setStartOffset(fileSplit.getStart());
        rangeDesc.setSize(fileSplit.getLength());
        // fileSize only be used when format is orc or parquet and TFileType is broker
        // When TFileType is other type, it is not necessary
        rangeDesc.setFileSize(fileSplit.getFileLength());
        rangeDesc.setColumnsFromPath(columnsFromPath);
        rangeDesc.setColumnsFromPathKeys(columnsFromPathKeys);

        if (getLocationType() == TFileType.FILE_HDFS) {
            rangeDesc.setPath(fileSplit.getPath().toUri().getPath());
        } else if (getLocationType() == TFileType.FILE_S3 || getLocationType() == TFileType.FILE_BROKER) {
            // need full path
            rangeDesc.setPath(fileSplit.getPath().toString());
        }
        rangeDesc.setModificationTime(fileSplit.getModificationTime());
        return rangeDesc;
    }

    protected TFileType getLocationType() throws UserException {
        throw new NotImplementedException("");
    }

    protected TFileFormatType getFileFormatType() throws UserException {
        throw new NotImplementedException("");
    }

    protected TFileCompressType getFileCompressType(FileSplit fileSplit) throws UserException {
        return Util.getFileCompressType(fileSplit.getPath().toString());
    }

    protected TFileAttributes getFileAttributes() throws UserException {
        throw new NotImplementedException("");
    }

    protected List<String> getPathPartitionKeys() throws UserException {
        throw new NotImplementedException("");
    }

    protected TableIf getTargetTable() throws UserException {
        throw new NotImplementedException("");
    }

    protected Map<String, String> getLocationProperties() throws UserException  {
        throw new NotImplementedException("");
    }

    // eg: hdfs://namenode  s3://buckets
    protected String getFsName(FileSplit split) {
        String fullPath = split.getPath().toUri().toString();
        String filePath = split.getPath().toUri().getPath();
        return fullPath.replace(filePath, "");
    }

    protected static Optional<TFileType> getTFileType(String location) {
        if (location != null && !location.isEmpty()) {
            if (FeConstants.isObjStorage(location)) {
                return Optional.of(TFileType.FILE_S3);
            } else if (location.startsWith(FeConstants.FS_PREFIX_HDFS)) {
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
