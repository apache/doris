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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.common.util.FileFormatConstants;
import org.apache.doris.common.util.FileFormatUtils;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.property.fileformat.CsvFileFormatProperties;
import org.apache.doris.datasource.property.fileformat.FileFormatProperties;
import org.apache.doris.datasource.property.fileformat.TextFileFormatProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.datasource.tvf.source.TVFScanNode;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.NotSupportedException;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PFetchTableSchemaRequest;
import org.apache.doris.proto.Types.PScalarType;
import org.apache.doris.proto.Types.PStructField;
import org.apache.doris.proto.Types.PTypeDesc;
import org.apache.doris.proto.Types.PTypeNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.THdfsParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * ExternalFileTableValuedFunction is used for S3/HDFS/LOCAL/HTTP_STREAM/GROUP_COMMIT table-valued-function
 */
public abstract class ExternalFileTableValuedFunction extends TableValuedFunctionIf {
    public static final Logger LOG = LogManager.getLogger(ExternalFileTableValuedFunction.class);
    protected static final String URI_KEY = "uri";

    public static final String PROP_TABLE_ID = "table_id";

    // Columns got from file and path(if has)
    protected List<Column> columns = null;
    // User specified csv columns, it will override columns got from file
    private final List<Column> csvSchema = Lists.newArrayList();

    // Partition columns from path, e.g. /path/to/columnName=columnValue.
    private List<String> pathPartitionKeys;

    protected List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();
    protected Map<String, String> backendConnectProperties = Maps.newHashMap();
    protected StorageProperties storageProperties;
    // Processed parameters derived from user input; includes normalization and default value filling.
    Map<String, String> processedParams;
    protected String filePath;

    protected Optional<String> resourceName = Optional.empty();

    public FileFormatProperties fileFormatProperties;
    private long tableId;

    public abstract TFileType getTFileType();

    public abstract String getFilePath();

    public abstract BrokerDesc getBrokerDesc();

    public TFileFormatType getTFileFormatType() {
        return fileFormatProperties.getFileFormatType();
    }

    public TFileCompressType getTFileCompressType() {
        return fileFormatProperties.getCompressionType();
    }

    public Map<String, String> getBackendConnectProperties() {
        return backendConnectProperties;
    }

    public List<String> getPathPartitionKeys() {
        return pathPartitionKeys;
    }

    protected void parseFile() throws AnalysisException {
        long startAt = System.currentTimeMillis();
        String path = getFilePath();
        BrokerDesc brokerDesc = getBrokerDesc();
        try {
            BrokerUtil.parseFile(path, brokerDesc, fileStatuses);
        } catch (UserException e) {
            throw new AnalysisException("parse file failed, err: " + e.getMessage(), e);
        } finally {
            SummaryProfile profile = SummaryProfile.getSummaryProfile(ConnectContext.get());
            if (profile != null) {
                profile.addExternalTvfInitTime(System.currentTimeMillis() - startAt);
            }
        }
    }

    // The keys in properties map need to be lowercase.
    protected Map<String, String> parseCommonProperties(Map<String, String> properties) throws AnalysisException {
        Map<String, String> mergedProperties = Maps.newHashMap();
        if (properties.containsKey("resource")) {
            Resource resource = Env.getCurrentEnv().getResourceMgr().getResource(properties.get("resource"));
            if (resource == null) {
                throw new AnalysisException("Can not find resource: " + properties.get("resource"));
            }
            this.resourceName = Optional.of(properties.get("resource"));
            mergedProperties = resource.getCopiedProperties();
        }
        mergedProperties.putAll(properties);
        // Copy the properties, because we will remove the key from properties.
        Map<String, String> copiedProps = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        copiedProps.putAll(mergedProperties);

        tableId = Long.valueOf(getOrDefaultAndRemove(copiedProps, PROP_TABLE_ID, "-1"));

        String formatString = getOrDefaultAndRemove(copiedProps, FileFormatConstants.PROP_FORMAT, "").toLowerCase();
        fileFormatProperties = FileFormatProperties.createFileFormatProperties(formatString);
        fileFormatProperties.analyzeFileFormatProperties(copiedProps, true);

        if (fileFormatProperties instanceof CsvFileFormatProperties
                || fileFormatProperties instanceof TextFileFormatProperties) {
            FileFormatUtils.parseCsvSchema(csvSchema, getOrDefaultAndRemove(copiedProps,
                    CsvFileFormatProperties.PROP_CSV_SCHEMA, ""));
            if (LOG.isDebugEnabled()) {
                LOG.debug("get csv schema: {}", csvSchema);
            }
        }

        pathPartitionKeys = Optional.ofNullable(
                        getOrDefaultAndRemove(copiedProps, FileFormatConstants.PROP_PATH_PARTITION_KEYS, null))
                .map(str -> Arrays.stream(str.split(","))
                        .map(String::trim)
                        .collect(Collectors.toList()))
                .orElse(Lists.newArrayList());
        this.processedParams = new HashMap<>(copiedProps);
        return copiedProps;
    }

    protected String getOrDefaultAndRemove(Map<String, String> props, String key, String defaultValue) {
        String value = props.getOrDefault(key, defaultValue);
        props.remove(key);
        return value;
    }

    public List<TBrokerFileStatus> getFileStatuses() {
        return fileStatuses;
    }

    public TFileAttributes getFileAttributes() {
        return fileFormatProperties.toTFileAttributes();
    }

    @Override
    public ScanNode getScanNode(PlanNodeId id, TupleDescriptor desc, SessionVariable sv) {
        return new TVFScanNode(id, desc, false, sv);
    }

    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        if (!csvSchema.isEmpty()) {
            return csvSchema;
        }
        // if (FeConstants.runningUnitTest) {
        //     Object mockedUtObj = FeConstants.unitTestConstant;
        //     if (mockedUtObj instanceof List) {
        //         return ((List<Column>) mockedUtObj);
        //     }
        //     return new ArrayList<>();
        // }
        if (this.columns != null) {
            return columns;
        }
        // get one BE address
        columns = Lists.newArrayList();
        Backend be = getBackend();
        if (be == null) {
            throw new AnalysisException("No Alive backends");
        }

        if (fileFormatProperties.getFileFormatType() == TFileFormatType.FORMAT_WAL) {
            List<Column> fileColumns = new ArrayList<>();
            Table table = Env.getCurrentInternalCatalog().getTableByTableId(tableId);
            List<Column> tableColumns = table.getBaseSchema(true);
            for (int i = 0; i < tableColumns.size(); i++) {
                Column column = new Column(tableColumns.get(i).getName(), tableColumns.get(i).getType(), true);
                column.setUniqueId(tableColumns.get(i).getUniqueId());
                column.setIsAllowNull(true);
                fileColumns.add(column);
            }
            return fileColumns;
        }

        TNetworkAddress address = new TNetworkAddress(be.getHost(), be.getBrpcPort());
        try {
            PFetchTableSchemaRequest request = getFetchTableStructureRequest();
            InternalService.PFetchTableSchemaResult result = null;

            // `request == null` means we don't need to get schemas from BE,
            // and we fill a dummy col for this table.
            if (request != null) {
                Future<InternalService.PFetchTableSchemaResult> future = BackendServiceProxy.getInstance()
                        .fetchTableStructureAsync(address, request);

                result = future.get();
                TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
                String errMsg;
                if (code != TStatusCode.OK) {
                    if (!result.getStatus().getErrorMsgsList().isEmpty()) {
                        errMsg = result.getStatus().getErrorMsgsList().get(0);
                    } else {
                        errMsg = "fetchTableStructureAsync failed. backend address: "
                                + NetUtils
                                .getHostPortInAccessibleFormat(address.getHostname(), address.getPort());
                    }
                    throw new AnalysisException(errMsg);
                }
            }
            fillColumns(result);
        } catch (RpcException e) {
            throw new AnalysisException("fetchTableStructureResult rpc exception", e);
        } catch (InterruptedException e) {
            throw new AnalysisException("fetchTableStructureResult interrupted exception", e);
        } catch (ExecutionException e) {
            throw new AnalysisException("fetchTableStructureResult exception", e);
        } catch (TException e) {
            throw new AnalysisException("getFetchTableStructureRequest exception", e);
        }
        return columns;
    }

    protected Backend getBackend() {
        // For the http stream task, we should obtain the be for processing the task
        ImmutableMap<Long, Backend> beIdToBe;
        try {
            beIdToBe = Env.getCurrentSystemInfo().getBackendsByCurrentCluster();
        } catch (AnalysisException e) {
            LOG.warn("get backend failed, ", e);
            return null;
        }

        if (getTFileType() == TFileType.FILE_STREAM) {
            long backendId = ConnectContext.get().getBackendId();
            Backend be = beIdToBe.get(backendId);
            if (be == null || !be.isAlive()) {
                LOG.warn("Backend {} is not alive", backendId);
                return null;
            } else {
                return be;
            }
        }
        for (Backend be : beIdToBe.values()) {
            if (be.isAlive()) {
                return be;
            }
        }
        return null;
    }

    /**
     * Convert PTypeDesc into doris column type
     *
     * @param typeNodes list PTypeNodes in PTypeDesc
     * @param start the start index of typeNode to parse
     * @return column type and the number of parsed PTypeNodes
     */
    private Pair<Type, Integer> getColumnType(List<PTypeNode> typeNodes, int start) {
        PScalarType columnType = typeNodes.get(start).getScalarType();
        TPrimitiveType tPrimitiveType = TPrimitiveType.findByValue(columnType.getType());
        Type type;
        int parsedNodes;
        if (tPrimitiveType == TPrimitiveType.ARRAY) {
            Pair<Type, Integer> itemType = getColumnType(typeNodes, start + 1);
            type = ArrayType.create(itemType.key(), true);
            parsedNodes = 1 + itemType.value();
        } else if (tPrimitiveType == TPrimitiveType.MAP) {
            Pair<Type, Integer> keyType = getColumnType(typeNodes, start + 1);
            Pair<Type, Integer> valueType = getColumnType(typeNodes, start + 1 + keyType.value());
            type = new MapType(keyType.key(), valueType.key());
            parsedNodes = 1 + keyType.value() + valueType.value();
        } else if (tPrimitiveType == TPrimitiveType.STRUCT) {
            parsedNodes = 1;
            ArrayList<StructField> fields = new ArrayList<>();
            Set<String> fieldLowerNames = new HashSet<>();

            for (int i = 0; i < typeNodes.get(start).getStructFieldsCount(); ++i) {
                Pair<Type, Integer> fieldType = getColumnType(typeNodes, start + parsedNodes);
                PStructField structField = typeNodes.get(start).getStructFields(i);
                String fieldName = structField.getName().toLowerCase();
                if (fieldLowerNames.contains(fieldName)) {
                    throw new NotSupportedException("Repeated lowercase field names: " + fieldName);
                } else {
                    fieldLowerNames.add(fieldName);
                    fields.add(new StructField(fieldName, fieldType.key(), structField.getComment(),
                            structField.getContainsNull()));
                }
                parsedNodes += fieldType.value();
            }
            type = new StructType(fields);
        } else {
            type = ScalarType.createType(PrimitiveType.fromThrift(tPrimitiveType),
                    columnType.getLen(), columnType.getPrecision(), columnType.getScale());
            parsedNodes = 1;
        }
        return Pair.of(type, parsedNodes);
    }

    private void fillColumns(InternalService.PFetchTableSchemaResult result) {
        // `result == null` means we don't need to get schemas from BE,
        // and we fill a dummy col for this table.
        if (result == null) {
            columns.add(new Column("__dummy_col", ScalarType.createStringType(), true));
            return;
        }
        // add fetched file columns
        Set<String> columnLowerNames = new HashSet<>();
        for (int idx = 0; idx < result.getColumnNums(); ++idx) {
            PTypeDesc type = result.getColumnTypes(idx);
            String colName = result.getColumnNames(idx).toLowerCase();
            // Since doris does not distinguish between upper and lower case columns when querying, in order to avoid
            // query ambiguity, two columns with the same name but different capitalization are not allowed.
            if (columnLowerNames.contains(colName)) {
                throw new NotSupportedException("Repeated lowercase column names: " + colName);
            } else {
                columnLowerNames.add(colName);
                columns.add(new Column(colName, getColumnType(type.getTypesList(), 0).key(), true));
            }
        }
        // add path columns
        // HACK(tsy): path columns are all treated as STRING type now, after BE supports reading all columns
        //  types by all format readers from file meta, maybe reading path columns types from BE then.
        for (String colName : pathPartitionKeys) {
            columns.add(new Column(colName, ScalarType.createVarcharType(ScalarType.MAX_VARCHAR_LENGTH), false));
        }
    }

    private PFetchTableSchemaRequest getFetchTableStructureRequest() throws TException {
        // set TFileScanRangeParams
        TFileScanRangeParams fileScanRangeParams = new TFileScanRangeParams();
        fileScanRangeParams.setFormatType(fileFormatProperties.getFileFormatType());
        Map<String, String> beProperties = new HashMap<>();
        beProperties.putAll(backendConnectProperties);
        fileScanRangeParams.setProperties(beProperties);
        fileScanRangeParams.setFileAttributes(getFileAttributes());
        ConnectContext ctx = ConnectContext.get();
        fileScanRangeParams.setLoadId(ctx.queryId());

        if (getTFileType() == TFileType.FILE_STREAM) {
            fileStatuses.add(new TBrokerFileStatus("", false, -1, true));
            fileScanRangeParams.setFileType(getTFileType());
        }

        if (getTFileType() == TFileType.FILE_HDFS) {
            THdfsParams tHdfsParams = HdfsResource.generateHdfsParam(
                    storageProperties.getBackendConfigProperties());
            String fsName = storageProperties.getBackendConfigProperties().get(HdfsResource.HADOOP_FS_NAME);
            tHdfsParams.setFsName(fsName);
            fileScanRangeParams.setHdfsParams(tHdfsParams);
        }

        // get first file, used to parse table schema
        TBrokerFileStatus firstFile = null;
        for (TBrokerFileStatus fileStatus : fileStatuses) {
            if (isFileContentEmpty(fileStatus)) {
                continue;
            }
            firstFile = fileStatus;
            break;
        }

        // `firstFile == null` means:
        // 1. No matching file path exists
        // 2. All matched files have a size of 0
        // For these two situations, we don't need to get schema from BE
        if (firstFile == null) {
            return null;
        }

        // set TFileRangeDesc
        TFileRangeDesc fileRangeDesc = new TFileRangeDesc();
        fileRangeDesc.setLoadId(ctx.queryId());
        fileRangeDesc.setFileType(getTFileType());
        fileRangeDesc.setCompressType(Util.getOrInferCompressType(
                fileFormatProperties.getCompressionType(), firstFile.getPath()));
        fileRangeDesc.setPath(firstFile.getPath());
        fileRangeDesc.setStartOffset(0);
        fileRangeDesc.setSize(firstFile.getSize());
        fileRangeDesc.setFileSize(firstFile.getSize());
        fileRangeDesc.setModificationTime(firstFile.getModificationTime());
        // set TFileScanRange
        TFileScanRange fileScanRange = new TFileScanRange();
        fileScanRange.addToRanges(fileRangeDesc);
        fileScanRange.setParams(fileScanRangeParams);
        return InternalService.PFetchTableSchemaRequest.newBuilder()
                .setFileScanRange(ByteString.copyFrom(new TSerializer().serialize(fileScanRange))).build();
    }

    private boolean isFileContentEmpty(TBrokerFileStatus fileStatus) {
        if (fileStatus.isIsDir() || fileStatus.size == 0) {
            return true;
        }
        if (Util.isCsvFormat(fileFormatProperties.getFileFormatType())
                || fileFormatProperties.getFileFormatType() == TFileFormatType.FORMAT_JSON) {
            int magicNumberBytes = 0;
            switch (fileFormatProperties.getCompressionType()) {
                case GZ:
                    magicNumberBytes = 20;
                    break;
                case LZO:
                case LZOP:
                    magicNumberBytes = 42;
                    break;
                case DEFLATE:
                    magicNumberBytes = 8;
                    break;
                case SNAPPYBLOCK:
                case LZ4BLOCK:
                case LZ4FRAME:
                    magicNumberBytes = 4;
                    break;
                case BZ2:
                    magicNumberBytes = 14;
                    break;
                case UNKNOWN:
                case PLAIN:
                default:
                    break;
            }
            // fileStatus.size may be -1 in http_stream
            if (fileStatus.size >= 0 && fileStatus.size <= magicNumberBytes) {
                return true;
            }
        }
        return false;
    }

    public void checkAuth(ConnectContext ctx) {
        if (resourceName.isPresent()) {
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkResourcePriv(ctx, resourceName.get(), PrivPredicate.USAGE)) {
                String message = ErrorCode.ERR_RESOURCE_ACCESS_DENIED_ERROR.formatErrorMsg(
                        PrivPredicate.USAGE.getPrivs().toString(), resourceName.get());
                throw new org.apache.doris.nereids.exceptions.AnalysisException(message);
            }
        }
    }
}

