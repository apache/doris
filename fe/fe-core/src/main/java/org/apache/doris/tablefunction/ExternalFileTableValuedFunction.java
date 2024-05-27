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
import org.apache.doris.analysis.Separator;
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
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.common.util.FileFormatConstants;
import org.apache.doris.common.util.FileFormatUtils;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.tvf.source.TVFScanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PFetchTableSchemaRequest;
import org.apache.doris.proto.Types.PScalarType;
import org.apache.doris.proto.Types.PStructField;
import org.apache.doris.proto.Types.PTypeDesc;
import org.apache.doris.proto.Types.PTypeNode;
import org.apache.doris.qe.ConnectContext;
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
import org.apache.doris.thrift.TFileTextScanRangeParams;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.THdfsParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTextSerdeType;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * ExternalFileTableValuedFunction is used for S3/HDFS/LOCAL table-valued-function
 */
public abstract class ExternalFileTableValuedFunction extends TableValuedFunctionIf {
    public static final Logger LOG = LogManager.getLogger(ExternalFileTableValuedFunction.class);

    public static final String PROP_TABLE_ID = "table_id";

    protected static final ImmutableSet<String> FILE_FORMAT_PROPERTIES = new ImmutableSet.Builder<String>()
            .add(FileFormatConstants.PROP_FORMAT)
            .add(FileFormatConstants.PROP_JSON_ROOT)
            .add(FileFormatConstants.PROP_JSON_PATHS)
            .add(FileFormatConstants.PROP_STRIP_OUTER_ARRAY)
            .add(FileFormatConstants.PROP_READ_JSON_BY_LINE)
            .add(FileFormatConstants.PROP_NUM_AS_STRING)
            .add(FileFormatConstants.PROP_FUZZY_PARSE)
            .add(FileFormatConstants.PROP_COLUMN_SEPARATOR)
            .add(FileFormatConstants.PROP_LINE_DELIMITER)
            .add(FileFormatConstants.PROP_TRIM_DOUBLE_QUOTES)
            .add(FileFormatConstants.PROP_SKIP_LINES)
            .add(FileFormatConstants.PROP_CSV_SCHEMA)
            .add(FileFormatConstants.PROP_COMPRESS_TYPE)
            .add(FileFormatConstants.PROP_PATH_PARTITION_KEYS)
            .build();

    // Columns got from file and path(if has)
    protected List<Column> columns = null;
    // User specified csv columns, it will override columns got from file
    private final List<Column> csvSchema = Lists.newArrayList();

    // Partition columns from path, e.g. /path/to/columnName=columnValue.
    private List<String> pathPartitionKeys;

    protected List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();
    protected Map<String, String> locationProperties = Maps.newHashMap();
    protected String filePath;

    protected TFileFormatType fileFormatType;
    private TFileCompressType compressionType;
    private String headerType = "";

    private TTextSerdeType textSerdeType = TTextSerdeType.JSON_TEXT_SERDE;
    private String columnSeparator = FileFormatConstants.DEFAULT_COLUMN_SEPARATOR;
    private String lineDelimiter = FileFormatConstants.DEFAULT_LINE_DELIMITER;
    private String jsonRoot = "";
    private String jsonPaths = "";
    private boolean stripOuterArray;
    private boolean readJsonByLine;
    private boolean numAsString;
    private boolean fuzzyParse;
    private boolean trimDoubleQuotes;
    private int skipLines;
    private long tableId;

    public abstract TFileType getTFileType();

    public abstract String getFilePath();

    public abstract BrokerDesc getBrokerDesc();

    public TFileFormatType getTFileFormatType() {
        return fileFormatType;
    }

    public TFileCompressType getTFileCompressType() {
        return compressionType;
    }

    public Map<String, String> getLocationProperties() {
        return locationProperties;
    }

    public List<String> getPathPartitionKeys() {
        return pathPartitionKeys;
    }

    protected void parseFile() throws AnalysisException {
        String path = getFilePath();
        BrokerDesc brokerDesc = getBrokerDesc();
        try {
            BrokerUtil.parseFile(path, brokerDesc, fileStatuses);
        } catch (UserException e) {
            throw new AnalysisException("parse file failed, err: " + e.getMessage(), e);
        }
    }

    //The keys in properties map need to be lowercase.
    protected Map<String, String> parseCommonProperties(Map<String, String> properties) throws AnalysisException {
        Map<String, String> mergedProperties = Maps.newHashMap();
        if (properties.containsKey("resource")) {
            Resource resource = Env.getCurrentEnv().getResourceMgr().getResource(properties.get("resource"));
            if (resource == null) {
                throw new AnalysisException("Can not find resource: " + properties.get("resource"));
            }
            mergedProperties = resource.getCopiedProperties();
        }
        mergedProperties.putAll(properties);
        // Copy the properties, because we will remove the key from properties.
        Map<String, String> copiedProps = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        copiedProps.putAll(mergedProperties);

        String formatString = getOrDefaultAndRemove(copiedProps, FileFormatConstants.PROP_FORMAT, "").toLowerCase();
        String defaultColumnSeparator = FileFormatConstants.DEFAULT_COLUMN_SEPARATOR;
        switch (formatString) {
            case "csv":
                this.fileFormatType = TFileFormatType.FORMAT_CSV_PLAIN;
                break;
            case "hive_text":
                this.fileFormatType = TFileFormatType.FORMAT_CSV_PLAIN;
                defaultColumnSeparator = FileFormatConstants.DEFAULT_HIVE_TEXT_COLUMN_SEPARATOR;
                this.textSerdeType = TTextSerdeType.HIVE_TEXT_SERDE;
                break;
            case "csv_with_names":
                this.headerType = FileFormatConstants.FORMAT_CSV_WITH_NAMES;
                this.fileFormatType = TFileFormatType.FORMAT_CSV_PLAIN;
                break;
            case "csv_with_names_and_types":
                this.headerType = FileFormatConstants.FORMAT_CSV_WITH_NAMES_AND_TYPES;
                this.fileFormatType = TFileFormatType.FORMAT_CSV_PLAIN;
                break;
            case "parquet":
                this.fileFormatType = TFileFormatType.FORMAT_PARQUET;
                break;
            case "orc":
                this.fileFormatType = TFileFormatType.FORMAT_ORC;
                break;
            case "json":
                this.fileFormatType = TFileFormatType.FORMAT_JSON;
                break;
            case "avro":
                this.fileFormatType = TFileFormatType.FORMAT_AVRO;
                break;
            case "wal":
                this.fileFormatType = TFileFormatType.FORMAT_WAL;
                break;
            default:
                throw new AnalysisException("format:" + formatString + " is not supported.");
        }

        tableId = Long.valueOf(getOrDefaultAndRemove(copiedProps, PROP_TABLE_ID, "-1"));
        columnSeparator = getOrDefaultAndRemove(copiedProps, FileFormatConstants.PROP_COLUMN_SEPARATOR,
                defaultColumnSeparator);
        if (Strings.isNullOrEmpty(columnSeparator)) {
            throw new AnalysisException("column_separator can not be empty.");
        }
        columnSeparator = Separator.convertSeparator(columnSeparator);

        lineDelimiter = getOrDefaultAndRemove(copiedProps, FileFormatConstants.PROP_LINE_DELIMITER,
                FileFormatConstants.DEFAULT_LINE_DELIMITER);
        if (Strings.isNullOrEmpty(lineDelimiter)) {
            throw new AnalysisException("line_delimiter can not be empty.");
        }
        lineDelimiter = Separator.convertSeparator(lineDelimiter);

        jsonRoot = getOrDefaultAndRemove(copiedProps, FileFormatConstants.PROP_JSON_ROOT, "");
        jsonPaths = getOrDefaultAndRemove(copiedProps, FileFormatConstants.PROP_JSON_PATHS, "");
        readJsonByLine = Boolean.valueOf(
                getOrDefaultAndRemove(copiedProps, FileFormatConstants.PROP_READ_JSON_BY_LINE, "")).booleanValue();
        stripOuterArray = Boolean.valueOf(
                getOrDefaultAndRemove(copiedProps, FileFormatConstants.PROP_STRIP_OUTER_ARRAY, "")).booleanValue();
        numAsString = Boolean.valueOf(
                getOrDefaultAndRemove(copiedProps, FileFormatConstants.PROP_NUM_AS_STRING, "")).booleanValue();
        fuzzyParse = Boolean.valueOf(
                getOrDefaultAndRemove(copiedProps, FileFormatConstants.PROP_FUZZY_PARSE, "")).booleanValue();
        trimDoubleQuotes = Boolean.valueOf(
                getOrDefaultAndRemove(copiedProps, FileFormatConstants.PROP_TRIM_DOUBLE_QUOTES, "")).booleanValue();
        skipLines = Integer.valueOf(
                getOrDefaultAndRemove(copiedProps, FileFormatConstants.PROP_SKIP_LINES, "0")).intValue();

        String compressTypeStr = getOrDefaultAndRemove(copiedProps, FileFormatConstants.PROP_COMPRESS_TYPE, "UNKNOWN");
        try {
            compressionType = Util.getFileCompressType(compressTypeStr);
        } catch (IllegalArgumentException e) {
            throw new AnalysisException("Compress type : " +  compressTypeStr + " is not supported.");
        }
        if (FileFormatUtils.isCsv(formatString)) {
            FileFormatUtils.parseCsvSchema(csvSchema, getOrDefaultAndRemove(copiedProps,
                    FileFormatConstants.PROP_CSV_SCHEMA, ""));
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
        TFileAttributes fileAttributes = new TFileAttributes();
        TFileTextScanRangeParams fileTextScanRangeParams = new TFileTextScanRangeParams();
        fileTextScanRangeParams.setColumnSeparator(this.columnSeparator);
        fileTextScanRangeParams.setLineDelimiter(this.lineDelimiter);
        fileAttributes.setTextParams(fileTextScanRangeParams);
        if (this.fileFormatType == TFileFormatType.FORMAT_CSV_PLAIN) {
            fileAttributes.setHeaderType(this.headerType);
            fileAttributes.setTrimDoubleQuotes(trimDoubleQuotes);
            fileAttributes.setSkipLines(skipLines);
        } else if (this.fileFormatType == TFileFormatType.FORMAT_JSON) {
            fileAttributes.setJsonRoot(jsonRoot);
            fileAttributes.setJsonpaths(jsonPaths);
            fileAttributes.setReadJsonByLine(readJsonByLine);
            fileAttributes.setStripOuterArray(stripOuterArray);
            fileAttributes.setNumAsString(numAsString);
            fileAttributes.setFuzzyParse(fuzzyParse);
        }
        return fileAttributes;
    }

    @Override
    public ScanNode getScanNode(PlanNodeId id, TupleDescriptor desc) {
        return new TVFScanNode(id, desc, false);
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

        if (this.fileFormatType == TFileFormatType.FORMAT_WAL) {
            List<Column> fileColumns = new ArrayList<>();
            Table table = Env.getCurrentInternalCatalog().getTableByTableId(tableId);
            List<Column> tableColumns = table.getBaseSchema(true);
            for (int i = 0; i < tableColumns.size(); i++) {
                Column column = new Column(tableColumns.get(i).getName(), tableColumns.get(i).getType(), true);
                column.setUniqueId(tableColumns.get(i).getUniqueId());
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
        if (getTFileType() == TFileType.FILE_STREAM) {
            long backendId = ConnectContext.get().getBackendId();
            Backend be = Env.getCurrentSystemInfo().getIdToBackend().get(backendId);
            if (be == null || !be.isAlive()) {
                LOG.warn("Backend {} is not alive", backendId);
                return null;
            } else {
                return be;
            }
        }
        for (Backend be : Env.getCurrentSystemInfo().getIdToBackend().values()) {
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
            for (int i = 0; i < typeNodes.get(start).getStructFieldsCount(); ++i) {
                Pair<Type, Integer> fieldType = getColumnType(typeNodes, start + parsedNodes);
                PStructField structField = typeNodes.get(start).getStructFields(i);
                fields.add(new StructField(structField.getName(), fieldType.key(), structField.getComment(),
                                            structField.getContainsNull()));
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
        for (int idx = 0; idx < result.getColumnNums(); ++idx) {
            PTypeDesc type = result.getColumnTypes(idx);
            String colName = result.getColumnNames(idx);
            columns.add(new Column(colName, getColumnType(type.getTypesList(), 0).key(), true));
        }
        // add path columns
        // HACK(tsy): path columns are all treated as STRING type now, after BE supports reading all columns
        //  types by all format readers from file meta, maybe reading path columns types from BE then.
        for (String colName : pathPartitionKeys) {
            columns.add(new Column(colName, Type.STRING, false));
        }
    }

    private PFetchTableSchemaRequest getFetchTableStructureRequest() throws TException {
        // set TFileScanRangeParams
        TFileScanRangeParams fileScanRangeParams = new TFileScanRangeParams();
        fileScanRangeParams.setFormatType(fileFormatType);
        fileScanRangeParams.setProperties(locationProperties);
        fileScanRangeParams.setTextSerdeType(textSerdeType);
        fileScanRangeParams.setFileAttributes(getFileAttributes());
        ConnectContext ctx = ConnectContext.get();
        fileScanRangeParams.setLoadId(ctx.queryId());

        if (getTFileType() == TFileType.FILE_STREAM) {
            fileStatuses.add(new TBrokerFileStatus("", false, -1, true));
            fileScanRangeParams.setFileType(getTFileType());
        }

        if (getTFileType() == TFileType.FILE_HDFS) {
            THdfsParams tHdfsParams = HdfsResource.generateHdfsParam(locationProperties);
            String fsName = getLocationProperties().get(HdfsResource.HADOOP_FS_NAME);
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
        fileRangeDesc.setCompressType(Util.getOrInferCompressType(compressionType, firstFile.getPath()));
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
        if (Util.isCsvFormat(fileFormatType) || fileFormatType == TFileFormatType.FORMAT_JSON) {
            int magicNumberBytes = 0;
            switch (compressionType) {
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
}

