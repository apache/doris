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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.common.util.FileFormatConstants;
import org.apache.doris.datasource.property.storage.LocalProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TMetadataType;
import org.apache.doris.thrift.TParquetMetadataParams;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Table-valued function parquet_meta for reading Parquet metadata.
 * Currently works in two modes:
 * - parquet_meta (mode parquet_metadata): row-group/column statistics similar to DuckDB parquet_metadata()
 * - parquet_schema: logical schema similar to DuckDB parquet_schema()
 * - parquet_file_metadata: file-level metadata aligned with DuckDB parquet_file_metadata()
 * - parquet_kv_metadata: file key/value metadata aligned with DuckDB parquet_kv_metadata()
 * - parquet_bloom_probe: row group bloom filter probe aligned with DuckDB parquet_bloom_probe()
 */
public class ParquetMetadataTableValuedFunction extends MetadataTableValuedFunction {

    public static final String NAME = "parquet_meta";
    public static final String NAME_FILE_METADATA = "parquet_file_metadata";
    public static final String NAME_KV_METADATA = "parquet_kv_metadata";
    public static final String NAME_BLOOM_PROBE = "parquet_bloom_probe";
    private static final String MODE = "mode";
    private static final String COLUMN = "column";
    private static final String VALUE = "value";

    private static final String MODE_METADATA = "parquet_metadata";
    private static final String MODE_SCHEMA = "parquet_schema";
    private static final String MODE_FILE_METADATA = "parquet_file_metadata";
    private static final String MODE_KV_METADATA = "parquet_kv_metadata";
    private static final String MODE_BLOOM_PROBE = "parquet_bloom_probe";
    private static final ImmutableSet<String> SUPPORTED_MODES =
            ImmutableSet.of(MODE_METADATA, MODE_SCHEMA, MODE_FILE_METADATA, MODE_KV_METADATA,
                    MODE_BLOOM_PROBE);

    private static final ImmutableList<Column> PARQUET_SCHEMA_COLUMNS = ImmutableList.of(
            // Align with DuckDB parquet_schema() output.
            new Column("file_name", ScalarType.createVarcharType(ScalarType.MAX_VARCHAR_LENGTH), true),
            new Column("name", ScalarType.createVarcharType(ScalarType.MAX_VARCHAR_LENGTH), true),
            new Column("type", ScalarType.createVarcharType(ScalarType.MAX_VARCHAR_LENGTH), true),
            new Column("type_length", PrimitiveType.BIGINT, true),
            new Column("repetition_type", ScalarType.createVarcharType(ScalarType.MAX_VARCHAR_LENGTH), true),
            new Column("num_children", PrimitiveType.BIGINT, true),
            new Column("converted_type", ScalarType.createVarcharType(ScalarType.MAX_VARCHAR_LENGTH), true),
            new Column("scale", PrimitiveType.BIGINT, true),
            new Column("precision", PrimitiveType.BIGINT, true),
            new Column("field_id", PrimitiveType.BIGINT, true),
            new Column("logical_type", ScalarType.createVarcharType(ScalarType.MAX_VARCHAR_LENGTH), true)
    );

    private static final ImmutableList<Column> PARQUET_METADATA_COLUMNS = ImmutableList.of(
            // Align with DuckDB parquet_metadata() output.
            new Column("file_name", ScalarType.createStringType(), true),
            new Column("row_group_id", PrimitiveType.BIGINT, true),
            new Column("row_group_num_rows", PrimitiveType.BIGINT, true),
            new Column("row_group_num_columns", PrimitiveType.BIGINT, true),
            new Column("row_group_bytes", PrimitiveType.BIGINT, true),
            new Column("column_id", PrimitiveType.BIGINT, true),
            new Column("file_offset", PrimitiveType.BIGINT, true),
            new Column("num_values", PrimitiveType.BIGINT, true),
            new Column("path_in_schema", ScalarType.createStringType(), true),
            new Column("type", ScalarType.createStringType(), true),
            new Column("stats_min", ScalarType.createStringType(), true),
            new Column("stats_max", ScalarType.createStringType(), true),
            new Column("stats_null_count", PrimitiveType.BIGINT, true),
            new Column("stats_distinct_count", PrimitiveType.BIGINT, true),
            new Column("stats_min_value", ScalarType.createStringType(), true),
            new Column("stats_max_value", ScalarType.createStringType(), true),
            new Column("compression", ScalarType.createStringType(), true),
            new Column("encodings", ScalarType.createStringType(), true),
            new Column("index_page_offset", PrimitiveType.BIGINT, true),
            new Column("dictionary_page_offset", PrimitiveType.BIGINT, true),
            new Column("data_page_offset", PrimitiveType.BIGINT, true),
            new Column("total_compressed_size", PrimitiveType.BIGINT, true),
            new Column("total_uncompressed_size", PrimitiveType.BIGINT, true),
            new Column("key_value_metadata", new MapType(
                    ScalarType.createVarbinaryType(ScalarType.MAX_VARBINARY_LENGTH),
                    ScalarType.createVarbinaryType(ScalarType.MAX_VARBINARY_LENGTH)), true),
            new Column("bloom_filter_offset", PrimitiveType.BIGINT, true),
            new Column("bloom_filter_length", PrimitiveType.BIGINT, true),
            new Column("min_is_exact", PrimitiveType.BOOLEAN, true),
            new Column("max_is_exact", PrimitiveType.BOOLEAN, true),
            new Column("row_group_compressed_bytes", PrimitiveType.BIGINT, true)
    );

    private static final ImmutableList<Column> PARQUET_FILE_METADATA_COLUMNS = ImmutableList.of(
            new Column("file_name", PrimitiveType.STRING, true),
            new Column("created_by", PrimitiveType.STRING, true),
            new Column("num_rows", PrimitiveType.BIGINT, true),
            new Column("num_row_groups", PrimitiveType.BIGINT, true),
            new Column("format_version", PrimitiveType.BIGINT, true),
            new Column("encryption_algorithm", PrimitiveType.STRING, true),
            new Column("footer_signing_key_metadata", PrimitiveType.STRING, true)
    );

    private static final ImmutableList<Column> PARQUET_KV_METADATA_COLUMNS = ImmutableList.of(
            new Column("file_name", PrimitiveType.STRING, true),
            new Column("key", ScalarType.createStringType(), true),
            new Column("value", ScalarType.createStringType(), true)
    );

    private static final ImmutableList<Column> PARQUET_BLOOM_PROBE_COLUMNS = ImmutableList.of(
            new Column("file_name", PrimitiveType.STRING, true),
            new Column("row_group_id", PrimitiveType.INT, true),
            // 1 = excluded by BF, 0 = might contain, -1 = no bloom filter in file
            new Column("bloom_filter_excludes", PrimitiveType.INT, true)
    );

    private final List<String> paths;
    private final String mode;
    // File system info for remote Parquet access (e.g. S3).
    private final TFileType fileType;
    private final Map<String, String> properties;
    private final String bloomColumn;
    private final String bloomLiteral;

    public ParquetMetadataTableValuedFunction(Map<String, String> params) throws AnalysisException {
        Map<String, String> normalizedParams = params.entrySet().stream().collect(Collectors.toMap(
                entry -> entry.getKey().toLowerCase(),
                Map.Entry::getValue,
                (value1, value2) -> value2
        ));
        String rawUri = normalizedParams.get(ExternalFileTableValuedFunction.URI_KEY);
        boolean uriProvided = !Strings.isNullOrEmpty(rawUri);
        String rawPath = uriProvided ? rawUri : normalizedParams.get(LocalProperties.PROP_FILE_PATH);
        if (Strings.isNullOrEmpty(rawPath)) {
            throw new AnalysisException(
                    "Property 'uri' or 'file_path' is required for parquet_meta");
        }
        String parsedPath = rawPath.trim();
        if (parsedPath.isEmpty()) {
            throw new AnalysisException(
                    "Property 'uri' or 'file_path' must contain at least one location");
        }

        String rawMode = normalizedParams.getOrDefault(MODE, MODE_METADATA);
        mode = rawMode.toLowerCase();
        if (!SUPPORTED_MODES.contains(mode)) {
            throw new AnalysisException("Unsupported mode '" + rawMode + "' for parquet_meta");
        }
        String tmpBloomColumn = null;
        String tmpBloomLiteral = null;
        if (MODE_BLOOM_PROBE.equals(mode)) {
            tmpBloomColumn = normalizedParams.get(COLUMN);
            tmpBloomLiteral = normalizedParams.get(VALUE);
            if (Strings.isNullOrEmpty(tmpBloomColumn) || Strings.isNullOrEmpty(tmpBloomLiteral)) {
                throw new AnalysisException(
                        "Missing 'column' or 'value' for mode parquet_bloom_probe");
            }
            tmpBloomColumn = tmpBloomColumn.trim();
            tmpBloomLiteral = tmpBloomLiteral.trim();
            if (tmpBloomColumn.isEmpty() || tmpBloomLiteral.isEmpty()) {
                throw new AnalysisException(
                        "Missing 'column' or 'value' for mode parquet_bloom_probe");
            }
        }

        String scheme = null;
        try {
            scheme = new URI(parsedPath).getScheme();
        } catch (URISyntaxException e) {
            scheme = null;
        }
        if (uriProvided) {
            if (Strings.isNullOrEmpty(scheme)) {
                throw new AnalysisException("Property 'uri' must contain a scheme for parquet_meta");
            }
        } else if (!Strings.isNullOrEmpty(scheme)) {
            throw new AnalysisException("Property 'file_path' must not contain a scheme for parquet_meta");
        }
        Map<String, String> storageParams = new HashMap<>(normalizedParams);
        // StorageProperties detects provider by "uri".
        if (uriProvided) {
            storageParams.put(ExternalFileTableValuedFunction.URI_KEY, parsedPath);
        } else {
            // Local file path, hint local fs support.
            storageParams.put(StorageProperties.FS_LOCAL_SUPPORT, "true");
            storageParams.put(LocalProperties.PROP_FILE_PATH, parsedPath);
        }
        StorageProperties storageProperties;
        try {
            storageProperties = StorageProperties.createPrimary(storageParams);
        } catch (RuntimeException e) {
            throw new AnalysisException(
                    "Failed to parse storage properties for parquet_meta: " + e.getMessage(), e);
        }
        this.fileType = mapToFileType(storageProperties.getType());
        Map<String, String> backendProps = storageProperties.getBackendConfigProperties();
        String normalizedPath;
        try {
            normalizedPath = storageProperties.validateAndNormalizeUri(parsedPath);
        } catch (UserException e) {
            throw new AnalysisException(
                    "Failed to normalize parquet_meta paths: " + e.getMessage(), e);
        }
        this.properties = backendProps;

        // Expand any glob patterns (e.g. *.parquet) to concrete file paths.
        List<String> normalizedPaths =
                expandGlobPath(normalizedPath, storageProperties, storageParams, this.fileType);

        this.paths = ImmutableList.copyOf(normalizedPaths);
        this.bloomColumn = tmpBloomColumn;
        this.bloomLiteral = tmpBloomLiteral;
    }

    /**
     * Expand a wildcard path to matching files.
     */
    private static List<String> expandGlobPath(String inputPath,
                                               StorageProperties storageProperties,
                                               Map<String, String> storageParams,
                                               TFileType fileType) throws AnalysisException {
        if (Strings.isNullOrEmpty(inputPath)) {
            return Collections.emptyList();
        }
        if (!containsWildcards(inputPath)) {
            return ImmutableList.of(inputPath);
        }
        List<String> expanded =
                expandSingleGlob(inputPath, storageProperties, storageParams, fileType);
        if (expanded.isEmpty()) {
            throw new AnalysisException("No files matched parquet_meta path patterns: " + inputPath);
        }
        return expanded;
    }

    private static boolean containsWildcards(String path) {
        if (Strings.isNullOrEmpty(path)) {
            return false;
        }
        return path.contains("*") || path.contains("[") || path.contains("{");
    }

    private static List<String> expandSingleGlob(String pattern,
                                                 StorageProperties storageProperties,
                                                 Map<String, String> storageParams,
                                                 TFileType fileType) throws AnalysisException {
        if (fileType == TFileType.FILE_LOCAL) {
            Map<String, String> localProps = new HashMap<>(storageParams);
            // Allow Local TVF to pick any alive backend when backend_id is not provided.
            localProps.putIfAbsent(LocalTableValuedFunction.PROP_SHARED_STORAGE, "true");
            localProps.putIfAbsent(FileFormatConstants.PROP_FORMAT, FileFormatConstants.FORMAT_PARQUET);
            // Local TVF expects the uri/path in properties; storageParams already contains it.
            LocalTableValuedFunction localTvf = new LocalTableValuedFunction(localProps);
            return localTvf.getFileStatuses().stream()
                    .filter(status -> !status.isIsDir())
                    .map(TBrokerFileStatus::getPath)
                    .collect(Collectors.toList());
        }
        if (fileType == TFileType.FILE_HTTP) {
            throw new AnalysisException("Glob patterns are not supported for file type: " + fileType);
        }
        if (storageProperties == null) {
            throw new AnalysisException("Storage properties is required for glob pattern: " + pattern);
        }
        if (fileType == TFileType.FILE_S3 || fileType == TFileType.FILE_HDFS) {
            return globRemoteWithBroker(pattern, storageParams);
        }
        throw new AnalysisException("Glob patterns are not supported for file type: " + fileType);
    }

    private static List<String> globRemoteWithBroker(String pattern,
                                                     Map<String, String> storageParams) throws AnalysisException {
        List<TBrokerFileStatus> remoteFiles = new ArrayList<>();
        BrokerDesc brokerDesc = new BrokerDesc("ParquetMetaTvf", storageParams);
        try {
            BrokerUtil.parseFile(pattern, brokerDesc, remoteFiles);
        } catch (UserException e) {
            throw new AnalysisException("Failed to expand glob pattern '" + pattern + "': "
                    + e.getMessage(), e);
        }
        return remoteFiles.stream()
                .filter(file -> !file.isIsDir())
                .map(TBrokerFileStatus::getPath)
                .collect(Collectors.toList());
    }

    /**
     * Map FE storage type to BE file type.
     */
    private static TFileType mapToFileType(StorageProperties.Type type) throws AnalysisException {
        switch (type) {
            case HDFS:
            case OSS_HDFS:
                return TFileType.FILE_HDFS;
            case HTTP:
                return TFileType.FILE_HTTP;
            case LOCAL:
                return TFileType.FILE_LOCAL;
            case S3:
            case OSS:
            case OBS:
            case COS:
            case GCS:
            case MINIO:
            case AZURE:
                return TFileType.FILE_S3;
            default:
                throw new AnalysisException("Unsupported storage type for parquet_meta: " + type);
        }
    }

    @Override
    public TMetadataType getMetadataType() {
        return TMetadataType.PARQUET;
    }

    @Override
    public TMetaScanRange getMetaScanRange(List<String> requiredFields) {
        TParquetMetadataParams parquetParams = new TParquetMetadataParams();
        parquetParams.setPaths(paths);
        parquetParams.setMode(mode);
        parquetParams.setFileType(fileType);
        parquetParams.setProperties(properties);
        if (MODE_BLOOM_PROBE.equals(mode)) {
            parquetParams.setBloomColumn(bloomColumn);
            parquetParams.setBloomLiteral(bloomLiteral);
        }

        TMetaScanRange scanRange = new TMetaScanRange();
        scanRange.setMetadataType(TMetadataType.PARQUET);
        scanRange.setParquetParams(parquetParams);
        // Fan out: one file per split so MetadataScanNode can distribute across BEs.
        if (paths != null && !paths.isEmpty()) {
            scanRange.setSerializedSplits(paths);
        }
        return scanRange;
    }

    @Override
    public String getTableName() {
        return "ParquetMetadataTableValuedFunction<" + mode + ">";
    }

    @Override
    public List<Column> getTableColumns() {
        if (MODE_SCHEMA.equals(mode)) {
            return PARQUET_SCHEMA_COLUMNS;
        }
        if (MODE_FILE_METADATA.equals(mode)) {
            return PARQUET_FILE_METADATA_COLUMNS;
        }
        if (MODE_KV_METADATA.equals(mode)) {
            return PARQUET_KV_METADATA_COLUMNS;
        }
        if (MODE_BLOOM_PROBE.equals(mode)) {
            return PARQUET_BLOOM_PROBE_COLUMNS;
        }
        return PARQUET_METADATA_COLUMNS;
    }
}
