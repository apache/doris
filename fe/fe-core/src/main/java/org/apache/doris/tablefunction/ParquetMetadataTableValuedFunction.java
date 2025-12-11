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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TMetadataType;
import org.apache.doris.thrift.TParquetMetadataParams;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Table-valued function for reading Parquet metadata.
 * Currently works in two modes:
 * - parquet_metadata: row-group/column statistics similar to DuckDB parquet_metadata()
 * - parquet_schema: logical schema similar to DuckDB parquet_schema()
 */
public class ParquetMetadataTableValuedFunction extends MetadataTableValuedFunction {

    public static final String NAME = "parquet_meta";
    private static final String PATH = "path";
    private static final String MODE = "mode";

    private static final String MODE_METADATA = "parquet_metadata";
    private static final String MODE_SCHEMA = "parquet_schema";
    private static final ImmutableSet<String> SUPPORTED_MODES =
            ImmutableSet.of(MODE_METADATA, MODE_SCHEMA);

    private static final ImmutableSet<String> PROPERTIES_SET = ImmutableSet.of(PATH, MODE);

    private static final ImmutableList<Column> PARQUET_SCHEMA_COLUMNS = ImmutableList.of(
            new Column("file_name", PrimitiveType.STRING, true),
            new Column("column_name", PrimitiveType.STRING, true),
            new Column("column_path", PrimitiveType.STRING, true),
            new Column("physical_type", PrimitiveType.STRING, true),
            new Column("logical_type", PrimitiveType.STRING, true),
            new Column("repetition_level", PrimitiveType.INT, true),
            new Column("definition_level", PrimitiveType.INT, true),
            new Column("type_length", PrimitiveType.INT, true),
            new Column("precision", PrimitiveType.INT, true),
            new Column("scale", PrimitiveType.INT, true),
            new Column("is_nullable", PrimitiveType.BOOLEAN, true)
    );

    private static final ImmutableList<Column> PARQUET_METADATA_COLUMNS = ImmutableList.of(
            new Column("file_name", PrimitiveType.STRING, true),
            new Column("row_group_id", PrimitiveType.INT, true),
            new Column("column_id", PrimitiveType.INT, true),
            new Column("column_name", PrimitiveType.STRING, true),
            new Column("column_path", PrimitiveType.STRING, true),
            new Column("physical_type", PrimitiveType.STRING, true),
            new Column("logical_type", PrimitiveType.STRING, true),
            new Column("type_length", PrimitiveType.INT, true),
            new Column("converted_type", PrimitiveType.STRING, true),
            new Column("num_values", PrimitiveType.BIGINT, true),
            new Column("null_count", PrimitiveType.BIGINT, true),
            new Column("distinct_count", PrimitiveType.BIGINT, true),
            new Column("encodings", PrimitiveType.STRING, true),
            new Column("compression", PrimitiveType.STRING, true),
            new Column("data_page_offset", PrimitiveType.BIGINT, true),
            new Column("index_page_offset", PrimitiveType.BIGINT, true),
            new Column("dictionary_page_offset", PrimitiveType.BIGINT, true),
            new Column("total_compressed_size", PrimitiveType.BIGINT, true),
            new Column("total_uncompressed_size", PrimitiveType.BIGINT, true),
            new Column("statistics_min", ScalarType.createStringType(), true),
            new Column("statistics_max", ScalarType.createStringType(), true)
    );

    private final List<String> paths;
    private final String mode;

    public ParquetMetadataTableValuedFunction(Map<String, String> params) throws AnalysisException {
        Map<String, String> normalizedParams = params.entrySet().stream().collect(Collectors.toMap(
                entry -> entry.getKey().toLowerCase(),
                Map.Entry::getValue,
                (value1, value2) -> value2
        ));
        for (String key : normalizedParams.keySet()) {
            if (!PROPERTIES_SET.contains(key)) {
                throw new AnalysisException("'" + key + "' is invalid property for parquet_metadata");
            }
        }
        String rawPath = normalizedParams.get(PATH);
        if (Strings.isNullOrEmpty(rawPath)) {
            throw new AnalysisException("Property 'path' is required for parquet_metadata");
        }
        List<String> parsedPaths = Arrays.stream(rawPath.split(","))
                .map(String::trim)
                .filter(token -> !token.isEmpty())
                .collect(Collectors.toList());
        if (parsedPaths.isEmpty()) {
            throw new AnalysisException("Property 'path' must contain at least one location");
        }
        this.paths = ImmutableList.copyOf(parsedPaths);

        String rawMode = normalizedParams.getOrDefault(MODE, MODE_METADATA);
        mode = rawMode.toLowerCase();
        if (!SUPPORTED_MODES.contains(mode)) {
            throw new AnalysisException("Unsupported mode '" + rawMode + "' for parquet_metadata");
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

        TMetaScanRange scanRange = new TMetaScanRange();
        scanRange.setMetadataType(TMetadataType.PARQUET);
        scanRange.setParquetParams(parquetParams);
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
        return PARQUET_METADATA_COLUMNS;
    }
}
