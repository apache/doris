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

package org.apache.doris.datasource.property.fileformat;

import org.apache.doris.datasource.property.fileformat.CsvFileFormatConfigurator.CsvFileFormatProperties;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.proto.InternalService.PFetchTableSchemaRequest;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TResultFileSinkOptions;
import org.apache.doris.thrift.TTextSerdeType;

import java.util.Map;

public abstract class FileFormatConfigurator {
    public static class FileFormatProperties {
        public static final String PROP_FORMAT = "format";
        public static final String FORMAT_PARQUET = "parquet";
        public static final String FORMAT_CSV = "csv";
        public static final String FORMAT_CSV_WITH_NAMES = "csv_with_names";
        public static final String FORMAT_CSV_WITH_NAMES_AND_TYPES = "csv_with_names_and_types";
        public static final String FORMAT_HIVE_TEXT = "hive_text";
        public static final String FORMAT_ORC = "orc";
        public static final String FORMAT_JSON = "json";
        public static final String FORMAT_AVRO = "avro";
        public static final String FORMAT_WAL = "wal";
        public static final String FORMAT_ARROW = "arrow";
    }

    protected TFileFormatType fileFormatType;

    protected TFileCompressType compressionType;

    public FileFormatConfigurator(TFileFormatType fileFormatType) {
        this.fileFormatType = fileFormatType;
    }

    /**
     *
     * @param formatProperties
     * @return properties needed by Doris
     * @throws AnalysisException
     */

    /**
     * Analyze user properties
     * @param formatProperties properties specified by user
     * @param isRemoveOriginProperty if this param is set to true, then this method would remove the origin property
     * @throws AnalysisException
     */
    public abstract void analyzeFileFormatProperties(
            Map<String, String> formatProperties, boolean isRemoveOriginProperty)
            throws AnalysisException;

    public abstract PFetchTableSchemaRequest toPFetchTableSchemaRequest();

    public abstract TResultFileSinkOptions toTResultFileSinkOptions();

    public abstract TFileAttributes toTFileAttributes();

    public static FileFormatConfigurator createFileFormatChecker(String formatString) {
        switch (formatString) {
            case FileFormatProperties.FORMAT_CSV:
                return new CsvFileFormatConfigurator(TFileFormatType.FORMAT_CSV_PLAIN);
            case FileFormatProperties.FORMAT_HIVE_TEXT:
                return new CsvFileFormatConfigurator(TFileFormatType.FORMAT_CSV_PLAIN,
                        CsvFileFormatProperties.DEFAULT_HIVE_TEXT_COLUMN_SEPARATOR,
                        TTextSerdeType.HIVE_TEXT_SERDE);
            case FileFormatProperties.FORMAT_CSV_WITH_NAMES:
                return new CsvFileFormatConfigurator(TFileFormatType.FORMAT_CSV_PLAIN,
                        FileFormatProperties.FORMAT_CSV_WITH_NAMES);
            case FileFormatProperties.FORMAT_CSV_WITH_NAMES_AND_TYPES:
                return new CsvFileFormatConfigurator(TFileFormatType.FORMAT_CSV_PLAIN,
                        FileFormatProperties.FORMAT_CSV_WITH_NAMES_AND_TYPES);
            case FileFormatProperties.FORMAT_PARQUET:
                return new ParquetFileFormatConfigurator(TFileFormatType.FORMAT_PARQUET);
            case FileFormatProperties.FORMAT_ORC:
                return new OrcFileFormatConfigurator(TFileFormatType.FORMAT_ORC);
            case FileFormatProperties.FORMAT_JSON:
                return new JsonFileFormatConfigurator(TFileFormatType.FORMAT_JSON);
            case FileFormatProperties.FORMAT_AVRO:
                return new AvroFileFormatConfigurator(TFileFormatType.FORMAT_AVRO);
            case FileFormatProperties.FORMAT_WAL:
                return new WalFileFormatConfigurator(TFileFormatType.FORMAT_WAL);
            default:
                throw new AnalysisException("format:" + formatString + " is not supported.");
        }
    }

    public static FileFormatConfigurator createFileFormatChecker(Map<String, String> formatProperties)
            throws AnalysisException {
        String formatString = formatProperties.getOrDefault(FileFormatProperties.PROP_FORMAT, "")
                .toLowerCase();
        return createFileFormatChecker(formatString);
    }

    protected String getOrDefaultAndRemove(Map<String, String> props, String key, String defaultValue,
            boolean isRemove) {
        String value = props.getOrDefault(key, defaultValue);
        if (isRemove) {
            props.remove(key);
        }
        return value;
    }

    public TFileFormatType getFileFormatType() {
        return fileFormatType;
    }

    public TFileCompressType getCompressionType() {
        return compressionType;
    }
}
