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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TResultFileSinkOptions;

import com.google.common.base.Strings;

import java.util.Map;

public abstract class FileFormatProperties {
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
    public static final String PROP_COMPRESS_TYPE = "compress_type";

    protected String formatName;
    protected TFileFormatType fileFormatType;

    protected TFileCompressType compressionType;

    public FileFormatProperties(TFileFormatType fileFormatType, String formatName) {
        this.fileFormatType = fileFormatType;
        this.formatName = formatName;
    }

    /**
     * Analyze user properties
     * @param formatProperties properties specified by user
     * @param isRemoveOriginProperty if this param is set to true, then this method would remove the origin property
     * @throws AnalysisException
     */
    public abstract void analyzeFileFormatProperties(
            Map<String, String> formatProperties, boolean isRemoveOriginProperty)
            throws AnalysisException;

    /**
     * generate TResultFileSinkOptions according to the properties of specified file format
     * You must call method `analyzeFileFormatProperties` once before calling method `toTResultFileSinkOptions`
     */
    public abstract void fullTResultFileSinkOptions(TResultFileSinkOptions sinkOptions);

    /**
     * generate TFileAttributes according to the properties of specified file format
     * You must call method `analyzeFileFormatProperties` once before calling method `toTFileAttributes`
     */
    public abstract TFileAttributes toTFileAttributes();

    public static FileFormatProperties createFileFormatProperties(String formatString) {
        if (formatString == null) {
            throw new AnalysisException("formatString can not be null");
        }
        switch (formatString.toLowerCase()) {
            case FORMAT_CSV:
                return new CsvFileFormatProperties(formatString);
            case FORMAT_HIVE_TEXT:
                return new TextFileFormatProperties();
            case FORMAT_CSV_WITH_NAMES:
                return new CsvFileFormatProperties(FORMAT_CSV_WITH_NAMES, formatString);
            case FORMAT_CSV_WITH_NAMES_AND_TYPES:
                return new CsvFileFormatProperties(FORMAT_CSV_WITH_NAMES_AND_TYPES, formatString);
            case FORMAT_PARQUET:
                return new ParquetFileFormatProperties();
            case FORMAT_ORC:
                return new OrcFileFormatProperties();
            case FORMAT_JSON:
                return new JsonFileFormatProperties();
            case FORMAT_AVRO:
                return new AvroFileFormatProperties();
            case FORMAT_WAL:
                return new WalFileFormatProperties();
            case FORMAT_ARROW:
                return new ArrowFileFormatProperties();
            default:
                throw new AnalysisException("format:" + formatString + " is not supported.");
        }
    }

    /**
     * Create a FileFormatProperties
     * If the format property is not specified, return a DeferredFileFormatProperties
     */
    public static FileFormatProperties createFileFormatPropertiesOrDeferred(String formatString)
            throws AnalysisException {
        if (Strings.isNullOrEmpty(formatString)) {
            return new DeferredFileFormatProperties();
        } else {
            return createFileFormatProperties(formatString);
        }
    }

    protected String getOrDefault(Map<String, String> props, String key, String defaultValue,
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

    public String getFormatName() {
        return formatName;
    }
}
