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

import org.apache.doris.datasource.property.constants.CsvProperties;
import org.apache.doris.datasource.property.constants.FileFormatBaseProperties;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.proto.InternalService.PFetchTableSchemaRequest;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TResultFileSinkOptions;
import org.apache.doris.thrift.TTextSerdeType;

import java.util.Map;

public abstract class FileFormatProperties {
    protected TFileFormatType fileFormatType;

    protected TFileCompressType compressionType;

    public FileFormatProperties(TFileFormatType fileFormatType) {
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

    public static FileFormatProperties createFileFormatChecker(String formatString) {
        switch (formatString) {
            case FileFormatBaseProperties.FORMAT_CSV:
                return new CsvFileFormatProperties(TFileFormatType.FORMAT_CSV_PLAIN);
            case FileFormatBaseProperties.FORMAT_HIVE_TEXT:
                return new CsvFileFormatProperties(TFileFormatType.FORMAT_CSV_PLAIN,
                        CsvProperties.DEFAULT_HIVE_TEXT_COLUMN_SEPARATOR,
                        TTextSerdeType.HIVE_TEXT_SERDE);
            case FileFormatBaseProperties.FORMAT_CSV_WITH_NAMES:
                return new CsvFileFormatProperties(TFileFormatType.FORMAT_CSV_PLAIN,
                        FileFormatBaseProperties.FORMAT_CSV_WITH_NAMES);
            case FileFormatBaseProperties.FORMAT_CSV_WITH_NAMES_AND_TYPES:
                return new CsvFileFormatProperties(TFileFormatType.FORMAT_CSV_PLAIN,
                        FileFormatBaseProperties.FORMAT_CSV_WITH_NAMES_AND_TYPES);
            case FileFormatBaseProperties.FORMAT_PARQUET:
                return new ParquetFileFormatProperties(TFileFormatType.FORMAT_PARQUET);
            case FileFormatBaseProperties.FORMAT_ORC:
                return new OrcFileFormatProperties(TFileFormatType.FORMAT_ORC);
            case FileFormatBaseProperties.FORMAT_JSON:
                return new JsonFileFormatProperties(TFileFormatType.FORMAT_JSON);
            case FileFormatBaseProperties.FORMAT_AVRO:
                return new AvroFileFormatProperties(TFileFormatType.FORMAT_AVRO);
            case FileFormatBaseProperties.FORMAT_WAL:
                return new WalFileFormatProperties(TFileFormatType.FORMAT_WAL);
            default:
                throw new AnalysisException("format:" + formatString + " is not supported.");
        }
    }

    public static FileFormatProperties createFileFormatChecker(Map<String, String> formatProperties)
            throws AnalysisException {
        String formatString = formatProperties.getOrDefault(FileFormatBaseProperties.PROP_FORMAT, "")
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
