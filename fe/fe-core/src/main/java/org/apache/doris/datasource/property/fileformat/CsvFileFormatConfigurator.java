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

import org.apache.doris.analysis.Separator;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.util.FileFormatUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.proto.InternalService.PFetchTableSchemaRequest;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TResultFileSinkOptions;
import org.apache.doris.thrift.TTextSerdeType;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class CsvFileFormatConfigurator extends FileFormatConfigurator {
    public static final Logger LOG = LogManager.getLogger(CsvFileFormatConfigurator.class);

    private String headerType = "";
    private TTextSerdeType textSerdeType = TTextSerdeType.JSON_TEXT_SERDE;
    private String columnSeparator = CsvFileFormatProperties.DEFAULT_COLUMN_SEPARATOR;
    private String lineDelimiter = CsvFileFormatProperties.DEFAULT_LINE_DELIMITER;
    private boolean trimDoubleQuotes;
    private int skipLines;
    private byte enclose;

    // used by tvf
    // User specified csv columns, it will override columns got from file
    private final List<Column> csvSchema = Lists.newArrayList();

    String defaultColumnSeparator = CsvFileFormatProperties.DEFAULT_COLUMN_SEPARATOR;

    public CsvFileFormatConfigurator(TFileFormatType fileFormatType) {
        super(fileFormatType);
    }

    public CsvFileFormatConfigurator(TFileFormatType fileFormatType, String defaultColumnSeparator,
            TTextSerdeType textSerdeType) {
        super(fileFormatType);
        this.defaultColumnSeparator = defaultColumnSeparator;
        this.textSerdeType = textSerdeType;
    }

    public CsvFileFormatConfigurator(TFileFormatType fileFormatType, String headerType) {
        super(fileFormatType);
        this.headerType = headerType;
    }


    @Override
    public void analyzeFileFormatProperties(Map<String, String> formatProperties)
            throws AnalysisException {
        try {
            // check properties specified by user -- formatProperties
            columnSeparator = formatProperties.getOrDefault(CsvFileFormatProperties.PROP_COLUMN_SEPARATOR,
                    defaultColumnSeparator);
            if (Strings.isNullOrEmpty(columnSeparator)) {
                throw new AnalysisException("column_separator can not be empty.");
            }
            columnSeparator = Separator.convertSeparator(columnSeparator);

            lineDelimiter = formatProperties.getOrDefault(CsvFileFormatProperties.PROP_LINE_DELIMITER,
                    CsvFileFormatProperties.DEFAULT_LINE_DELIMITER);
            if (Strings.isNullOrEmpty(lineDelimiter)) {
                throw new AnalysisException("line_delimiter can not be empty.");
            }
            lineDelimiter = Separator.convertSeparator(lineDelimiter);

            String enclosedString = formatProperties.getOrDefault(CsvFileFormatProperties.PROP_ENCLOSE, "");
            if (!Strings.isNullOrEmpty(enclosedString)) {
                if (enclosedString.length() > 1) {
                    throw new AnalysisException("enclose should not be longer than one byte.");
                }
                enclose = (byte) enclosedString.charAt(0);
                if (enclose == 0) {
                    throw new AnalysisException("enclose should not be byte [0].");
                }
            }

            trimDoubleQuotes = Boolean.valueOf(
                    formatProperties.getOrDefault(CsvFileFormatProperties.PROP_TRIM_DOUBLE_QUOTES, "")).booleanValue();
            skipLines = Integer.valueOf(
                    formatProperties.getOrDefault(CsvFileFormatProperties.PROP_SKIP_LINES, "0")).intValue();

            String compressTypeStr = formatProperties.getOrDefault(CsvFileFormatProperties.PROP_COMPRESS_TYPE,
                    "UNKNOWN");
            try {
                compressionType = Util.getFileCompressType(compressTypeStr);
            } catch (IllegalArgumentException e) {
                throw new AnalysisException("Compress type : " +  compressTypeStr + " is not supported.");
            }

            FileFormatUtils.parseCsvSchema(csvSchema, formatProperties.getOrDefault(
                    CsvFileFormatProperties.PROP_CSV_SCHEMA, ""));
            if (LOG.isDebugEnabled()) {
                LOG.debug("get csv schema: {}", csvSchema);
            }
        } catch (org.apache.doris.common.AnalysisException e) {
            throw new AnalysisException(e.getMessage());
        }
    }

    @Override
    public PFetchTableSchemaRequest toPFetchTableSchemaRequest() {
        return null;
    }

    @Override
    public TResultFileSinkOptions toTResultFileSinkOptions() {
        return null;
    }

    @Override
    public TFileAttributes toTFileAttributes() {
        return null;
    }

    public String getHeaderType() {
        return headerType;
    }

    public TTextSerdeType getTextSerdeType() {
        return textSerdeType;
    }

    public String getColumnSeparator() {
        return columnSeparator;
    }

    public String getLineDelimiter() {
        return lineDelimiter;
    }

    public boolean isTrimDoubleQuotes() {
        return trimDoubleQuotes;
    }

    public int getSkipLines() {
        return skipLines;
    }

    public byte getEnclose() {
        return enclose;
    }

    public List<Column> getCsvSchema() {
        return csvSchema;
    }

    public static class CsvFileFormatProperties {
        public static final String DEFAULT_COLUMN_SEPARATOR = "\t";
        public static final String DEFAULT_HIVE_TEXT_COLUMN_SEPARATOR = "\001";
        public static final String DEFAULT_LINE_DELIMITER = "\n";

        public static final String PROP_COLUMN_SEPARATOR = "column_separator";
        public static final String PROP_LINE_DELIMITER = "line_delimiter";

        public static final String PROP_SKIP_LINES = "skip_lines";
        public static final String PROP_CSV_SCHEMA = "csv_schema";
        public static final String PROP_COMPRESS = "compress";
        public static final String PROP_COMPRESS_TYPE = "compress_type";
        public static final String PROP_TRIM_DOUBLE_QUOTES = "trim_double_quotes";

        public static final String PROP_ENCLOSE = "enclose";
    }
}
