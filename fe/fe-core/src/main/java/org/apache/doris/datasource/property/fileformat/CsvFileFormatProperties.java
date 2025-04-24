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
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.property.constants.CsvProperties;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileTextScanRangeParams;
import org.apache.doris.thrift.TResultFileSinkOptions;
import org.apache.doris.thrift.TTextSerdeType;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class CsvFileFormatProperties extends FileFormatProperties {
    public static final Logger LOG = LogManager.getLogger(
            org.apache.doris.datasource.property.fileformat.CsvFileFormatProperties.class);

    private String headerType = "";
    private TTextSerdeType textSerdeType = TTextSerdeType.JSON_TEXT_SERDE;
    private String columnSeparator = CsvProperties.DEFAULT_COLUMN_SEPARATOR;
    private String lineDelimiter = CsvProperties.DEFAULT_LINE_DELIMITER;
    private boolean trimDoubleQuotes;
    private int skipLines;
    private byte enclose;

    // used by tvf
    // User specified csv columns, it will override columns got from file
    private final List<Column> csvSchema = Lists.newArrayList();

    String defaultColumnSeparator = CsvProperties.DEFAULT_COLUMN_SEPARATOR;

    public CsvFileFormatProperties() {
        super(TFileFormatType.FORMAT_CSV_PLAIN);
    }

    public CsvFileFormatProperties(String defaultColumnSeparator, TTextSerdeType textSerdeType) {
        super(TFileFormatType.FORMAT_CSV_PLAIN);
        this.defaultColumnSeparator = defaultColumnSeparator;
        this.textSerdeType = textSerdeType;
    }

    public CsvFileFormatProperties(String headerType) {
        super(TFileFormatType.FORMAT_CSV_PLAIN);
        this.headerType = headerType;
    }


    @Override
    public void analyzeFileFormatProperties(Map<String, String> formatProperties, boolean isRemoveOriginProperty)
            throws AnalysisException {
        try {
            // analyze properties specified by user
            columnSeparator = getOrDefault(formatProperties, CsvProperties.PROP_COLUMN_SEPARATOR,
                    defaultColumnSeparator, isRemoveOriginProperty);
            if (Strings.isNullOrEmpty(columnSeparator)) {
                throw new AnalysisException("column_separator can not be empty.");
            }
            columnSeparator = Separator.convertSeparator(columnSeparator);

            lineDelimiter = getOrDefault(formatProperties, CsvProperties.PROP_LINE_DELIMITER,
                    CsvProperties.DEFAULT_LINE_DELIMITER, isRemoveOriginProperty);
            if (Strings.isNullOrEmpty(lineDelimiter)) {
                throw new AnalysisException("line_delimiter can not be empty.");
            }
            lineDelimiter = Separator.convertSeparator(lineDelimiter);

            String enclosedString = getOrDefault(formatProperties, CsvProperties.PROP_ENCLOSE,
                    "", isRemoveOriginProperty);
            if (!Strings.isNullOrEmpty(enclosedString)) {
                if (enclosedString.length() > 1) {
                    throw new AnalysisException("enclose should not be longer than one byte.");
                }
                enclose = (byte) enclosedString.charAt(0);
                if (enclose == 0) {
                    throw new AnalysisException("enclose should not be byte [0].");
                }
            }

            trimDoubleQuotes = Boolean.valueOf(getOrDefault(formatProperties,
                    CsvProperties.PROP_TRIM_DOUBLE_QUOTES, "", isRemoveOriginProperty))
                    .booleanValue();
            skipLines = Integer.valueOf(getOrDefault(formatProperties,
                    CsvProperties.PROP_SKIP_LINES, "0", isRemoveOriginProperty)).intValue();
            if (skipLines < 0) {
                throw new AnalysisException("skipLines should not be less than 0.");
            }

            String compressTypeStr = getOrDefault(formatProperties,
                    CsvProperties.PROP_COMPRESS_TYPE, "UNKNOWN", isRemoveOriginProperty);
            compressionType = Util.getFileCompressType(compressTypeStr);

        } catch (org.apache.doris.common.AnalysisException e) {
            throw new AnalysisException(e.getMessage());
        }
    }

    @Override
    public TResultFileSinkOptions toTResultFileSinkOptions() {
        return null;
    }

    // The method `analyzeFileFormatProperties` must have been called once before this method
    @Override
    public TFileAttributes toTFileAttributes() {
        TFileAttributes fileAttributes = new TFileAttributes();
        TFileTextScanRangeParams fileTextScanRangeParams = new TFileTextScanRangeParams();
        fileTextScanRangeParams.setColumnSeparator(this.columnSeparator);
        fileTextScanRangeParams.setLineDelimiter(this.lineDelimiter);
        if (this.enclose != 0) {
            fileTextScanRangeParams.setEnclose(this.enclose);
        }
        fileAttributes.setTextParams(fileTextScanRangeParams);
        fileAttributes.setHeaderType(headerType);
        fileAttributes.setTrimDoubleQuotes(trimDoubleQuotes);
        fileAttributes.setSkipLines(skipLines);
        fileAttributes.setEnableTextValidateUtf8(
                ConnectContext.get().getSessionVariable().enableTextValidateUtf8);
        return fileAttributes;
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
}
