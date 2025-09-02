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
import org.apache.doris.common.util.Util;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileTextScanRangeParams;
import org.apache.doris.thrift.TResultFileSinkOptions;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Set;

public class CsvFileFormatProperties extends FileFormatProperties {
    public static final Logger LOG = LogManager.getLogger(
            org.apache.doris.datasource.property.fileformat.CsvFileFormatProperties.class);

    // supported compression types for csv writer
    public static final Set<TFileCompressType> SUPPORTED_CSV_WRITE_COMPRESSION_TYPES = Sets.newHashSet();

    static {
        SUPPORTED_CSV_WRITE_COMPRESSION_TYPES.add(TFileCompressType.PLAIN);
        SUPPORTED_CSV_WRITE_COMPRESSION_TYPES.add(TFileCompressType.GZ);
        SUPPORTED_CSV_WRITE_COMPRESSION_TYPES.add(TFileCompressType.BZ2);
        SUPPORTED_CSV_WRITE_COMPRESSION_TYPES.add(TFileCompressType.SNAPPYBLOCK);
        SUPPORTED_CSV_WRITE_COMPRESSION_TYPES.add(TFileCompressType.LZ4BLOCK);
        SUPPORTED_CSV_WRITE_COMPRESSION_TYPES.add(TFileCompressType.ZSTD);
    }

    public static final String DEFAULT_COLUMN_SEPARATOR = "\t";
    public static final String DEFAULT_LINE_DELIMITER = "\n";

    public static final String PROP_COLUMN_SEPARATOR = "column_separator";
    public static final String PROP_LINE_DELIMITER = "line_delimiter";

    public static final String PROP_SKIP_LINES = "skip_lines";
    public static final String PROP_CSV_SCHEMA = "csv_schema";
    public static final String PROP_COMPRESS_TYPE = "compress_type";
    public static final String PROP_TRIM_DOUBLE_QUOTES = "trim_double_quotes";

    public static final String PROP_ENCLOSE = "enclose";
    public static final String PROP_ESCAPE = "escape";

    public static final String PROP_ENABLE_TEXT_VALIDATE_UTF8 = "enable_text_validate_utf8";

    private String headerType = "";
    private String columnSeparator = DEFAULT_COLUMN_SEPARATOR;
    private String lineDelimiter = DEFAULT_LINE_DELIMITER;
    private boolean trimDoubleQuotes;
    private int skipLines;
    private byte enclose;
    private byte escape;
    private boolean enableTextValidateUTF8 = true;

    public CsvFileFormatProperties(String formatName) {
        super(TFileFormatType.FORMAT_CSV_PLAIN, formatName);
    }

    public CsvFileFormatProperties(String headerType, String formatName) {
        super(TFileFormatType.FORMAT_CSV_PLAIN, formatName);
        this.headerType = headerType;
    }

    @Override
    public void analyzeFileFormatProperties(Map<String, String> formatProperties, boolean isRemoveOriginProperty)
            throws AnalysisException {
        try {
            // analyze properties specified by user
            columnSeparator = getOrDefault(formatProperties, PROP_COLUMN_SEPARATOR,
                    DEFAULT_COLUMN_SEPARATOR, isRemoveOriginProperty);
            if (Strings.isNullOrEmpty(columnSeparator)) {
                throw new AnalysisException("column_separator can not be empty.");
            }
            columnSeparator = Separator.convertSeparator(columnSeparator);

            lineDelimiter = getOrDefault(formatProperties, PROP_LINE_DELIMITER,
                    DEFAULT_LINE_DELIMITER, isRemoveOriginProperty);
            if (Strings.isNullOrEmpty(lineDelimiter)) {
                throw new AnalysisException("line_delimiter can not be empty.");
            }
            lineDelimiter = Separator.convertSeparator(lineDelimiter);

            String enclosedString = getOrDefault(formatProperties, PROP_ENCLOSE,
                    "", isRemoveOriginProperty);
            if (!Strings.isNullOrEmpty(enclosedString)) {
                if (enclosedString.length() > 1) {
                    throw new AnalysisException("enclose should not be longer than one byte.");
                }
                enclose = (byte) enclosedString.charAt(0);
            }

            String escapeStr = getOrDefault(formatProperties, PROP_ESCAPE,
                    "", isRemoveOriginProperty);
            if (!Strings.isNullOrEmpty(escapeStr)) {
                if (escapeStr.length() != 1) {
                    throw new AnalysisException("escape must be single-char");
                } else {
                    escape = escapeStr.getBytes()[0];
                }
            }

            trimDoubleQuotes = Boolean.valueOf(getOrDefault(formatProperties,
                    PROP_TRIM_DOUBLE_QUOTES, "", isRemoveOriginProperty))
                    .booleanValue();
            skipLines = Integer.valueOf(getOrDefault(formatProperties,
                    PROP_SKIP_LINES, "0", isRemoveOriginProperty)).intValue();
            if (skipLines < 0) {
                throw new AnalysisException("skipLines should not be less than 0.");
            }

            // This default value is "UNKNOWN", so that the caller may infer the compression type by suffix of file.
            String compressTypeStr = getOrDefault(formatProperties,
                    PROP_COMPRESS_TYPE, "UNKNOWN", isRemoveOriginProperty);
            compressionType = Util.getFileCompressType(compressTypeStr);

            // get ENABLE_TEXT_VALIDATE_UTF8 from properties map first,
            // if not exist, try getting from session variable,
            // if connection context is null, use "true" as default value.
            String validateUtf8 = getOrDefault(formatProperties, PROP_ENABLE_TEXT_VALIDATE_UTF8, "",
                    isRemoveOriginProperty);
            if (Strings.isNullOrEmpty(validateUtf8)) {
                enableTextValidateUTF8 = ConnectContext.get() == null ? true
                        : ConnectContext.get().getSessionVariable().enableTextValidateUtf8;
            } else {
                enableTextValidateUTF8 = Boolean.parseBoolean(validateUtf8);
            }

        } catch (org.apache.doris.common.AnalysisException e) {
            throw new AnalysisException(e.getMessage());
        }
    }

    public void checkSupportedCompressionType(boolean isWrite) {
        // Currently, only check for write operation.
        // Because we only support a subset of compression type for writing.
        if (isWrite) {
            // "UNKNOWN" means user does not specify the compression type
            if (this.compressionType == TFileCompressType.UNKNOWN) {
                this.compressionType = TFileCompressType.PLAIN;
            }
            if (!SUPPORTED_CSV_WRITE_COMPRESSION_TYPES.contains(this.compressionType)) {
                throw new AnalysisException(
                        "csv compression type [" + this.compressionType.name() + "] is invalid for writing");
            }
        }
    }

    @Override
    public void fullTResultFileSinkOptions(TResultFileSinkOptions sinkOptions) {
        sinkOptions.setColumnSeparator(columnSeparator);
        sinkOptions.setLineDelimiter(lineDelimiter);
        sinkOptions.setCompressionType(compressionType);
    }

    // The method `analyzeFileFormatProperties` must have been called once before this method
    @Override
    public TFileAttributes toTFileAttributes() {
        TFileAttributes fileAttributes = new TFileAttributes();
        TFileTextScanRangeParams fileTextScanRangeParams = new TFileTextScanRangeParams();
        fileTextScanRangeParams.setColumnSeparator(this.columnSeparator);
        fileTextScanRangeParams.setLineDelimiter(this.lineDelimiter);
        fileTextScanRangeParams.setEnclose(this.enclose);
        fileTextScanRangeParams.setEscape(this.escape);
        fileAttributes.setTextParams(fileTextScanRangeParams);
        fileAttributes.setHeaderType(headerType);
        fileAttributes.setTrimDoubleQuotes(trimDoubleQuotes);
        fileAttributes.setSkipLines(skipLines);
        fileAttributes.setEnableTextValidateUtf8(enableTextValidateUTF8);
        return fileAttributes;
    }

    public String getHeaderType() {
        return headerType;
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

    public byte getEscape() {
        return escape;
    }
}
