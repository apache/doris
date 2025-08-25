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
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileTextScanRangeParams;
import org.apache.doris.thrift.TResultFileSinkOptions;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class TextFileFormatProperties extends FileFormatProperties {
    public static final Logger LOG = LogManager.getLogger(
            org.apache.doris.datasource.property.fileformat.TextFileFormatProperties.class);

    public static final String DEFAULT_COLUMN_SEPARATOR = "\001";
    public static final String DEFAULT_LINE_DELIMITER = "\n";

    public static final String PROP_COLUMN_SEPARATOR = "column_separator";
    public static final String PROP_LINE_DELIMITER = "line_delimiter";

    public static final String PROP_SKIP_LINES = "skip_lines";
    public static final String PROP_COMPRESS_TYPE = "compress_type";

    private String columnSeparator = DEFAULT_COLUMN_SEPARATOR;
    private String lineDelimiter = DEFAULT_LINE_DELIMITER;
    private int skipLines;

    public TextFileFormatProperties() {
        super(TFileFormatType.FORMAT_TEXT, FileFormatProperties.FORMAT_HIVE_TEXT);
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

            skipLines = Integer.valueOf(getOrDefault(formatProperties,
                    PROP_SKIP_LINES, "0", isRemoveOriginProperty)).intValue();
            if (skipLines < 0) {
                throw new AnalysisException("skipLines should not be less than 0.");
            }

            String compressTypeStr = getOrDefault(formatProperties,
                    PROP_COMPRESS_TYPE, "UNKNOWN", isRemoveOriginProperty);
            compressionType = Util.getFileCompressType(compressTypeStr);

        } catch (org.apache.doris.common.AnalysisException e) {
            throw new AnalysisException(e.getMessage());
        }
    }

    @Override
    public void fullTResultFileSinkOptions(TResultFileSinkOptions sinkOptions) {
        sinkOptions.setColumnSeparator(columnSeparator);
        sinkOptions.setLineDelimiter(lineDelimiter);
    }

    // The method `analyzeFileFormatProperties` must have been called once before
    // this method
    @Override
    public TFileAttributes toTFileAttributes() {
        TFileAttributes fileAttributes = new TFileAttributes();
        TFileTextScanRangeParams fileTextScanRangeParams = new TFileTextScanRangeParams();
        fileTextScanRangeParams.setColumnSeparator(this.columnSeparator);
        fileTextScanRangeParams.setLineDelimiter(this.lineDelimiter);

        fileAttributes.setTextParams(fileTextScanRangeParams);
        fileAttributes.setSkipLines(skipLines);
        fileAttributes.setEnableTextValidateUtf8(
                ConnectContext.get().getSessionVariable().enableTextValidateUtf8);
        return fileAttributes;
    }

    public String getColumnSeparator() {
        return columnSeparator;
    }

    public String getLineDelimiter() {
        return lineDelimiter;
    }

    public int getSkipLines() {
        return skipLines;
    }
}
