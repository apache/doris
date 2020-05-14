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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.ParseUtil;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TResultFileSinkOptions;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import java.util.Map;

// For syntax select * from tbl INTO OUTFILE xxxx
public class OutFileClause {

    private static final String PROP_COLUMN_SEPARATOR = "column_separator";
    private static final String PROP_LINE_DELIMITER = "line_delimiter";
    private static final String PROP_MAX_FILE_SIZE_BYTES = "max_file_size_bytes";

    private static final long DEFAULT_MAX_FILE_SIZE_BYTES = 1 * 1024 * 1024 * 1024; // 1GB
    private static final long MIN_FILE_SIZE_BYTES = 5 * 1024 * 1024L; // 5MB
    private static final long MAX_FILE_SIZE_BYTES = 2 * 1024 * 1024 * 1024L; // 2GB

    private String filePath;
    private String format;
    private BrokerDesc brokerDesc;
    private Map<String, String> properties;

    // set following members after analyzing
    private String columnSeparator = "\t";
    private String lineDelimiter = "\n";
    private TFileFormatType fileFormatType;
    private long maxFileSizeBytes = DEFAULT_MAX_FILE_SIZE_BYTES;

    public OutFileClause(String filePath, String format, BrokerDesc brokerDesc, Map<String, String> properties) {
        this.filePath = filePath;
        this.format = Strings.isNullOrEmpty(format) ? "csv" : format.toLowerCase();
        this.brokerDesc = brokerDesc;
        this.properties = properties;
    }

    public OutFileClause(OutFileClause other) {
        this.filePath = other.filePath;
        this.format = other.format;
        this.brokerDesc = new BrokerDesc(other.getBrokerDesc().getName(), other.getBrokerDesc().getProperties());
        this.properties = other.properties == null ? null : Maps.newHashMap(other.properties);
    }

    public String getColumnSeparator() {
        return columnSeparator;
    }

    public String getLineDelimiter() {
        return lineDelimiter;
    }

    public TFileFormatType getFileFormatType() {
        return fileFormatType;
    }

    public long getMaxFileSizeBytes() {
        return maxFileSizeBytes;
    }

    public BrokerDesc getBrokerDesc() {
        return brokerDesc;
    }

    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (Strings.isNullOrEmpty(filePath)) {
            throw new AnalysisException("Must specify file in OUTFILE clause");
        }

        if (!format.equals("csv")) {
            throw new AnalysisException("Only support CSV format");
        }
        fileFormatType = TFileFormatType.FORMAT_CSV_PLAIN;

        if (brokerDesc == null) {
            throw new AnalysisException("Must specify BROKER in OUTFILE clause");
        }

        analyzeProperties();
    }

    private void analyzeProperties() throws AnalysisException {
        if (properties == null || properties.isEmpty()) {
            return;
        }
        
        if (properties.containsKey(PROP_COLUMN_SEPARATOR)) {
            if (!isCsvFormat()) {
                throw new AnalysisException(PROP_COLUMN_SEPARATOR + " is only for CSV format");
            }
            columnSeparator = properties.get(PROP_COLUMN_SEPARATOR);
        }
        
        if (properties.containsKey(PROP_LINE_DELIMITER)) {
            if (!isCsvFormat()) {
                throw new AnalysisException(PROP_LINE_DELIMITER + " is only for CSV format");
            }
            lineDelimiter = properties.get(PROP_LINE_DELIMITER);
        }

        if (properties.containsKey(PROP_MAX_FILE_SIZE_BYTES)) {
            maxFileSizeBytes = ParseUtil.analyzeDataVolumn(properties.get(PROP_MAX_FILE_SIZE_BYTES));
            if (maxFileSizeBytes > MAX_FILE_SIZE_BYTES || maxFileSizeBytes < MIN_FILE_SIZE_BYTES) {
                throw new AnalysisException("max file size should between 5MB and 2GB. Given: " + maxFileSizeBytes);
            }
        }
    }

    private boolean isCsvFormat() {
        return fileFormatType == TFileFormatType.FORMAT_CSV_BZ2
                || fileFormatType == TFileFormatType.FORMAT_CSV_DEFLATE
                || fileFormatType == TFileFormatType.FORMAT_CSV_GZ
                || fileFormatType == TFileFormatType.FORMAT_CSV_LZ4FRAME
                || fileFormatType == TFileFormatType.FORMAT_CSV_LZO
                || fileFormatType == TFileFormatType.FORMAT_CSV_LZOP
                || fileFormatType == TFileFormatType.FORMAT_CSV_PLAIN;
    }

    @Override
    public OutFileClause clone() {
        return new OutFileClause(this);
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append(" INTO OUTFILE '").append(filePath).append(" FORMAT AS ").append(format);
        sb.append(brokerDesc.toSql());
        if (properties != null && !properties.isEmpty()) {
            sb.append(" PROPERTIES(");
            sb.append(new PrintableMap<>(properties, " = ", true, false));
            sb.append(")");
        }
        return sb.toString();
    }

    public TResultFileSinkOptions toSinkOptions() {
        TResultFileSinkOptions sinkOptions = new TResultFileSinkOptions(filePath, fileFormatType);
        if (isCsvFormat()) {
            sinkOptions.setColumn_separator(columnSeparator);
            sinkOptions.setLine_delimiter(lineDelimiter);
        }
        sinkOptions.setMax_file_size_bytes(maxFileSizeBytes);
        if (brokerDesc != null) {
            sinkOptions.setBroker_properties(brokerDesc.getProperties());
            // broker_addresses of sinkOptions will be set in Coordinator.
            // Because we need to choose the nearest broker with the result sink node.
        }
        return sinkOptions;
    }
}

