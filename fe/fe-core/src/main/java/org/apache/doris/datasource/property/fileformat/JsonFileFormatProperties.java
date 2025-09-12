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
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileTextScanRangeParams;
import org.apache.doris.thrift.TResultFileSinkOptions;

import com.google.common.base.Strings;

import java.util.Map;

public class JsonFileFormatProperties extends FileFormatProperties {
    public static final String PROP_JSON_ROOT = "json_root";
    public static final String PROP_JSON_PATHS = "jsonpaths";
    public static final String PROP_STRIP_OUTER_ARRAY = "strip_outer_array";
    public static final String PROP_READ_JSON_BY_LINE = "read_json_by_line";
    public static final String PROP_NUM_AS_STRING = "num_as_string";
    public static final String PROP_FUZZY_PARSE = "fuzzy_parse";

    // from ExternalFileTableValuedFunction:
    private String jsonRoot = "";
    private String jsonPaths = "";
    private boolean stripOuterArray = false;
    private boolean readJsonByLine;
    private boolean numAsString = false;
    private boolean fuzzyParse = false;
    private String lineDelimiter = CsvFileFormatProperties.DEFAULT_LINE_DELIMITER;


    public JsonFileFormatProperties() {
        super(TFileFormatType.FORMAT_JSON, FileFormatProperties.FORMAT_JSON);
    }

    @Override
    public void analyzeFileFormatProperties(Map<String, String> formatProperties, boolean isRemoveOriginProperty)
            throws AnalysisException {
        try {
            jsonRoot = getOrDefault(formatProperties, PROP_JSON_ROOT,
                    "", isRemoveOriginProperty);
            jsonPaths = getOrDefault(formatProperties, PROP_JSON_PATHS,
                    "", isRemoveOriginProperty);
            readJsonByLine = Boolean.valueOf(
                    getOrDefault(formatProperties, PROP_READ_JSON_BY_LINE,
                            "true", isRemoveOriginProperty)).booleanValue();
            stripOuterArray = Boolean.valueOf(
                    getOrDefault(formatProperties, PROP_STRIP_OUTER_ARRAY,
                            "", isRemoveOriginProperty)).booleanValue();
            numAsString = Boolean.valueOf(
                    getOrDefault(formatProperties, PROP_NUM_AS_STRING,
                            "", isRemoveOriginProperty)).booleanValue();
            fuzzyParse = Boolean.valueOf(
                    getOrDefault(formatProperties, PROP_FUZZY_PARSE,
                            "", isRemoveOriginProperty)).booleanValue();
            lineDelimiter = getOrDefault(formatProperties, CsvFileFormatProperties.PROP_LINE_DELIMITER,
                    CsvFileFormatProperties.DEFAULT_LINE_DELIMITER, isRemoveOriginProperty);
            if (Strings.isNullOrEmpty(lineDelimiter)) {
                throw new AnalysisException("line_delimiter can not be empty.");
            }
            lineDelimiter = Separator.convertSeparator(lineDelimiter);
            // (TODO Refrain) if both are set to true, read_json_by_line will be ignored
            if (stripOuterArray) {
                readJsonByLine = false;
            }
            String compressTypeStr = getOrDefault(formatProperties, PROP_COMPRESS_TYPE,
                    "UNKNOWN", isRemoveOriginProperty);
            compressionType = Util.getFileCompressType(compressTypeStr);
        } catch (org.apache.doris.common.AnalysisException e) {
            throw new AnalysisException("Analyze file format failed: " + e.getMessage());
        }
    }

    @Override
    public void fullTResultFileSinkOptions(TResultFileSinkOptions sinkOptions) {
        sinkOptions.setLineDelimiter(lineDelimiter);
    }

    @Override
    public TFileAttributes toTFileAttributes() {
        TFileAttributes fileAttributes = new TFileAttributes();
        TFileTextScanRangeParams fileTextScanRangeParams = new TFileTextScanRangeParams();
        fileTextScanRangeParams.setLineDelimiter(this.lineDelimiter);
        fileAttributes.setTextParams(fileTextScanRangeParams);
        fileAttributes.setJsonRoot(jsonRoot);
        fileAttributes.setJsonpaths(jsonPaths);
        fileAttributes.setReadJsonByLine(readJsonByLine);
        fileAttributes.setStripOuterArray(stripOuterArray);
        fileAttributes.setNumAsString(numAsString);
        fileAttributes.setFuzzyParse(fuzzyParse);
        return fileAttributes;
    }

    public String getJsonRoot() {
        return jsonRoot;
    }

    public String getJsonPaths() {
        return jsonPaths;
    }

    public boolean isStripOuterArray() {
        return stripOuterArray;
    }

    public boolean isReadJsonByLine() {
        return readJsonByLine;
    }

    public boolean isNumAsString() {
        return numAsString;
    }

    public boolean isFuzzyParse() {
        return fuzzyParse;
    }

    public String getLineDelimiter() {
        return lineDelimiter;
    }
}
