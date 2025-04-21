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

import org.apache.doris.common.util.FileFormatConstants;
import org.apache.doris.common.util.Util;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.proto.InternalService.PFetchTableSchemaRequest;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileTextScanRangeParams;
import org.apache.doris.thrift.TResultFileSinkOptions;

import java.util.Map;

public class JsonFileFormatConfigurator extends FileFormatConfigurator {
    // from ExternalFileTableValuedFunction:
    private String jsonRoot = "";
    private String jsonPaths = "";
    private boolean stripOuterArray;
    private boolean readJsonByLine;
    private boolean numAsString;
    private boolean fuzzyParse;


    public JsonFileFormatConfigurator(TFileFormatType fileFormatType) {
        super(fileFormatType);
    }

    @Override
    public void analyzeFileFormatProperties(Map<String, String> formatProperties, boolean isRemoveOriginProperty)
            throws AnalysisException {
        // 这几个json应该移到json checker中
        jsonRoot = getOrDefaultAndRemove(formatProperties, FileFormatConstants.PROP_JSON_ROOT,
                "", isRemoveOriginProperty);
        jsonPaths = getOrDefaultAndRemove(formatProperties, FileFormatConstants.PROP_JSON_PATHS,
                "", isRemoveOriginProperty);
        readJsonByLine = Boolean.valueOf(
                getOrDefaultAndRemove(formatProperties, FileFormatConstants.PROP_READ_JSON_BY_LINE,
                        "", isRemoveOriginProperty)).booleanValue();
        stripOuterArray = Boolean.valueOf(
                getOrDefaultAndRemove(formatProperties, FileFormatConstants.PROP_STRIP_OUTER_ARRAY,
                        "", isRemoveOriginProperty)).booleanValue();
        numAsString = Boolean.valueOf(
                getOrDefaultAndRemove(formatProperties, FileFormatConstants.PROP_NUM_AS_STRING,
                        "", isRemoveOriginProperty)).booleanValue();
        fuzzyParse = Boolean.valueOf(
                getOrDefaultAndRemove(formatProperties, FileFormatConstants.PROP_FUZZY_PARSE,
                        "", isRemoveOriginProperty)).booleanValue();

        String compressTypeStr = getOrDefaultAndRemove(formatProperties, FileFormatConstants.PROP_COMPRESS_TYPE,
                "UNKNOWN", isRemoveOriginProperty);
        try {
            compressionType = Util.getFileCompressType(compressTypeStr);
        } catch (IllegalArgumentException e) {
            throw new AnalysisException("Compress type : " +  compressTypeStr + " is not supported.");
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
        TFileAttributes fileAttributes = new TFileAttributes();
        TFileTextScanRangeParams fileTextScanRangeParams = new TFileTextScanRangeParams();
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


    public static class JsonFileFormatProperties {
        public static final String PROP_JSON_ROOT = "json_root";
        public static final String PROP_JSON_PATHS = "jsonpaths";
        public static final String PROP_STRIP_OUTER_ARRAY = "strip_outer_array";
        public static final String PROP_READ_JSON_BY_LINE = "read_json_by_line";
        public static final String PROP_NUM_AS_STRING = "num_as_string";
        public static final String PROP_FUZZY_PARSE = "fuzzy_parse";
    }
}
