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

import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.property.fileformat.CsvFileFormatConfigurator.CsvFileFormatProperties;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.proto.InternalService.PFetchTableSchemaRequest;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileFormatType;
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
    public void analyzeFileFormatProperties(Map<String, String> formatProperties)
            throws AnalysisException {
        // 这几个json应该移到json checker中
        jsonRoot = formatProperties.getOrDefault(JsonFileFormatProperties.PROP_JSON_ROOT, "");
        jsonPaths = formatProperties.getOrDefault(JsonFileFormatProperties.PROP_JSON_PATHS, "");
        readJsonByLine = Boolean.valueOf(
                formatProperties.getOrDefault(JsonFileFormatProperties.PROP_READ_JSON_BY_LINE, "")).booleanValue();
        stripOuterArray = Boolean.valueOf(
                formatProperties.getOrDefault(JsonFileFormatProperties.PROP_STRIP_OUTER_ARRAY, "")).booleanValue();
        numAsString = Boolean.valueOf(
                formatProperties.getOrDefault(JsonFileFormatProperties.PROP_NUM_AS_STRING, "")).booleanValue();
        fuzzyParse = Boolean.valueOf(
                formatProperties.getOrDefault(JsonFileFormatProperties.PROP_FUZZY_PARSE, "")).booleanValue();

        String compressTypeStr = formatProperties.getOrDefault(CsvFileFormatProperties.PROP_COMPRESS_TYPE,
                "UNKNOWN");
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
        return null;
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
