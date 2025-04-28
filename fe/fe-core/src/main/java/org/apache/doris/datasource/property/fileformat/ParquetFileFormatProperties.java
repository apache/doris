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
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileTextScanRangeParams;
import org.apache.doris.thrift.TParquetCompressionType;
import org.apache.doris.thrift.TParquetVersion;
import org.apache.doris.thrift.TResultFileSinkOptions;

import java.util.Map;

public class ParquetFileFormatProperties extends FileFormatProperties {
    public static final String PARQUET_DISABLE_DICTIONARY = "disable_dictionary";
    public static final TParquetVersion parquetVersion = TParquetVersion.PARQUET_1_0;
    public static final String PARQUET_VERSION = "version";

    private TParquetCompressionType parquetCompressionType = TParquetCompressionType.SNAPPY;
    private boolean parquetDisableDictionary = false;

    public ParquetFileFormatProperties() {
        super(TFileFormatType.FORMAT_PARQUET);
    }

    @Override
    public void analyzeFileFormatProperties(Map<String, String> formatProperties, boolean isRemoveOriginProperty)
            throws AnalysisException {
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
        return fileAttributes;
    }

    public TParquetCompressionType getParquetCompressionType() {
        return parquetCompressionType;
    }

    public boolean isParquetDisableDictionary() {
        return parquetDisableDictionary;
    }
}
