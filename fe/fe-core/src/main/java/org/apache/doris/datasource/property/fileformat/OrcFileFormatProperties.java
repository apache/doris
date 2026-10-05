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
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileTextScanRangeParams;
import org.apache.doris.thrift.TResultFileSinkOptions;

import com.google.common.collect.Maps;

import java.util.Map;

public class OrcFileFormatProperties extends FileFormatProperties {
    public static final Map<String, TFileCompressType> ORC_COMPRESSION_TYPE_MAP = Maps.newHashMap();

    static {
        ORC_COMPRESSION_TYPE_MAP.put("plain", TFileCompressType.PLAIN);
        ORC_COMPRESSION_TYPE_MAP.put("snappy", TFileCompressType.SNAPPYBLOCK);
        ORC_COMPRESSION_TYPE_MAP.put("zlib", TFileCompressType.ZLIB);
        ORC_COMPRESSION_TYPE_MAP.put("zstd", TFileCompressType.ZSTD);
    }

    private TFileCompressType orcCompressionType = TFileCompressType.ZLIB;

    public OrcFileFormatProperties() {
        super(TFileFormatType.FORMAT_ORC, FileFormatProperties.FORMAT_ORC);
    }

    @Override
    public void analyzeFileFormatProperties(Map<String, String> formatProperties, boolean isRemoveOriginProperty)
            throws AnalysisException {
        // get compression type
        // save compress type
        if (formatProperties.containsKey(PROP_COMPRESS_TYPE)) {
            if (ORC_COMPRESSION_TYPE_MAP.containsKey(
                    formatProperties.get(PROP_COMPRESS_TYPE).toLowerCase())) {
                this.orcCompressionType = ORC_COMPRESSION_TYPE_MAP.get(
                        formatProperties.get(PROP_COMPRESS_TYPE).toLowerCase());
                formatProperties.remove(PROP_COMPRESS_TYPE);
            } else {
                throw new AnalysisException("orc compression type ["
                        + formatProperties.get(PROP_COMPRESS_TYPE) + "] is invalid,"
                        + " please choose one among ZLIB, SNAPPY, ZSTD or PLAIN");
            }
        }
    }

    @Override
    public void fullTResultFileSinkOptions(TResultFileSinkOptions sinkOptions) {
        sinkOptions.setOrcCompressionType(orcCompressionType);
        sinkOptions.setOrcWriterVersion(1);
    }

    @Override
    public TFileAttributes toTFileAttributes() {
        TFileAttributes fileAttributes = new TFileAttributes();
        TFileTextScanRangeParams fileTextScanRangeParams = new TFileTextScanRangeParams();
        fileAttributes.setTextParams(fileTextScanRangeParams);
        return fileAttributes;
    }

    public TFileCompressType getOrcCompressionType() {
        return orcCompressionType;
    }
}
