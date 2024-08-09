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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/DataSink.java
// and modified by Doris

package org.apache.doris.planner;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertCommandContext;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;

import java.util.Optional;
import java.util.Set;

public abstract class BaseExternalTableDataSink extends DataSink {

    protected TDataSink tDataSink;

    @Override
    protected TDataSink toThrift() {
        return tDataSink;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return null;
    }

    @Override
    public DataPartition getOutputPartition() {
        return DataPartition.RANDOM;
    }

    /**
     * File format types supported by the current table
     */
    protected abstract Set<TFileFormatType> supportedFileFormatTypes();

    protected TFileFormatType getTFileFormatType(String format) throws AnalysisException {
        TFileFormatType fileFormatType = TFileFormatType.FORMAT_UNKNOWN;
        String lowerCase = format.toLowerCase();
        if (lowerCase.contains("orc")) {
            fileFormatType = TFileFormatType.FORMAT_ORC;
        } else if (lowerCase.contains("parquet")) {
            fileFormatType = TFileFormatType.FORMAT_PARQUET;
        } else if (lowerCase.contains("text")) {
            fileFormatType = TFileFormatType.FORMAT_CSV_PLAIN;
        }
        if (!supportedFileFormatTypes().contains(fileFormatType)) {
            throw new AnalysisException("Unsupported input format type: " + format);
        }
        return fileFormatType;
    }

    protected TFileCompressType getTFileCompressType(String compressType) {
        if ("snappy".equalsIgnoreCase(compressType)) {
            return TFileCompressType.SNAPPYBLOCK;
        } else if ("lz4".equalsIgnoreCase(compressType)) {
            return TFileCompressType.LZ4BLOCK;
        } else if ("lzo".equalsIgnoreCase(compressType)) {
            return TFileCompressType.LZO;
        } else if ("zlib".equalsIgnoreCase(compressType)) {
            return TFileCompressType.ZLIB;
        } else if ("zstd".equalsIgnoreCase(compressType)) {
            return TFileCompressType.ZSTD;
        } else if ("uncompressed".equalsIgnoreCase(compressType)) {
            return TFileCompressType.PLAIN;
        } else {
            // try to use plain type to decompress parquet or orc file
            return TFileCompressType.PLAIN;
        }
    }

    /**
     * check sink params and generate thrift data sink to BE
     * @param insertCtx insert info context
     * @throws AnalysisException if source file format cannot be read
     */
    public abstract void bindDataSink(Optional<InsertCommandContext> insertCtx) throws AnalysisException;
}
