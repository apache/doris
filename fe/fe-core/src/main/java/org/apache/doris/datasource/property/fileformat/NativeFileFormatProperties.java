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
import org.apache.doris.thrift.TResultFileSinkOptions;

import java.util.Map;

/**
 * File format properties for Doris Native binary columnar format.
 *
 * This format is intended for high-performance internal data exchange
 */

public class NativeFileFormatProperties extends FileFormatProperties {

    public NativeFileFormatProperties() {
        super(TFileFormatType.FORMAT_NATIVE, FileFormatProperties.FORMAT_NATIVE);
    }

    @Override
    public void analyzeFileFormatProperties(Map<String, String> formatProperties,
                                            boolean isRemoveOriginProperty)
            throws AnalysisException {
        // Currently no extra user visible properties for native format.
        // Just ignore all other properties gracefully.
    }

    @Override
    public void fullTResultFileSinkOptions(TResultFileSinkOptions sinkOptions) {
        // No extra sink options are required for native format.
    }

    @Override
    public TFileAttributes toTFileAttributes() {
        // For now we don't need text params for Native format, but TFileAttributes
        // requires a text_params field to be non-null on BE side.
        TFileAttributes fileAttributes = new TFileAttributes();
        TFileTextScanRangeParams textParams = new TFileTextScanRangeParams();
        fileAttributes.setTextParams(textParams);
        return fileAttributes;
    }
}



