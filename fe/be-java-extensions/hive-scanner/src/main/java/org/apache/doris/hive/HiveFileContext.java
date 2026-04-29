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

package org.apache.doris.hive;

import org.apache.doris.thrift.TFileFormatType;

public class HiveFileContext {
    private final String serde;
    private final String inputFormat;

    HiveFileContext(TFileFormatType fileFormatType) {
        switch (fileFormatType) {
            case FORMAT_RCBINARY:
                serde = HiveProperties.RC_BINARY_SERDE_CLASS;
                inputFormat = HiveProperties.RC_BINARY_INPUT_FORMAT;
                break;
            case FORMAT_RCTEXT:
                serde = HiveProperties.RC_TEXT_SERDE_CLASS;
                inputFormat = HiveProperties.RC_TEXT_INPUT_FORMAT;
                break;
            case FORMAT_SEQUENCE:
                serde = HiveProperties.SEQUENCE_SERDE_CLASS;
                inputFormat = HiveProperties.SEQUENCE_INPUT_FORMAT;
                break;
            default:
                throw new UnsupportedOperationException("Unrecognized file format " + fileFormatType);
        }
    }

    String getSerde() {
        return serde;
    }

    String getInputFormat() {
        return inputFormat;
    }
}
