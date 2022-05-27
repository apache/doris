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

package org.apache.doris.common;

import org.apache.doris.common.io.Text;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.RandomAccessFile;

public class MetaJsonHeader {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final String IMAGE_VERSION = FeConstants.meta_format.getVersion();
    // the version of image format
    public String imageVersion;

    public static MetaJsonHeader read(RandomAccessFile raf) throws IOException {
        String jsonHeader = Text.readString(raf);
        return MetaJsonHeader.fromJson(jsonHeader);
    }

    public static void write(RandomAccessFile raf) throws IOException {
        MetaJsonHeader metaJsonHeader = new MetaJsonHeader();
        metaJsonHeader.imageVersion = IMAGE_VERSION;
        String jsonHeader =  MetaJsonHeader.toJson(metaJsonHeader);
        Text.writeString(raf, jsonHeader);
    }

    private static MetaJsonHeader fromJson(String json) throws IOException {
        return (MetaJsonHeader) OBJECT_MAPPER.readValue(json, MetaJsonHeader.class);
    }

    private static String toJson(MetaJsonHeader jsonHeader) throws IOException {
        return OBJECT_MAPPER.writeValueAsString(jsonHeader);
    }
}
