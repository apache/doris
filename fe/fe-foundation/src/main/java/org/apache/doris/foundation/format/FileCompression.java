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

package org.apache.doris.foundation.format;

import java.util.Locale;

/** Filename compression inference shared by native-reader planning and FE-to-BE codec selection. */
public enum FileCompression {
    PLAIN,
    GZ,
    BZ2,
    LZ4FRAME,
    LZOP,
    LZO,
    DEFLATE,
    SNAPPYBLOCK,
    ZSTD;

    // Names match TFileCompressType while keeping foundation independent of the Thrift module.
    public static FileCompression fromPath(String path) {
        String lowerCasePath = path.toLowerCase(Locale.ROOT);
        if (lowerCasePath.endsWith(".gz")) {
            return GZ;
        } else if (lowerCasePath.endsWith(".bz2")) {
            return BZ2;
        } else if (lowerCasePath.endsWith(".lz4")) {
            return LZ4FRAME;
        } else if (lowerCasePath.endsWith(".lzo")) {
            return LZOP;
        } else if (lowerCasePath.endsWith(".lzo_deflate")) {
            return LZO;
        } else if (lowerCasePath.endsWith(".deflate")) {
            return DEFLATE;
        } else if (lowerCasePath.endsWith(".snappy")) {
            return SNAPPYBLOCK;
        } else if (lowerCasePath.endsWith(".zst") || lowerCasePath.endsWith(".zstd")) {
            return ZSTD;
        } else {
            return PLAIN;
        }
    }
}
