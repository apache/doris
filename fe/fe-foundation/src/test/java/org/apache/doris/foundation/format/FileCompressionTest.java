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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Locale;

public class FileCompressionTest {
    @Test
    public void recognizesNativeCompressionSuffixes() {
        String[] extensions = {"gz", "bz2", "lz4", "lzo", "lzo_deflate", "deflate", "snappy", "zst", "zstd"};
        FileCompression[] codecs = {FileCompression.GZ, FileCompression.BZ2, FileCompression.LZ4FRAME,
                FileCompression.LZOP, FileCompression.LZO, FileCompression.DEFLATE,
                FileCompression.SNAPPYBLOCK, FileCompression.ZSTD, FileCompression.ZSTD};
        for (int i = 0; i < extensions.length; i++) {
            Assertions.assertEquals(codecs[i], FileCompression.fromPath("s3://bucket/data." + extensions[i]));
            Assertions.assertEquals(codecs[i],
                    FileCompression.fromPath("hdfs://nameservice/data." + extensions[i].toUpperCase(Locale.ROOT)));
        }
    }

    @Test
    public void preservesPlainAndUnknownSuffixBehavior() {
        for (String path : new String[] {"", "data", "data.csv", "data.parquet", "data.orc", "data.gzip",
                "dir.gz/data", "data.gz.tmp", "data.gz?key=value", "data.gz#fragment"}) {
            Assertions.assertEquals(FileCompression.PLAIN, FileCompression.fromPath(path), path);
        }
    }
}
