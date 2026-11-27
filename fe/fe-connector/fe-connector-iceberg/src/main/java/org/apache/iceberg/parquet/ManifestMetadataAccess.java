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

package org.apache.iceberg.parquet;

import org.apache.iceberg.io.InputFile;

import org.apache.parquet.hadoop.ParquetFileReader;

import java.io.IOException;
import java.util.Map;

/** Bridge to the SDK's InputFile adapter without depending on a Hadoop filesystem. */
public final class ManifestMetadataAccess {
    private ManifestMetadataAccess() {
    }

    public static Map<String, String> read(InputFile file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(file))) {
            return reader.getFooter().getFileMetaData().getKeyValueMetaData();
        }
    }
}
