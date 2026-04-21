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

package org.apache.doris.connector.api.scan;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * Describes a delete file associated with a scan range.
 *
 * <p>Used primarily by Iceberg merge-on-read (MOR) tables where
 * positional or equality delete files must be applied during reads.
 * Connectors that do not use delete files can ignore this class.</p>
 */
public class ConnectorDeleteFile implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String path;
    private final String fileFormat;
    private final long recordCount;
    private final Map<String, String> properties;

    public ConnectorDeleteFile(String path, String fileFormat, long recordCount,
            Map<String, String> properties) {
        this.path = path;
        this.fileFormat = fileFormat;
        this.recordCount = recordCount;
        this.properties = properties != null
                ? Collections.unmodifiableMap(properties)
                : Collections.emptyMap();
    }

    public ConnectorDeleteFile(String path, String fileFormat, long recordCount) {
        this(path, fileFormat, recordCount, Collections.emptyMap());
    }

    /** Returns the path to the delete file. */
    public String getPath() {
        return path;
    }

    /** Returns the file format (e.g., "parquet", "avro"). */
    public String getFileFormat() {
        return fileFormat;
    }

    /** Returns the number of delete records in this file. */
    public long getRecordCount() {
        return recordCount;
    }

    /** Returns additional properties for this delete file. */
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "ConnectorDeleteFile{path='" + path + "', format='" + fileFormat
                + "', records=" + recordCount + "}";
    }
}
