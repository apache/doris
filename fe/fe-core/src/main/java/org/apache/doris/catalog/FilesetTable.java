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

package org.apache.doris.catalog;

import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TFilesetTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * FilesetTable is an internal table backed by object-storage files.
 * It has exactly one column of FILE type and references files via a
 * location URI that may contain a trailing wildcard (e.g. s3://bucket/path/*).
 */
public class FilesetTable extends Table {
    private static final Logger LOG = LogManager.getLogger(FilesetTable.class);

    private static final String PROP_LOCATION = "location";

    @SerializedName("loc")
    private String location;

    @SerializedName("fp")
    private Map<String, String> storageProperties;

    public FilesetTable() {
        super(TableType.FILESET);
    }

    public FilesetTable(long id, String name, List<Column> schema, Map<String, String> properties)
            throws DdlException {
        super(id, name, TableType.FILESET, schema);
        validate(properties);
    }

    private void validate(Map<String, String> properties) throws DdlException {
        if (properties == null || !properties.containsKey(PROP_LOCATION)) {
            throw new DdlException("Fileset table must specify 'location' property");
        }
        this.location = properties.get(PROP_LOCATION);
        Map<String, String> props = new HashMap<>(properties);
        props.remove(PROP_LOCATION);
        this.storageProperties = props;
    }

    public String getLocation() {
        return location;
    }

    public Map<String, String> getStorageProperties() {
        return storageProperties;
    }

    /**
     * Strip trailing wildcard characters to get the directory path for the BE reader.
     * For example: s3://bucket/path/* → s3://bucket/path/
     */
    public String getTablePath() {
        String path = location;
        if (path.endsWith("/**")) {
            path = path.substring(0, path.length() - 2);
        } else if (path.endsWith("/*")) {
            path = path.substring(0, path.length() - 1);
        } else if (path.endsWith("*")) {
            path = path.substring(0, path.length() - 1);
        }
        return path;
    }

    /**
     * Derive the TFileType from the location URI scheme.
     */
    public TFileType getFileType() {
        if (location.startsWith("s3://") || location.startsWith("s3a://") || location.startsWith("s3n://")) {
            return TFileType.FILE_S3;
        } else if (location.startsWith("hdfs://")) {
            return TFileType.FILE_HDFS;
        } else {
            return TFileType.FILE_S3;
        }
    }

    /**
     * Convert user-facing S3 storage properties to backend-facing AWS_* properties.
     */
    public Map<String, String> getBackendProperties() {
        try {
            return S3Properties.of(storageProperties).getBackendConfigProperties();
        } catch (Exception e) {
            LOG.warn("Failed to convert S3 properties to backend properties, using raw properties", e);
            return storageProperties;
        }
    }

    @Override
    public TTableDescriptor toThrift() {
        TFilesetTable tFilesetTable = new TFilesetTable();
        tFilesetTable.setTablePath(getTablePath());
        tFilesetTable.setFileType(getFileType());

        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.FILESET_TABLE,
                fullSchema.size(), 0, getName(), "");
        tTableDescriptor.setFilesetTable(tFilesetTable);
        return tTableDescriptor;
    }
}
