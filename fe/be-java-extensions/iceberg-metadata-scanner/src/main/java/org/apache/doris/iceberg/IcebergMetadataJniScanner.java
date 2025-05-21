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

package org.apache.doris.iceberg;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class for Iceberg metadata scanner.
 */
public abstract class IcebergMetadataJniScanner extends JniScanner {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergMetadataJniScanner.class);
    protected final String serializedTable;
    protected final String[] requiredFields;
    private final String[] metadataColumnNames;
    private final String[] metadataColumnTypes;
    private ColumnType[] requiredTypes;
    private final int batchSize;
    protected final ClassLoader classLoader;
    protected Table table;
    protected String timezone;

    public IcebergMetadataJniScanner(int batchSize, Map<String, String> params) {
        this.batchSize = batchSize;
        this.requiredFields = params.get("required_fields").split(",");
        this.metadataColumnNames = params.get("metadata_column_names").split(",");
        this.metadataColumnTypes = params.get("metadata_column_types").split("#");
        this.serializedTable = params.get("serialized_table");
        this.timezone = params.getOrDefault("time_zone", TimeZone.getDefault().getID());
        this.classLoader = this.getClass().getClassLoader();
    }

    @Override
    public void open() throws IOException {
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            this.table = SerializationUtil.deserializeFromBase64(serializedTable);
            parseRequiredTypes();
            initTableInfo(requiredTypes, requiredFields, batchSize);
            openInner();
            initReader();
        } catch (Exception e) {
            this.close();
            String msg = String.format("Failed to open IcebergMetadataJniScanner");
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    @Override
    public int getNext() throws IOException {
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            return getNextInner();
        } catch (Exception e) {
            this.close();
            String msg = String.format(
                    "Failed to get next IcebergMetadataJniScanner");
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            closeInner();
        } catch (Exception e) {
            String msg = String.format("Failed to close IcebergMetadataJniScanner");
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    protected abstract void initReader() throws IOException;

    protected abstract void openInner();

    protected abstract int getNextInner();

    protected abstract void closeInner() throws IOException;

    private void parseRequiredTypes() {
        HashMap<String, String> metadataColumnMap = new HashMap<>();
        for (int i = 0; i < metadataColumnNames.length; i++) {
            metadataColumnMap.put(metadataColumnNames[i], metadataColumnTypes[i]);
        }
        requiredTypes = new ColumnType[requiredFields.length];
        for (int i = 0; i < requiredFields.length; i++) {
            String field = requiredFields[i];
            String type = metadataColumnMap.get(field);
            if (type == null) {
                throw new IllegalArgumentException("Field " + field + " not found in metadata column map");
            }
            requiredTypes[i] = ColumnType.parseType(field, type);
        }
    }
}
