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

import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.security.authentication.PreExecutionAuthenticator;
import org.apache.doris.common.security.authentication.PreExecutionAuthenticatorCache;

import org.apache.iceberg.Table;
import org.apache.iceberg.util.SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

/**
 * Abstract class for Iceberg metadata scanner.
 */
public abstract class IcebergMetadataJniScanner extends JniScanner {
    protected final String[] requiredFields;
    protected final String timezone;

    private static final String HADOOP_OPTION_PREFIX = "hadoop.";
    private static final Logger LOG = LoggerFactory.getLogger(IcebergMetadataJniScanner.class);
    private PreExecutionAuthenticator preExecutionAuthenticator;
    private final ClassLoader classLoader;
    private final String serializedTable;
    private final int batchSize;
    private ColumnType[] requiredTypes;

    public IcebergMetadataJniScanner(int batchSize, Map<String, String> params) {
        this.classLoader = this.getClass().getClassLoader();
        this.batchSize = batchSize;
        this.requiredFields = params.get("required_fields").split(",");
        this.serializedTable = params.get("serialized_table");
        this.timezone = params.getOrDefault("time_zone", TimeZone.getDefault().getID());
        Map<String, String> hadoopOptionParams = params.entrySet().stream()
                .filter(kv -> kv.getKey().startsWith(HADOOP_OPTION_PREFIX))
                .collect(Collectors
                        .toMap(kv1 -> kv1.getKey().substring(HADOOP_OPTION_PREFIX.length()), kv1 -> kv1.getValue()));
        this.preExecutionAuthenticator = PreExecutionAuthenticatorCache.getAuthenticator(hadoopOptionParams);
        parseRequiredTypes();
        initTableInfo(requiredTypes, requiredFields, batchSize);
    }

    @Override
    public void open() throws IOException {
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            Table table = SerializationUtil.deserializeFromBase64(serializedTable);
            preExecutionAuthenticator.execute(() -> {
                loadTable(table);
                return null;
            });
        } catch (Exception e) {
            this.close();
            String msg = String.format("Failed to open IcebergMetadataJniScanner");
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    /**
     * This method is called in the open() method.
     * It is used to initialize the metadata table data.
     *
     * @throws IOException
     */
    protected abstract void loadTable(Table table) throws IOException;

    /**
     * Get the metadata schema from the table.
     *
     * @return a map of metadata column name to type
     */
    protected abstract HashMap<String, String> getMetadataSchema();

    private void parseRequiredTypes() {
        HashMap<String, String> metadataSchema = getMetadataSchema();
        requiredTypes = new ColumnType[requiredFields.length];
        for (int i = 0; i < requiredFields.length; i++) {
            String field = requiredFields[i];
            String type = metadataSchema.get(field);
            if (type == null) {
                throw new IllegalArgumentException("Field " + field + " not found in metadata column map");
            }
            requiredTypes[i] = ColumnType.parseType(field, type);
        }
    }
}
