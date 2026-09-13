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

import org.apache.doris.common.classloader.ThreadClassLoaderContext;
import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ColumnValue;
import org.apache.doris.kerberos.PreExecutionAuthenticator;
import org.apache.doris.kerberos.PreExecutionAuthenticatorCache;

import com.google.common.base.Preconditions;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.util.SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

/**
 * JniScanner to read Iceberg SysTables
 */
public class IcebergSysTableJniScanner extends JniScanner {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergSysTableJniScanner.class);
    private static final String HADOOP_OPTION_PREFIX = "hadoop.";
    private final ClassLoader classLoader;
    private final PreExecutionAuthenticator preExecutionAuthenticator;
    private final FileScanTask scanTask;
    private final int requiredFieldCount;
    private final String timezone;
    private CloseableIterator<StructLike> reader;

    public IcebergSysTableJniScanner(int batchSize, Map<String, String> params) {
        this.classLoader = this.getClass().getClassLoader();
        String serializedSplitParams = params.get("serialized_split");
        Preconditions.checkArgument(serializedSplitParams != null && !serializedSplitParams.isEmpty(),
                "serialized_split should not be empty");
        this.scanTask = SerializationUtil.deserializeFromBase64(serializedSplitParams);
        String requiredFieldsParam = params.get("required_fields");
        Preconditions.checkArgument(requiredFieldsParam != null && !requiredFieldsParam.isEmpty(),
                "required_fields should not be empty");
        String[] requiredFields = requiredFieldsParam.split(",");
        this.requiredFieldCount = requiredFields.length;
        this.timezone = params.getOrDefault("time_zone", TimeZone.getDefault().getID());
        Map<String, String> hadoopOptionParams = params.entrySet().stream()
                .filter(kv -> kv.getKey().startsWith(HADOOP_OPTION_PREFIX))
                .collect(Collectors
                        .toMap(kv1 -> kv1.getKey().substring(HADOOP_OPTION_PREFIX.length()), kv1 -> kv1.getValue()));
        this.preExecutionAuthenticator = PreExecutionAuthenticatorCache.getAuthenticator(hadoopOptionParams);
        String requiredTypesParam = params.get("required_types");
        Preconditions.checkArgument(requiredTypesParam != null && !requiredTypesParam.isEmpty(),
                "required_types should not be empty");
        String[] requiredTypeStrings = requiredTypesParam.split("#");
        ColumnType[] requiredTypes = parseRequiredTypes(requiredTypeStrings, requiredFields);
        initTableInfo(requiredTypes, requiredFields, batchSize);
    }

    @Override
    public void open() throws IOException {
        try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(classLoader)) {
            openReader();
        }
    }

    private void openReader() throws IOException {
        try {
            try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(classLoader)) {
                preExecutionAuthenticator.execute(() -> {
                    // execute FileScanTask to get rows
                    reader = scanTask.asDataTask().rows().iterator();
                    return null;
                });
            }
        } catch (Exception e) {
            this.close();
            String msg = String.format("Failed to open scan task: %s", scanTask);
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    @Override
    protected int getNext() throws IOException {
        try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(classLoader)) {
            int rows = 0;
            long startAppendDataTime = System.nanoTime();
            while (rows < getBatchSize()) {
                if (!reader.hasNext()) {
                    break;
                }
                StructLike row = reader.next();
                for (int i = 0; i < requiredFieldCount; i++) {
                    // Read positionally: FE (IcebergScanPlanProvider.doPlanSystemTableScan) projects the
                    // metadata-table scan to exactly the BE-requested fields, in required_fields order, so the
                    // i-th projected row field is the i-th required field. Do NOT index via scanTask.schema():
                    // for a metadata StaticDataTask, schema() returns the FULL table schema while rows() yields a
                    // narrowed StructProjection, so a full-schema ordinal overruns the projected row (upstream
                    // #65262 -- reverting this to a by-name/schema() lookup reintroduces ArrayIndexOutOfBounds).
                    Object value = row.get(i, Object.class);
                    ColumnValue columnValue = new IcebergSysTableColumnValue(value, timezone);
                    appendData(i, columnValue);
                }
                rows++;
            }
            appendDataTime += System.nanoTime() - startAppendDataTime;
            return rows;
        }
    }

    @Override
    public void close() throws IOException {
        try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(classLoader)) {
            if (reader != null) {
                // Close the iterator to release resources
                reader.close();
            }
        }
    }

    private static ColumnType[] parseRequiredTypes(String[] typeStrings, String[] requiredFields) {
        Preconditions.checkArgument(typeStrings.length == requiredFields.length,
                "required_types size %s does not match required_fields size %s",
                typeStrings.length, requiredFields.length);
        ColumnType[] requiredTypes = new ColumnType[typeStrings.length];
        for (int i = 0; i < typeStrings.length; i++) {
            String type = typeStrings[i];
            ColumnType parsedType = ColumnType.parseType(requiredFields[i], type);
            if (parsedType.isUnsupported()) {
                throw new IllegalArgumentException("Unsupported type " + type + " for field " + requiredFields[i]);
            }
            requiredTypes[i] = parsedType;
        }
        return requiredTypes;
    }
}
