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
import org.apache.doris.common.jni.vec.ColumnValue;
import org.apache.doris.common.security.authentication.PreExecutionAuthenticator;
import org.apache.doris.common.security.authentication.PreExecutionAuthenticatorCache;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
    private final List<NestedField> fields;
    private final String timezone;
    private CloseableIterator<StructLike> reader;

    public IcebergSysTableJniScanner(int batchSize, Map<String, String> params) {
        this.classLoader = this.getClass().getClassLoader();
        this.scanTask = SerializationUtil.deserializeFromBase64(params.get("serialized_task"));
        String[] requiredFields = params.get("required_fields").split(",");
        this.fields = selectSchema(scanTask.schema().asStruct(), requiredFields);
        this.timezone = params.getOrDefault("time_zone", TimeZone.getDefault().getID());
        Map<String, String> hadoopOptionParams = params.entrySet().stream()
                .filter(kv -> kv.getKey().startsWith(HADOOP_OPTION_PREFIX))
                .collect(Collectors
                        .toMap(kv1 -> kv1.getKey().substring(HADOOP_OPTION_PREFIX.length()), kv1 -> kv1.getValue()));
        this.preExecutionAuthenticator = PreExecutionAuthenticatorCache.getAuthenticator(hadoopOptionParams);
        ColumnType[] requiredTypes = parseRequiredTypes(params.get("required_types").split("#"), requiredFields);
        initTableInfo(requiredTypes, requiredFields, batchSize);
    }

    @Override
    public void open() throws IOException {
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            preExecutionAuthenticator.execute(() -> {
                // execute FileScanTask to get rows
                reader = scanTask.asDataTask().rows().iterator();
                return null;
            });
        } catch (Exception e) {
            this.close();
            String msg = String.format("Failed to open IcebergMetadataJniScanner");
            LOG.error(msg, e);
            throw new IOException(msg, e);
        }
    }

    @Override
    protected int getNext() throws IOException {
        if (reader == null) {
            return 0;
        }
        int rows = 0;
        while (reader.hasNext() && rows < getBatchSize()) {
            StructLike row = reader.next();
            for (int i = 0; i < fields.size(); i++) {
                NestedField field = fields.get(i);
                Object value = row.get(i, field.type().typeId().javaClass());
                ColumnValue columnValue = new IcebergSysTableColumnValue(value, timezone);
                appendData(i, columnValue);
            }
            rows++;
        }
        return rows;
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            // Close the iterator to release resources
            reader.close();
        }
    }

    private static List<NestedField> selectSchema(StructType schema, String[] requiredFields) {
        List<NestedField> selectedFields = new ArrayList<>();
        for (String requiredField : requiredFields) {
            NestedField field = schema.field(requiredField);
            if (field == null) {
                throw new IllegalArgumentException("RequiredField " + requiredField + " not found in schema");
            }
            selectedFields.add(field);
        }
        return selectedFields;
    }

    private static ColumnType[] parseRequiredTypes(String[] typeStrings, String[] requiredFields) {
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
