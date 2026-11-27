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

package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorTableSchema;

import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

final class DeltaLakeHistorySchema {
    static final String NAME = "history";
    static final String VERSION = "version";
    static final String TIMESTAMP = "timestamp";
    static final String COMMIT_INFO = "commitInfo";
    static final String ICT = "inCommitTimestamp";

    private static final StructType JOB = new StructType()
            .add("jobId", StringType.STRING)
            .add("jobName", StringType.STRING)
            .add("jobRunId", StringType.STRING)
            .add("runId", StringType.STRING)
            .add("jobOwnerId", StringType.STRING)
            .add("triggerType", StringType.STRING);
    private static final StructType NOTEBOOK = new StructType().add("notebookId", StringType.STRING);
    private static final MapType STRING_MAP = new MapType(StringType.STRING, StringType.STRING, true);

    // These are the DESCRIBE HISTORY fields stored in commitInfo. Kernel's own projection
    // omits several of them, including the user, job, and notebook fields.
    static final StructType DETAIL_FIELDS = new StructType()
            .add("userId", StringType.STRING)
            .add("userName", StringType.STRING)
            .add("operation", StringType.STRING)
            .add("operationParameters", STRING_MAP)
            .add("job", JOB)
            .add("notebook", NOTEBOOK)
            .add("clusterId", StringType.STRING)
            .add("readVersion", LongType.LONG)
            .add("isolationLevel", StringType.STRING)
            .add("isBlindAppend", BooleanType.BOOLEAN)
            .add("operationMetrics", STRING_MAP)
            .add("userMetadata", StringType.STRING)
            .add("engineInfo", StringType.STRING);

    private static final List<ConnectorColumn> COLUMNS = columns();

    private DeltaLakeHistorySchema() {
    }

    static ConnectorTableSchema tableSchema(String table) {
        return new ConnectorTableSchema(table + "$" + NAME, COLUMNS, "deltalake", Map.of());
    }

    private static List<ConnectorColumn> columns() {
        List<ConnectorColumn> columns = new ArrayList<>();
        // FileScannerV2 reads external file columns as nullable. These two columns are
        // present in every history row, so retain that contract for schema display.
        columns.add(new ConnectorColumn(VERSION, DeltaLakeTypeMapping.toConnectorType(LongType.LONG, true, true),
                "", true, null).withSourceNullable(false));
        columns.add(new ConnectorColumn(TIMESTAMP,
                DeltaLakeTypeMapping.toConnectorType(TimestampType.TIMESTAMP, true, true),
                "", true, null).withTimeZone().withSourceNullable(false));
        DETAIL_FIELDS.fields().forEach(field -> columns.add(new ConnectorColumn(field.getName(),
                DeltaLakeTypeMapping.toConnectorType(field.getDataType(), true, true), "", true, null)));
        return List.copyOf(columns);
    }
}
