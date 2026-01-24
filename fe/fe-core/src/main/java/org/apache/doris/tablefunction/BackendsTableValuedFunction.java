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

package org.apache.doris.tablefunction;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TBackendsMetadataParams;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TMetadataType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.ArrayUtils;

import java.util.List;
import java.util.Map;

/**
 * The Implement of table valued function
 * backends().
 */
public class BackendsTableValuedFunction extends MetadataTableValuedFunction {
    public static final String NAME = "backends";

    /**
     * Design Explanation for Column Schema:
     *
     * The schema is split into multiple arrays to handle dynamic column visibility cleanly
     * without introducing complex functional interfaces or runtime overhead.
     *
     * Current arrays:
     * 1. HISTORICAL_COLUMNS: The stable core columns that always exist. Do not modify the order
     *    to maintain backward compatibility for existing `SHOW BACKENDS` parsing tools.
     * 2. EXTENSION_COLUMNS: Unconditionally displayed columns added in the future.
     * 3. FQDN_CONDITIONAL_COLUMNS: Columns that only appear when Config.enable_fqdn_mode is true.
     *
     * Future column additions can easily fit into this structure:
     * - Non-conditional columns: Append to EXTENSION_COLUMNS.
     * - New FQDN-related conditional columns: Append to FQDN_CONDITIONAL_COLUMNS.
     * - Columns based on OTHER conditions (e.g., Config.isCloudMode):
     *   Create a new specific array (e.g., CLOUD_CONDITIONAL_COLUMNS) and append it to the
     *   merge logic in ALL_COLUMNS (and handle its visibility in the static block accordingly).
     *
     * NOTE ON COLUMN ORDER:
     * This structure dynamically merges arrays, implying that conditional columns will always
     * be placed at the end of the schema. This is perfectly acceptable for "backends" metadata,
     * as appending newly introduced environment-specific columns to the end is the standard
     * convention in Doris to maintain backward compatibility.
     */

    // 1. Historical core columns (Do not modify order)
    private static final Column[] HISTORICAL_COLUMNS = new Column[]{
        new Column("BackendId", ScalarType.createType(PrimitiveType.BIGINT)),
        new Column("Host", ScalarType.createStringType()),
        new Column("HeartbeatPort", ScalarType.createType(PrimitiveType.INT)),
        new Column("BePort", ScalarType.createType(PrimitiveType.INT)),
        new Column("HttpPort", ScalarType.createType(PrimitiveType.INT)),
        new Column("BrpcPort", ScalarType.createType(PrimitiveType.INT)),
        new Column("ArrowFlightSqlPort", ScalarType.createType(PrimitiveType.INT)),
        new Column("LastStartTime", ScalarType.createStringType()),
        new Column("LastHeartbeat", ScalarType.createStringType()),
        new Column("Alive", ScalarType.createType(PrimitiveType.BOOLEAN)),
        new Column("SystemDecommissioned", ScalarType.createType(PrimitiveType.BOOLEAN)),
        new Column("TabletNum", ScalarType.createType(PrimitiveType.BIGINT)),
        new Column("DataUsedCapacity", ScalarType.createType(PrimitiveType.BIGINT)),
        new Column("TrashUsedCapacity", ScalarType.createType(PrimitiveType.BIGINT)),
        new Column("AvailCapacity", ScalarType.createType(PrimitiveType.BIGINT)),
        new Column("TotalCapacity", ScalarType.createType(PrimitiveType.BIGINT)),
        new Column("UsedPct", ScalarType.createType(PrimitiveType.DOUBLE)),
        new Column("MaxDiskUsedPct", ScalarType.createType(PrimitiveType.DOUBLE)),
        new Column("RemoteUsedCapacity", ScalarType.createType(PrimitiveType.BIGINT)),
        new Column("Tag", ScalarType.createStringType()),
        new Column("ErrMsg", ScalarType.createStringType()),
        new Column("Version", ScalarType.createStringType()),
        new Column("Status", ScalarType.createStringType()),
        new Column("HeartbeatFailureCounter", ScalarType.createType(PrimitiveType.INT)),
        new Column("CpuCores", ScalarType.createType(PrimitiveType.INT)),
        new Column("Memory", ScalarType.createStringType()),
        new Column("LiveSince", ScalarType.createStringType()),
        new Column("RunningTasks", ScalarType.createType(PrimitiveType.BIGINT)),
        new Column("NodeRole", ScalarType.createStringType())
    };

    // 2. Future unconditionally added columns
    private static final Column[] EXTENSION_COLUMNS = new Column[]{
        // Intentionally empty. Future unconditional columns go here.
    };

    // 3. Conditionally displayed columns depending on FQDN mode
    private static final Column[] FQDN_CONDITIONAL_COLUMNS = new Column[]{
        new Column("Ip", ScalarType.createStringType())
    };

    // Combine historical and extension columns as UNCONDITIONAL_COLUMNS (always displayed)
    private static final Column[] UNCONDITIONAL_COLUMNS = ArrayUtils.addAll(HISTORICAL_COLUMNS, EXTENSION_COLUMNS);

    // Append conditional columns based on UNCONDITIONAL_COLUMNS
    // Future other conditional arrays can be appended here, e.g.,
    // ArrayUtils.addAll(UNCONDITIONAL_COLUMNS, FQDN_CONDITIONAL_COLUMNS, CLOUD_CONDITIONAL_COLUMNS)
    private static final Column[] ALL_COLUMNS = ArrayUtils.addAll(UNCONDITIONAL_COLUMNS, FQDN_CONDITIONAL_COLUMNS);

    private static final ImmutableList<Column> SCHEMA;
    private static final ImmutableMap<String, Integer> COLUMN_TO_INDEX;
    private static final ImmutableList<String> TITLE_NAMES;

    static {
        // Config is initialized at the very beginning of FE startup,
        // so it is safe to read Config.enable_fqdn_mode here.
        Column[] columns = Config.enable_fqdn_mode ? ALL_COLUMNS : UNCONDITIONAL_COLUMNS;
        SCHEMA = ImmutableList.copyOf(columns);

        ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder<>();
        ImmutableList.Builder<String> titleBuilder = ImmutableList.builder();
        for (int i = 0; i < SCHEMA.size(); i++) {
            String columnName = SCHEMA.get(i).getName();
            builder.put(columnName.toLowerCase(), i);
            titleBuilder.add(columnName);
        }
        COLUMN_TO_INDEX = builder.build();
        TITLE_NAMES = titleBuilder.build();
    }

    public static Integer getColumnIndexFromColumnName(String columnName) {
        return COLUMN_TO_INDEX.get(columnName.toLowerCase());
    }

    public BackendsTableValuedFunction(Map<String, String> params) throws AnalysisException {
        if (params.size() != 0) {
            throw new AnalysisException("backends table-valued-function does not support any params");
        }
        if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(),
                InternalCatalog.INTERNAL_CATALOG_NAME, InfoSchemaDb.DATABASE_NAME, PrivPredicate.SELECT)) {
            String message = ErrorCode.ERR_DB_ACCESS_DENIED_ERROR.formatErrorMsg(
                    PrivPredicate.SELECT.getPrivs().toString(), InfoSchemaDb.DATABASE_NAME);
            throw new AnalysisException(message);
        }
    }

    @Override
    public TMetadataType getMetadataType() {
        return TMetadataType.BACKENDS;
    }

    @Override
    public TMetaScanRange getMetaScanRange(List<String> requiredFileds) {
        TMetaScanRange metaScanRange = new TMetaScanRange();
        metaScanRange.setMetadataType(TMetadataType.BACKENDS);
        TBackendsMetadataParams backendsMetadataParams = new TBackendsMetadataParams();
        backendsMetadataParams.setClusterName("");
        metaScanRange.setBackendsParams(backendsMetadataParams);
        return metaScanRange;
    }

    @Override
    public String getTableName() {
        return "BackendsTableValuedFunction";
    }

    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        return SCHEMA;
    }

    /**
     * unify title names for backends function and show backends command
     */
    public static ImmutableList<String> getBackendsTitleNames() {
        return TITLE_NAMES;
    }
}
