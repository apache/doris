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

package org.apache.doris.datasource.systable;

import org.apache.doris.info.TableValuedFunctionRefInfo;
import org.apache.doris.nereids.trees.expressions.functions.table.IcebergMeta;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.tablefunction.IcebergTableValuedFunction;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.iceberg.MetadataTableType;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * System table type for Iceberg metadata tables.
 *
 * <p>Iceberg system tables provide access to table metadata such as
 * snapshots, history, manifests, files, partitions, etc.
 *
 * <p>Iceberg system tables currently use the TVF path (MetadataScanNode).
 *
 * @see org.apache.iceberg.MetadataTableType for all supported system table types
 */
public class IcebergSysTable extends TvfSysTable {

    private static final String TVF_NAME = "iceberg_meta";

    /**
     * All supported Iceberg system tables.
     * Key is the system table name (e.g., "snapshots", "history").
     */
    public static final Map<String, SysTable> SUPPORTED_SYS_TABLES = Collections.unmodifiableMap(
            Arrays.stream(MetadataTableType.values())
                    .map(type -> new IcebergSysTable(type.name().toLowerCase()))
                    .collect(Collectors.toMap(SysTable::getSysTableName, Function.identity())));

    private final String tableName;

    private IcebergSysTable(String tableName) {
        super(tableName, TVF_NAME);
        this.tableName = tableName;
    }

    @Override
    public String getSysTableName() {
        return tableName;
    }

    @Override
    public TableValuedFunction createFunction(String ctlName, String dbName, String sourceNameWithMetaName) {
        return IcebergMeta.createIcebergMeta(
                Lists.newArrayList(ctlName, dbName, getSourceTableName(sourceNameWithMetaName)),
                getSysTableName());
    }

    @Override
    public TableValuedFunctionRefInfo createFunctionRef(String ctlName, String dbName, String sourceNameWithMetaName) {
        String tableName = String.format("%s.%s.%s", ctlName, dbName, getSourceTableName(sourceNameWithMetaName));
        try {
            java.util.Map<String, String> params = Maps.newHashMap();
            params.put(IcebergTableValuedFunction.TABLE, tableName);
            params.put(IcebergTableValuedFunction.QUERY_TYPE, getSysTableName());
            return new TableValuedFunctionRefInfo(tvfName, null, params);
        } catch (org.apache.doris.common.AnalysisException e) {
            throw new RuntimeException("Failed to create iceberg_meta tvf ref", e);
        }
    }
}
