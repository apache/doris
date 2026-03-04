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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.info.TableValuedFunctionRefInfo;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;

import com.google.common.base.Preconditions;

import java.util.Optional;

/**
 * Central resolver for external system tables.
 *
 * <p>Provides a single place to resolve system table types and build the
 * execution artifacts needed by planners or describe commands.
 */
public final class SysTableResolver {

    private SysTableResolver() {
    }

    public static Optional<SysTable> resolveType(TableIf table, String tableNameWithSysTableName) {
        return table.findSysTable(tableNameWithSysTableName);
    }

    /**
     * Resolve system table for planning (Nereids). TVF path only needs TableValuedFunction.
     */
    public static Optional<SysTablePlan> resolveForPlan(TableIf table, String ctlName, String dbName,
            String tableNameWithSysTableName) {
        Optional<SysTable> typeOpt = resolveType(table, tableNameWithSysTableName);
        if (!typeOpt.isPresent()) {
            return Optional.empty();
        }
        SysTable type = typeOpt.get();
        if (type instanceof NativeSysTable && table instanceof ExternalTable) {
            ExternalTable sysExternalTable = ((NativeSysTable) type).createSysExternalTable((ExternalTable) table);
            return Optional.of(SysTablePlan.forNative(type, sysExternalTable));
        }
        if (type instanceof TvfSysTable) {
            TableValuedFunction tvf = ((TvfSysTable) type)
                    .createFunction(ctlName, dbName, tableNameWithSysTableName);
            return Optional.of(SysTablePlan.forTvf(type, tvf));
        }
        return Optional.empty();
    }

    /**
     * Resolve system table for DESCRIBE. TVF path needs TableValuedFunctionRefInfo.
     */
    public static Optional<SysTableDescribe> resolveForDescribe(TableIf table, String ctlName, String dbName,
            String tableNameWithSysTableName) {
        Optional<SysTable> typeOpt = resolveType(table, tableNameWithSysTableName);
        if (!typeOpt.isPresent()) {
            return Optional.empty();
        }
        SysTable type = typeOpt.get();
        if (type instanceof NativeSysTable && table instanceof ExternalTable) {
            ExternalTable sysExternalTable = ((NativeSysTable) type).createSysExternalTable((ExternalTable) table);
            return Optional.of(SysTableDescribe.forNative(type, sysExternalTable));
        }
        if (type instanceof TvfSysTable) {
            TableValuedFunctionRefInfo tvfRef = ((TvfSysTable) type)
                    .createFunctionRef(ctlName, dbName, tableNameWithSysTableName);
            return Optional.of(SysTableDescribe.forTvf(type, tvfRef));
        }
        return Optional.empty();
    }

    /**
     * Validate system table existence. For TVF path, ensures the TVF can be created.
     */
    public static boolean validateForQuery(TableIf table, String ctlName, String dbName,
            String tableNameWithSysTableName) {
        Optional<SysTable> typeOpt = resolveType(table, tableNameWithSysTableName);
        if (!typeOpt.isPresent()) {
            return false;
        }
        SysTable type = typeOpt.get();
        if (!type.useNativeTablePath()) {
            if (!(type instanceof TvfSysTable)) {
                return false;
            }
            ((TvfSysTable) type).createFunction(ctlName, dbName, tableNameWithSysTableName);
        }
        return true;
    }

    public static final class SysTablePlan {
        private final SysTable type;
        private final ExternalTable sysExternalTable;
        private final TableValuedFunction tvf;

        private SysTablePlan(SysTable type, ExternalTable sysExternalTable, TableValuedFunction tvf) {
            this.type = type;
            this.sysExternalTable = sysExternalTable;
            this.tvf = tvf;
        }

        public static SysTablePlan forNative(SysTable type, ExternalTable sysExternalTable) {
            Preconditions.checkNotNull(sysExternalTable, "sysExternalTable is null");
            return new SysTablePlan(type, sysExternalTable, null);
        }

        public static SysTablePlan forTvf(SysTable type, TableValuedFunction tvf) {
            Preconditions.checkNotNull(tvf, "tvf is null");
            return new SysTablePlan(type, null, tvf);
        }

        public boolean isNative() {
            return sysExternalTable != null;
        }

        public SysTable getSysTable() {
            return type;
        }

        public ExternalTable getSysExternalTable() {
            Preconditions.checkState(isNative(), "Not a native system table");
            return sysExternalTable;
        }

        public TableValuedFunction getTvf() {
            Preconditions.checkState(!isNative(), "Not a TVF system table");
            return tvf;
        }
    }

    public static final class SysTableDescribe {
        private final SysTable type;
        private final ExternalTable sysExternalTable;
        private final TableValuedFunctionRefInfo tvfRef;

        private SysTableDescribe(SysTable type, ExternalTable sysExternalTable, TableValuedFunctionRefInfo tvfRef) {
            this.type = type;
            this.sysExternalTable = sysExternalTable;
            this.tvfRef = tvfRef;
        }

        public static SysTableDescribe forNative(SysTable type, ExternalTable sysExternalTable) {
            Preconditions.checkNotNull(sysExternalTable, "sysExternalTable is null");
            return new SysTableDescribe(type, sysExternalTable, null);
        }

        public static SysTableDescribe forTvf(SysTable type, TableValuedFunctionRefInfo tvfRef) {
            Preconditions.checkNotNull(tvfRef, "tvfRef is null");
            return new SysTableDescribe(type, null, tvfRef);
        }

        public boolean isNative() {
            return sysExternalTable != null;
        }

        public SysTable getSysTable() {
            return type;
        }

        public ExternalTable getSysExternalTable() {
            Preconditions.checkState(isNative(), "Not a native system table");
            return sysExternalTable;
        }

        public TableValuedFunctionRefInfo getTvfRef() {
            Preconditions.checkState(!isNative(), "Not a TVF system table");
            return tvfRef;
        }
    }
}
