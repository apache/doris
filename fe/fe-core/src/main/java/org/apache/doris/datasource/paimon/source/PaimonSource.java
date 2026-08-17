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

package org.apache.doris.datasource.paimon.source;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.PaimonScanParams;
import org.apache.doris.datasource.paimon.PaimonSysExternalTable;
import org.apache.doris.thrift.TFileAttributes;

import com.google.common.annotations.VisibleForTesting;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

public class PaimonSource {
    private final ExternalTable paimonExtTable;
    private final Table originTable;
    private final FileStoreTable boundSystemDataTable;
    private final TupleDescriptor desc;

    @VisibleForTesting
    public PaimonSource() {
        this.desc = null;
        this.paimonExtTable = null;
        this.originTable = null;
        this.boundSystemDataTable = null;
    }

    public PaimonSource(TupleDescriptor desc) {
        this(desc, MvccUtil.getSnapshotFromContext((ExternalTable) desc.getTable()));
    }

    public PaimonSource(TupleDescriptor desc, Optional<MvccSnapshot> snapshot) {
        this.desc = desc;
        this.paimonExtTable = (ExternalTable) desc.getTable();
        if (paimonExtTable instanceof PaimonSysExternalTable) {
            PaimonSysExternalTable systemTable = (PaimonSysExternalTable) paimonExtTable;
            this.boundSystemDataTable = systemTable.getBoundDataTable(snapshot);
            this.originTable = systemTable.getRawSysPaimonTable(boundSystemDataTable);
        } else {
            this.boundSystemDataTable = null;
            this.originTable = resolvePaimonTable(paimonExtTable, snapshot);
        }
    }

    public TupleDescriptor getDesc() {
        return desc;
    }

    public Table getPaimonTable() {
        return originTable;
    }

    public Table getPaimonTable(TableScanParams scanParams) {
        if (paimonExtTable instanceof PaimonExternalTable) {
            if (scanParams != null && scanParams.isOptions()
                    && PaimonScanParams.usesStatementSnapshot(scanParams.getMapParams())
                    && !PaimonScanParams.selectsSchema(scanParams.getMapParams())) {
                Map<String, String> resolvedOptions = scanParams.getOrResolveMapParams(
                        options -> PaimonScanParams.resolveOptions(originTable, options));
                // Behavioral OPTIONS must decorate this relation's retained table; consulting the
                // statement cache here can borrow another relation's historical generation.
                return PaimonScanParams.applyOptions(originTable, resolvedOptions);
            }
            return ((PaimonExternalTable) paimonExtTable).getPaimonTable(scanParams);
        }
        if (paimonExtTable instanceof PaimonSysExternalTable) {
            return ((PaimonSysExternalTable) paimonExtTable)
                    .getSysPaimonTable(boundSystemDataTable, scanParams);
        }
        throw new IllegalArgumentException(
                "Expected Paimon table but got " + paimonExtTable.getClass().getSimpleName());
    }

    public TableIf getTargetTable() {
        return paimonExtTable;
    }

    public ExternalTable getExternalTable() {
        return paimonExtTable;
    }

    private Table resolvePaimonTable(ExternalTable table, Optional<MvccSnapshot> snapshot) {
        if (table instanceof PaimonExternalTable) {
            return ((PaimonExternalTable) table).getPaimonTable(snapshot);
        }
        throw new IllegalArgumentException(
                "Expected Paimon table but got " + table.getClass().getSimpleName());
    }

    public OptionalInt runtimeSafeManifestParallelism(TableScanParams scanParams) {
        return ((PaimonSysExternalTable) paimonExtTable)
                .runtimeSafeManifestParallelism(boundSystemDataTable, scanParams);
    }

    public FileStoreTable runtimeSafeSystemDataTable(
            TableScanParams scanParams, Map<String, String> incrementalOptions) {
        return ((PaimonSysExternalTable) paimonExtTable)
                .runtimeSafeDataTable(boundSystemDataTable, scanParams, incrementalOptions);
    }

    public void validateEffectiveSystemDataTable(TableScanParams scanParams) {
        ((PaimonSysExternalTable) paimonExtTable)
                .validateEffectiveDataTable(boundSystemDataTable, scanParams);
    }

    public TFileAttributes getFileAttributes() throws UserException {
        return new TFileAttributes();
    }

    public ExternalCatalog getCatalog() {
        return paimonExtTable.getCatalog();
    }

    public String getFileFormatFromTableProperties() {
        return originTable.options().getOrDefault("file.format", "parquet");
    }

    public String getTableLocation() {
        if (originTable instanceof FileStoreTable) {
            return ((FileStoreTable) originTable).location().toString();
        }
        // Fallback to path option
        return originTable.options().get("path");
    }
}
