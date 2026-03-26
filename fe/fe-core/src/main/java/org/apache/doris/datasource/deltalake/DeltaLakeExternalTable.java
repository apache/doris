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

package org.apache.doris.datasource.deltalake;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ExternalAnalysisTask;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Represents a Delta Lake table in Doris.
 * Reads schema and partition info from Delta Lake metadata (via Delta Kernel API).
 *
 * <p>Works with both HMS-backed ({@link DeltaLakeMetadataOps}) and
 * Unity Catalog-backed ({@link DeltaLakeUnityMetadataOps}) catalogs
 * via the common base class {@link AbstractDeltaLakeMetadataOps}.
 */
public class DeltaLakeExternalTable extends ExternalTable {
    private static final Logger LOG = LogManager.getLogger(DeltaLakeExternalTable.class);

    private final ExternalCatalog catalog;
    private final DeltaLakeExternalDatabase db;

    public DeltaLakeExternalTable(long id, String name, String remoteName,
            ExternalCatalog catalog, DeltaLakeExternalDatabase db) {
        super(id, name, remoteName, catalog, db, TableIf.TableType.DELTALAKE_EXTERNAL_TABLE);
        this.catalog = catalog;
        this.db = db;
    }

    @Override
    public Optional<SchemaCacheValue> initSchema() {
        makeSureInitialized();
        try {
            AbstractDeltaLakeMetadataOps ops = getDeltaLakeMetadataOps();
            String tableLocation = ops.getTableLocation(db.getRemoteName(), getRemoteName());

            Engine engine = ops.getEngine();
            Table deltaTable = Table.forPath(engine, tableLocation);
            Snapshot snapshot = deltaTable.getLatestSnapshot(engine);
            StructType deltaSchema = snapshot.getSchema();

            List<Column> columns = new ArrayList<>();
            for (StructField field : deltaSchema.fields()) {
                Column dorisColumn = DeltaLakeUtils.convertDeltaFieldToDoris(field);
                columns.add(dorisColumn);
            }

            if (columns.isEmpty()) {
                LOG.warn("Empty schema for Delta Lake table {}.{}", db.getFullName(), name);
                return Optional.empty();
            }
            return Optional.of(new SchemaCacheValue(columns));
        } catch (Exception e) {
            LOG.warn("Failed to init schema for Delta Lake table {}.{}", db.getFullName(), name, e);
            return Optional.empty();
        }
    }

    @Override
    public TTableDescriptor toThrift() {
        THiveTable tHiveTable = new THiveTable(db.getRemoteName(), getRemoteName(),
                Collections.emptyMap());
        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.HIVE_TABLE,
                getFullSchema().size(), 0, getName(), db.getFullName());
        tTableDescriptor.setHiveTable(tHiveTable);
        return tTableDescriptor;
    }

    @Override
    public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
        makeSureInitialized();
        return new ExternalAnalysisTask(info);
    }

    /**
     * Returns the partition column names from Delta Lake metadata.
     */
    public List<String> getPartitionColumnNames() {
        try {
            AbstractDeltaLakeMetadataOps ops = getDeltaLakeMetadataOps();
            String tableLocation = ops.getTableLocation(db.getRemoteName(), getRemoteName());

            Engine engine = ops.getEngine();
            Table deltaTable = Table.forPath(engine, tableLocation);
            Snapshot snapshot = deltaTable.getLatestSnapshot(engine);

            return snapshot.getPartitionColumnNames();
        } catch (Exception e) {
            LOG.warn("Failed to get partition columns for Delta Lake table {}.{}", db.getFullName(), name, e);
            return Collections.emptyList();
        }
    }

    /**
     * Returns the location of this Delta Lake table.
     */
    public String getTableLocation() {
        AbstractDeltaLakeMetadataOps ops = getDeltaLakeMetadataOps();
        return ops.getTableLocation(db.getRemoteName(), getRemoteName());
    }

    private AbstractDeltaLakeMetadataOps getDeltaLakeMetadataOps() {
        return (AbstractDeltaLakeMetadataOps) catalog.getMetadataOps();
    }
}
