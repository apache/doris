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

package org.apache.doris.datasource.lance;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.lance.index.LancePhysicalIndexEntry;
import org.apache.doris.datasource.lance.index.LanceShowIndexInfo;
import org.apache.doris.datasource.lance.metadata.LanceMvccSnapshot;
import org.apache.doris.datasource.lance.metadata.LanceSchemaHelper;
import org.apache.doris.datasource.lance.metadata.LanceTableMetadata;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ExternalAnalysisTask;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.namespace.model.DescribeTableResponse;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class LanceExternalTable extends ExternalTable implements MvccTable {
    public LanceExternalTable(long id, String name, String remoteName, LanceExternalCatalog catalog,
            LanceExternalDatabase db) {
        super(id, name, remoteName, catalog, db, TableType.LANCE_EXTERNAL_TABLE);
    }

    @Override
    public Optional<SchemaCacheValue> initSchema() {
        Schema schema = ((LanceExternalCatalog) catalog).loadTableSchema(db.getRemoteName(), remoteName);
        return Optional.of(new SchemaCacheValue(LanceSchemaHelper.toDorisColumns(schema)));
    }

    public Map<String, String> getTableProperties() {
        DescribeTableResponse table = ((LanceExternalCatalog) catalog).describeTable(
                db.getRemoteName(), remoteName);
        return table.getProperties() == null
                ? Collections.emptyMap()
                : new HashMap<>(table.getProperties());
    }

    public LanceTableMetadata loadMetadata() {
        return ((LanceExternalCatalog) catalog).loadTableMetadata(db.getRemoteName(), remoteName);
    }

    public LanceTableMetadata loadMetadataForSearch() {
        return ((LanceExternalCatalog) catalog).loadTableMetadataForSearch(
                db.getRemoteName(), remoteName);
    }

    public LanceTableMetadata loadBasicMetadata() {
        return ((LanceExternalCatalog) catalog).loadBasicTableMetadata(db.getRemoteName(), remoteName);
    }

    public List<LanceShowIndexInfo> loadIndexesForShow() throws AnalysisException {
        return ((LanceExternalCatalog) catalog).loadTableIndexesForShow(
                db.getRemoteName(), remoteName);
    }

    public List<LancePhysicalIndexEntry> loadIndexEntries() throws AnalysisException {
        return ((LanceExternalCatalog) catalog).loadTableIndexEntries(
                db.getRemoteName(), remoteName);
    }

    private LanceTableMetadata loadMetadata(Optional<TableSnapshot> tableSnapshot) {
        return ((LanceExternalCatalog) catalog).loadTableMetadata(
                db.getRemoteName(), remoteName, tableSnapshot);
    }

    public LanceTableMetadata getMetadata(Optional<MvccSnapshot> snapshot) {
        if (snapshot.isPresent()) {
            return ((LanceMvccSnapshot) snapshot.get()).getMetadata();
        }
        return loadMetadata();
    }

    @Override
    public MvccSnapshot loadSnapshot(Optional<TableSnapshot> tableSnapshot,
            Optional<TableScanParams> scanParams) {
        return new LanceMvccSnapshot(loadMetadata(tableSnapshot));
    }

    @Override
    public List<Column> getFullSchema() {
        Optional<MvccSnapshot> snapshot = MvccUtil.getSnapshotForTableMetadataFromContext(this);
        if (snapshot.isPresent()) {
            return getFullSchema(snapshot);
        }
        return super.getFullSchema();
    }

    @Override
    public List<Column> getFullSchema(Optional<MvccSnapshot> snapshot) {
        if (snapshot.isPresent()) {
            return LanceSchemaHelper.toDorisColumns(getMetadata(snapshot).getSchema());
        }
        return getFullSchema();
    }

    @Override
    public long fetchRowCount() {
        long rowCount = getMetadata(MvccUtil.getSnapshotForTableMetadataFromContext(this)).getRowCount();
        return rowCount > 0 ? rowCount : UNKNOWN_ROW_COUNT;
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        THiveTable thriftTable = new THiveTable(dbName, name, new HashMap<>());
        TTableDescriptor descriptor = new TTableDescriptor(getId(), TTableType.HIVE_TABLE,
                schema.size(), 0, getName(), dbName);
        descriptor.setHiveTable(thriftTable);
        return descriptor;
    }

    @Override
    public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
        makeSureInitialized();
        return new ExternalAnalysisTask(info);
    }
}
