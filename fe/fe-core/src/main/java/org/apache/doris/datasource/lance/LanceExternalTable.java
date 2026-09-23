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
import org.apache.doris.datasource.lance.metadata.LanceRefSelector;
import org.apache.doris.datasource.lance.metadata.LanceSchemaHelper;
import org.apache.doris.datasource.lance.metadata.LanceSnapshotResolver;
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
import org.apache.commons.lang3.StringUtils;

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

    public LanceTableMetadata getMetadata(Optional<MvccSnapshot> snapshot) {
        if (snapshot.isPresent()) {
            return ((LanceMvccSnapshot) snapshot.get()).getMetadata();
        }
        return loadMetadata();
    }

    @Override
    public MvccSnapshot loadSnapshot(Optional<TableSnapshot> tableSnapshot,
            Optional<TableScanParams> scanParams) {
        // As for Iceberg and Paimon tables, a non-numeric FOR VERSION AS OF names a tag.
        boolean versionIsTag = tableSnapshot.isPresent()
                && tableSnapshot.get().getType() == TableSnapshot.VersionType.VERSION
                && !LanceSnapshotResolver.isVersionNumber(tableSnapshot.get().getValue());
        LanceRefSelector selector = versionIsTag
                ? LanceRefSelector.tag(tableSnapshot.get().getValue()) : LanceRefSelector.snapshot(tableSnapshot);
        if (scanParams.isPresent()) {
            TableScanParams params = scanParams.get();
            if (params.isBranch()) {
                String branch = refName(params);
                // Lance calls the main chain "main"; it lives at the table root, not under tree/.
                if (LanceCatalogClient.MAIN_BRANCH.equals(branch)) {
                    // selector stays the main-chain one, including a tag named in FOR VERSION AS OF.
                } else if (versionIsTag) {
                    throw new IllegalArgumentException("Lance table " + getName() + ": FOR VERSION AS OF '"
                            + tableSnapshot.get().getValue() + "' names a tag, which cannot be combined with @branch;"
                            + " use @tag(...) or a numeric version");
                } else {
                    selector = LanceRefSelector.branch(branch, tableSnapshot);
                }
            } else if (params.isTag()) {
                if (tableSnapshot.isPresent()) {
                    throw new IllegalArgumentException("Lance table " + getName()
                            + ": @tag cannot be combined with FOR VERSION AS OF or FOR TIME AS OF");
                }
                selector = LanceRefSelector.tag(refName(params));
            } else {
                // Silently reading the latest version instead would return wrong data.
                throw new IllegalArgumentException("Lance table " + getName() + " does not support @"
                        + params.getParamType() + "; use @branch, @tag, FOR VERSION AS OF or FOR TIME AS OF");
            }
        }
        return new LanceMvccSnapshot(((LanceExternalCatalog) catalog).loadTableMetadata(
                db.getRemoteName(), remoteName, selector));
    }

    /**
     * {@code tbl@tag(name)} arrives as a list parameter, {@code tbl@tag('name'='x')} as a map;
     * anything else, such as extra keys or arguments, is rejected rather than ignored.
     */
    private static String refName(TableScanParams params) {
        String usage = "Lance @" + params.getParamType() + " takes exactly one name, as @"
                + params.getParamType() + "(x) or @" + params.getParamType() + "('" + TableScanParams.PARAMS_NAME
                + "'='x')";
        Map<String, String> map = params.getMapParams();
        List<String> list = params.getListParams();
        String name;
        if (!map.isEmpty()) {
            if (!list.isEmpty() || map.size() != 1 || !map.containsKey(TableScanParams.PARAMS_NAME)) {
                throw new IllegalArgumentException(usage);
            }
            name = map.get(TableScanParams.PARAMS_NAME);
        } else {
            if (list.size() != 1) {
                throw new IllegalArgumentException(usage);
            }
            name = list.get(0);
        }
        if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException(usage);
        }
        return name;
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
