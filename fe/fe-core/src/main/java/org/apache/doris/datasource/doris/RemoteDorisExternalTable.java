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

package org.apache.doris.datasource.doris;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.statistics.analysis.AnalysisInfo;
import org.apache.doris.statistics.analysis.BaseAnalysisTask;
import org.apache.doris.statistics.analysis.ExternalAnalysisTask;
import org.apache.doris.thrift.TRemoteDorisTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

public class RemoteDorisExternalTable extends ExternalTable {
    private static final Logger LOG = LogManager.getLogger(RemoteDorisExternalTable.class);
    private volatile List<Partition> partitions = Lists.newArrayList();
    private volatile List<Partition> tempPartitions = Lists.newArrayList();
    private volatile long tableId = -1;
    private transient FutureTask<RemoteOlapTable> currentRefreshTask;

    public RemoteDorisExternalTable(long id, String name, String remoteName,
            RemoteDorisExternalCatalog catalog, ExternalDatabase db) {
        super(id, name, remoteName, catalog, db, TableType.DORIS_EXTERNAL_TABLE);
    }

    @Override
    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            objectCreated = true;
        }
    }

    private RemoteOlapTable getDorisOlapTable() {
        FutureTask<RemoteOlapTable> refreshTask;
        boolean shouldRun;
        synchronized (this) {
            if (currentRefreshTask == null || currentRefreshTask.isDone()) {
                currentRefreshTask = new FutureTask<>(this::loadDorisOlapTable);
                shouldRun = true;
            } else {
                shouldRun = false;
            }
            refreshTask = currentRefreshTask;
        }

        if (shouldRun) {
            refreshTask.run();
        }
        return getRefreshResult(refreshTask);
    }

    private RemoteOlapTable loadDorisOlapTable() {
        try {
            List<Partition> cachedPartitions = Lists.newArrayList(partitions);
            List<Partition> cachedTempPartitions = Lists.newArrayList(tempPartitions);
            RemoteOlapTable olapTable = ((RemoteDorisExternalCatalog) catalog).getFeServiceClient()
                    .getOlapTable(dbName, remoteName, tableId, cachedPartitions, cachedTempPartitions);
            olapTable.setCatalog((RemoteDorisExternalCatalog) catalog);
            olapTable.setDatabase((RemoteDorisExternalDatabase) db);

            tableId = olapTable.getId();
            partitions = Lists.newArrayList(olapTable.getPartitions());
            tempPartitions = Lists.newArrayList(olapTable.getTempPartitions().getPartitions());

            olapTable.setId(id); // change id in case of possible conflicts
            olapTable.invalidateBackendsIfNeed();
            return olapTable;
        } catch (RuntimeException e) {
            LOG.warn("Failed to get remote doris olap table: {}.{}", dbName, remoteName, e);
            throw e;
        }
    }

    private RemoteOlapTable getRefreshResult(FutureTask<RemoteOlapTable> refreshTask) {
        try {
            // The underlying Thrift RPC has its own timeout; this only waits for the shared refresh result.
            return refreshTask.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AnalysisException("interrupted while getting doris olap table", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw new AnalysisException(
                    "failed to get remote doris olap table: " + Util.getRootCauseMessage(cause),
                    cause);
        }
    }

    public OlapTable getOlapTable() {
        makeSureInitialized();
        return getDorisOlapTable();
    }

    public boolean useArrowFlight() {
        return ((RemoteDorisExternalCatalog) catalog).useArrowFlight();
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        TRemoteDorisTable tRemoteDorisTable = new TRemoteDorisTable();
        tRemoteDorisTable.setDbName(dbName);
        tRemoteDorisTable.setTableName(name);
        tRemoteDorisTable.setProperties(getCatalog().getProperties());

        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(),
                TTableType.REMOTE_DORIS_TABLE, schema.size(), 0, getName(), dbName);

        tTableDescriptor.setRemoteDorisTable(tRemoteDorisTable);
        return tTableDescriptor;
    }

    @Override
    public Optional<SchemaCacheValue> initSchema() {
        RemoteDorisRestClient restClient = ((RemoteDorisExternalCatalog) catalog).getDorisRestClient();
        return Optional.of(new SchemaCacheValue(restClient.getColumns(dbName, name)));
    }

    @Override
    public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
        makeSureInitialized();
        return new ExternalAnalysisTask(info);
    }

    @Override
    public long fetchRowCount() {
        RemoteDorisRestClient restClient = ((RemoteDorisExternalCatalog) catalog).getDorisRestClient();
        return restClient.getRowCount(getDbName(), getName());
    }

    public String getExternalTableName() {
        return getDbName() + "." + getName();
    }
}
