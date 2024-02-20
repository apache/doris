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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.event.MetastoreEventsProcessor;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

/**
 * <pre>
 * ExternalMetaIdMgr is responsible for managing external meta ids.
 * Now it just manages the external meta ids of hms events,
 * but it will be extended to manage other external meta ids in the future.
 * </pre>
 * TODO: remove InitCatalogLog and InitDatabaseLog, manage external meta ids at ExternalMetaIdMgr
 */
public class ExternalMetaIdMgr {

    private static final Logger LOG = LogManager.getLogger(ExternalMetaIdMgr.class);

    public static final long META_ID_FOR_NOT_EXISTS = -1L;

    private final Map<Long, CtlMetaIdMgr> idToCtlMgr = Maps.newConcurrentMap();

    public ExternalMetaIdMgr() {
    }

    // invoke this method only on master
    public static long nextMetaId() {
        return Env.getCurrentEnv().getNextId();
    }

    // return the db id of the specified db, -1 means not exists
    public long getDbId(long catalogId, String dbName) {
        DbMetaIdMgr dbMetaIdMgr = getDbMetaIdMgr(catalogId, dbName);
        if (dbMetaIdMgr == null) {
            return META_ID_FOR_NOT_EXISTS;
        }
        return dbMetaIdMgr.dbId;
    }

    // return the tbl id of the specified tbl, -1 means not exists
    public long getTblId(long catalogId, String dbName, String tblName) {
        TblMetaIdMgr tblMetaIdMgr = getTblMetaIdMgr(catalogId, dbName, tblName);
        if (tblMetaIdMgr == null) {
            return META_ID_FOR_NOT_EXISTS;
        }
        return tblMetaIdMgr.tblId;
    }

    // return the partition id of the specified partition, -1 means not exists
    public long getPartitionId(long catalogId, String dbName,
                               String tblName, String partitionName) {
        PartitionMetaIdMgr partitionMetaIdMgr = getPartitionMetaIdMgr(catalogId, dbName, tblName, partitionName);
        if (partitionMetaIdMgr == null) {
            return META_ID_FOR_NOT_EXISTS;
        }
        return partitionMetaIdMgr.partitionId;
    }

    private @Nullable DbMetaIdMgr getDbMetaIdMgr(long catalogId, String dbName) {
        CtlMetaIdMgr ctlMetaIdMgr = idToCtlMgr.get(catalogId);
        if (ctlMetaIdMgr == null) {
            return null;
        }
        return ctlMetaIdMgr.dbNameToMgr.get(dbName);
    }

    private @Nullable TblMetaIdMgr getTblMetaIdMgr(long catalogId, String dbName, String tblName) {
        DbMetaIdMgr dbMetaIdMgr = getDbMetaIdMgr(catalogId, dbName);
        if (dbMetaIdMgr == null) {
            return null;
        }
        return dbMetaIdMgr.tblNameToMgr.get(tblName);
    }

    private PartitionMetaIdMgr getPartitionMetaIdMgr(long catalogId, String dbName,
                                                    String tblName, String partitionName) {
        TblMetaIdMgr tblMetaIdMgr = getTblMetaIdMgr(catalogId, dbName, tblName);
        if (tblMetaIdMgr == null) {
            return null;
        }
        return tblMetaIdMgr.partitionNameToMgr.get(partitionName);
    }

    public void replayMetaIdMappingsLog(@NotNull MetaIdMappingsLog log) {
        Preconditions.checkNotNull(log);
        long catalogId = log.getCatalogId();
        CtlMetaIdMgr ctlMetaIdMgr = idToCtlMgr.computeIfAbsent(catalogId, CtlMetaIdMgr::new);
        for (MetaIdMappingsLog.MetaIdMapping mapping : log.getMetaIdMappings()) {
            handleMetaIdMapping(mapping, ctlMetaIdMgr);
        }
        if (log.isFromHmsEvent()) {
            CatalogIf<?> catalogIf = Env.getCurrentEnv().getCatalogMgr().getCatalog(log.getCatalogId());
            if (catalogIf != null) {
                MetastoreEventsProcessor metastoreEventsProcessor = Env.getCurrentEnv().getMetastoreEventsProcessor();
                metastoreEventsProcessor.updateMasterLastSyncedEventId(
                            (HMSExternalCatalog) catalogIf, log.getLastSyncedEventId());
            }
        }
    }

    // no lock because the operations is serialized currently
    private void handleMetaIdMapping(MetaIdMappingsLog.MetaIdMapping mapping, CtlMetaIdMgr ctlMetaIdMgr) {
        MetaIdMappingsLog.OperationType opType = MetaIdMappingsLog.getOperationType(mapping.getOpType());
        MetaIdMappingsLog.MetaObjectType objType = MetaIdMappingsLog.getMetaObjectType(mapping.getMetaObjType());
        switch (opType) {
            case ADD:
                handleAddMetaIdMapping(mapping, ctlMetaIdMgr, objType);
                break;

            case DELETE:
                handleDelMetaIdMapping(mapping, ctlMetaIdMgr, objType);
                break;

            default:
                break;
        }
    }

    private static void handleDelMetaIdMapping(MetaIdMappingsLog.MetaIdMapping mapping,
                                               CtlMetaIdMgr ctlMetaIdMgr,
                                               MetaIdMappingsLog.MetaObjectType objType) {
        TblMetaIdMgr tblMetaIdMgr;
        DbMetaIdMgr dbMetaIdMgr;
        switch (objType) {
            case DATABASE:
                ctlMetaIdMgr.dbNameToMgr.remove(mapping.getDbName());
                break;

            case TABLE:
                dbMetaIdMgr = ctlMetaIdMgr.dbNameToMgr.get(mapping.getDbName());
                if (dbMetaIdMgr != null) {
                    dbMetaIdMgr.tblNameToMgr.remove(mapping.getTblName());
                }
                break;

            case PARTITION:
                dbMetaIdMgr = ctlMetaIdMgr.dbNameToMgr.get(mapping.getDbName());
                if (dbMetaIdMgr != null) {
                    tblMetaIdMgr = dbMetaIdMgr.tblNameToMgr.get(mapping.getTblName());
                    if (tblMetaIdMgr != null) {
                        tblMetaIdMgr.partitionNameToMgr.remove(mapping.getPartitionName());
                    }
                }
                break;

            default:
                break;
        }
    }

    private static void handleAddMetaIdMapping(MetaIdMappingsLog.MetaIdMapping mapping,
                                               CtlMetaIdMgr ctlMetaIdMgr,
                                               MetaIdMappingsLog.MetaObjectType objType) {
        DbMetaIdMgr dbMetaIdMgr;
        TblMetaIdMgr tblMetaIdMgr;
        switch (objType) {
            case DATABASE:
                ctlMetaIdMgr.dbNameToMgr.put(mapping.getDbName(),
                            new DbMetaIdMgr(mapping.getId(), mapping.getDbName()));
                break;

            case TABLE:
                dbMetaIdMgr = ctlMetaIdMgr.dbNameToMgr
                            .computeIfAbsent(mapping.getDbName(), DbMetaIdMgr::new);
                dbMetaIdMgr.tblNameToMgr.put(mapping.getTblName(),
                            new TblMetaIdMgr(mapping.getId(), mapping.getTblName()));
                break;

            case PARTITION:
                dbMetaIdMgr = ctlMetaIdMgr.dbNameToMgr
                            .computeIfAbsent(mapping.getDbName(), DbMetaIdMgr::new);
                tblMetaIdMgr = dbMetaIdMgr.tblNameToMgr
                            .computeIfAbsent(mapping.getTblName(), TblMetaIdMgr::new);
                tblMetaIdMgr.partitionNameToMgr.put(mapping.getPartitionName(),
                            new PartitionMetaIdMgr(mapping.getId(), mapping.getPartitionName()));
                break;

            default:
                break;
        }
    }

    public static class CtlMetaIdMgr {
        protected final long catalogId;

        protected CtlMetaIdMgr(long catalogId) {
            this.catalogId = catalogId;
        }

        protected Map<String, DbMetaIdMgr> dbNameToMgr = Maps.newConcurrentMap();
    }

    public static class DbMetaIdMgr {
        protected volatile long dbId = META_ID_FOR_NOT_EXISTS;
        protected final String dbName;

        protected DbMetaIdMgr(long dbId, String dbName) {
            this.dbId = dbId;
            this.dbName = dbName;
        }

        protected DbMetaIdMgr(String dbName) {
            this.dbName = dbName;
        }

        protected Map<String, TblMetaIdMgr> tblNameToMgr = Maps.newConcurrentMap();
    }

    public static class TblMetaIdMgr {
        protected volatile long tblId = META_ID_FOR_NOT_EXISTS;
        protected final String tblName;

        protected TblMetaIdMgr(long tblId, String tblName) {
            this.tblId = tblId;
            this.tblName = tblName;
        }

        protected TblMetaIdMgr(String tblName) {
            this.tblName = tblName;
        }

        protected Map<String, PartitionMetaIdMgr> partitionNameToMgr = Maps.newConcurrentMap();
    }

    public static class PartitionMetaIdMgr {
        protected volatile long partitionId = META_ID_FOR_NOT_EXISTS;
        protected final String partitionName;

        protected PartitionMetaIdMgr(long partitionId, String partitionName) {
            this.partitionId = partitionId;
            this.partitionName = partitionName;
        }

        protected PartitionMetaIdMgr(String partitionName) {
            this.partitionName = partitionName;
        }
    }
}
