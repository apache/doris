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

package org.apache.doris.backup;

import org.apache.doris.analysis.BackupStmt.BackupContent;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.backup.RestoreFileMapping.IdChain;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OdbcCatalogResource;
import org.apache.doris.catalog.OdbcTable;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.View;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Version;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 * This is a memory structure mapping the job info file in repository.
 * It contains all content of a job info file.
 * It also be used to save the info of a restore job, such as alias of table and meta info file path
 */
public class BackupJobInfo implements Writable {
    private static final Logger LOG = LogManager.getLogger(BackupJobInfo.class);

    @SerializedName("name")
    public String name;
    @SerializedName("database")
    public String dbName;
    @SerializedName("id")
    public long dbId;
    @SerializedName("backup_time")
    public long backupTime;
    @SerializedName("content")
    public BackupContent content;
    // only include olap table
    @SerializedName("backup_objects")
    public Map<String, BackupOlapTableInfo> backupOlapTableObjects = Maps.newHashMap();
    // include other objects: view, external table
    @SerializedName("new_backup_objects")
    public NewBackupObjects newBackupObjects = new NewBackupObjects();
    public boolean success;
    @SerializedName("backup_result")
    public String successJson = "succeed";

    @SerializedName("meta_version")
    public int metaVersion;
    @SerializedName("major_version")
    public int majorVersion;
    @SerializedName("minor_version")
    public int minorVersion;
    @SerializedName("patch_version")
    public int patchVersion;

    @SerializedName("tablet_be_map")
    public Map<Long, Long> tabletBeMap = Maps.newHashMap();

    @SerializedName("tablet_snapshot_path_map")
    public Map<Long, String> tabletSnapshotPathMap = Maps.newHashMap();

    @SerializedName("table_commit_seq_map")
    public Map<Long, Long> tableCommitSeqMap;

    public static class ExtraInfo {
        public static class NetworkAddrss {
            @SerializedName("ip")
            public String ip;
            @SerializedName("port")
            public int port;
        }

        @SerializedName("be_network_map")
        public Map<Long, NetworkAddrss> beNetworkMap = Maps.newHashMap();

        @SerializedName("token")
        public String token;
    }

    @SerializedName("extra_info")
    public ExtraInfo extraInfo;


    // This map is used to save the table alias mapping info when processing a restore job.
    // origin -> alias
    public Map<String, String> tblAlias = Maps.newHashMap();

    public long getBackupTime() {
        return backupTime;
    }

    public void initBackupJobInfoAfterDeserialize() {
        // transform success
        if (successJson.equals("succeed")) {
            success = true;
        } else {
            success = false;
        }

        // init meta version
        if (metaVersion == 0) {
            // meta_version does not exist
            metaVersion = FeConstants.meta_version;
        }

        // init olap table info
        for (BackupOlapTableInfo backupOlapTableInfo : backupOlapTableObjects.values()) {
            for (BackupPartitionInfo backupPartitionInfo : backupOlapTableInfo.partitions.values()) {
                for (BackupIndexInfo backupIndexInfo : backupPartitionInfo.indexes.values()) {
                    List<Long> sortedTabletIds = backupIndexInfo.getSortedTabletIds();
                    for (Long tabletId : sortedTabletIds) {
                        List<String> files = backupIndexInfo.getTabletFiles(tabletId);
                        if (files == null) {
                            continue;
                        }
                        BackupTabletInfo backupTabletInfo = new BackupTabletInfo(tabletId, files);
                        backupIndexInfo.sortedTabletInfoList.add(backupTabletInfo);
                    }
                }
            }
        }
    }

    public Table.TableType getTypeByTblName(String tblName) {
        if (backupOlapTableObjects.containsKey(tblName)) {
            return Table.TableType.OLAP;
        }
        for (BackupViewInfo backupViewInfo : newBackupObjects.views) {
            if (backupViewInfo.name.equals(tblName)) {
                return Table.TableType.VIEW;
            }
        }
        for (BackupOdbcTableInfo backupOdbcTableInfo : newBackupObjects.odbcTables) {
            if (backupOdbcTableInfo.dorisTableName.equals(tblName)) {
                return Table.TableType.ODBC;
            }
        }
        return null;
    }

    public BackupOlapTableInfo getOlapTableInfo(String tblName) {
        return backupOlapTableObjects.get(tblName);
    }

    public void removeTable(TableRef tableRef, TableType tableType) {
        switch (tableType) {
            case OLAP:
                removeOlapTable(tableRef);
                break;
            case VIEW:
                removeView(tableRef);
                break;
            case ODBC:
                removeOdbcTable(tableRef);
                break;
            default:
                break;
        }
    }

    public void removeOlapTable(TableRef tableRef) {
        String tblName = tableRef.getName().getTbl();
        BackupOlapTableInfo tblInfo = backupOlapTableObjects.get(tblName);
        if (tblInfo == null) {
            LOG.info("Ignore error: exclude table " + tblName + " does not exist in snapshot " + name);
            return;
        }
        PartitionNames partitionNames = tableRef.getPartitionNames();
        if (partitionNames == null) {
            backupOlapTableObjects.remove(tblInfo);
            return;
        }
        // check the selected partitions
        for (String partName : partitionNames.getPartitionNames()) {
            if (tblInfo.containsPart(partName)) {
                tblInfo.partitions.remove(partName);
            } else {
                LOG.info("Ignore error: exclude partition " + partName + " of table " + tblName
                        + " does not exist in snapshot");
            }
        }
    }

    public void removeView(TableRef tableRef) {
        Iterator<BackupViewInfo> iter = newBackupObjects.views.listIterator();
        while (iter.hasNext()) {
            if (iter.next().name.equals(tableRef.getName().getTbl())) {
                iter.remove();
                return;
            }
        }
    }

    public void removeOdbcTable(TableRef tableRef) {
        Iterator<BackupOdbcTableInfo> iter = newBackupObjects.odbcTables.listIterator();
        while (iter.hasNext()) {
            BackupOdbcTableInfo backupOdbcTableInfo = iter.next();
            if (backupOdbcTableInfo.dorisTableName.equals(tableRef.getName().getTbl())) {
                if (backupOdbcTableInfo.resourceName != null) {
                    Iterator<BackupOdbcResourceInfo> resourceIter = newBackupObjects.odbcResources.listIterator();
                    while (resourceIter.hasNext()) {
                        if (resourceIter.next().name.equals(backupOdbcTableInfo.resourceName)) {
                            resourceIter.remove();
                        }
                    }
                }
                iter.remove();
                return;
            }
        }
    }

    public void retainOlapTables(Set<String> tblNames) {
        Iterator<Map.Entry<String, BackupOlapTableInfo>> iter = backupOlapTableObjects.entrySet().iterator();
        while (iter.hasNext()) {
            if (!tblNames.contains(iter.next().getKey())) {
                iter.remove();
            }
        }
    }

    public void retainView(Set<String> viewNames) {
        Iterator<BackupViewInfo> iter = newBackupObjects.views.listIterator();
        while (iter.hasNext()) {
            if (!viewNames.contains(iter.next().name)) {
                iter.remove();
            }
        }
    }

    public void retainOdbcTables(Set<String> odbcTableNames) {
        Iterator<BackupOdbcTableInfo> odbcIter = newBackupObjects.odbcTables.listIterator();
        Set<String> removedOdbcResourceNames = Sets.newHashSet();
        while (odbcIter.hasNext()) {
            BackupOdbcTableInfo backupOdbcTableInfo = odbcIter.next();
            if (!odbcTableNames.contains(backupOdbcTableInfo.dorisTableName)) {
                removedOdbcResourceNames.add(backupOdbcTableInfo.resourceName);
                odbcIter.remove();
            }
        }
        Iterator<BackupOdbcResourceInfo> resourceIter = newBackupObjects.odbcResources.listIterator();
        while (resourceIter.hasNext()) {
            if (removedOdbcResourceNames.contains(resourceIter.next().name)) {
                resourceIter.remove();
            }
        }
    }

    public void setAlias(String orig, String alias) {
        tblAlias.put(orig, alias);
    }

    public String getAliasByOriginNameIfSet(String orig) {
        return tblAlias.containsKey(orig) ? tblAlias.get(orig) : orig;
    }

    public String getOrginNameByAlias(String alias) {
        for (Map.Entry<String, String> entry : tblAlias.entrySet()) {
            if (entry.getValue().equals(alias)) {
                return entry.getKey();
            }
        }
        return alias;
    }

    public static class BriefBackupJobInfo {
        @SerializedName("name")
        public String name;
        @SerializedName("database")
        public String database;
        @SerializedName("backup_time")
        public long backupTime;
        @SerializedName("content")
        public BackupContent content;
        @SerializedName("olap_table_list")
        public List<BriefBackupOlapTable> olapTableList = Lists.newArrayList();
        @SerializedName("view_list")
        public List<BackupViewInfo> viewList = Lists.newArrayList();
        @SerializedName("odbc_table_list")
        public List<BackupOdbcTableInfo> odbcTableList = Lists.newArrayList();
        @SerializedName("odbc_resource_list")
        public List<BackupOdbcResourceInfo> odbcResourceList = Lists.newArrayList();

        public static BriefBackupJobInfo fromBackupJobInfo(BackupJobInfo backupJobInfo) {
            BriefBackupJobInfo briefBackupJobInfo = new BriefBackupJobInfo();
            briefBackupJobInfo.name = backupJobInfo.name;
            briefBackupJobInfo.database = backupJobInfo.dbName;
            briefBackupJobInfo.backupTime = backupJobInfo.backupTime;
            briefBackupJobInfo.content = backupJobInfo.content;
            for (Map.Entry<String, BackupOlapTableInfo> olapTableEntry :
                    backupJobInfo.backupOlapTableObjects.entrySet()) {
                BriefBackupOlapTable briefBackupOlapTable = new BriefBackupOlapTable();
                briefBackupOlapTable.name = olapTableEntry.getKey();
                briefBackupOlapTable.partitionNames = Lists.newArrayList(olapTableEntry.getValue().partitions.keySet());
                briefBackupJobInfo.olapTableList.add(briefBackupOlapTable);
            }
            briefBackupJobInfo.viewList = backupJobInfo.newBackupObjects.views;
            briefBackupJobInfo.odbcTableList = backupJobInfo.newBackupObjects.odbcTables;
            briefBackupJobInfo.odbcResourceList = backupJobInfo.newBackupObjects.odbcResources;
            return briefBackupJobInfo;
        }
    }

    public static class BriefBackupOlapTable {
        @SerializedName("name")
        public String name;
        @SerializedName("partition_names")
        public List<String> partitionNames;
    }

    public static class NewBackupObjects {
        @SerializedName("views")
        public List<BackupViewInfo> views = Lists.newArrayList();
        @SerializedName("odbc_tables")
        public List<BackupOdbcTableInfo> odbcTables = Lists.newArrayList();
        @SerializedName("odbc_resources")
        public List<BackupOdbcResourceInfo> odbcResources = Lists.newArrayList();
    }

    public static class BackupOlapTableInfo {
        @SerializedName("id")
        public long id;
        @SerializedName("partitions")
        public Map<String, BackupPartitionInfo> partitions = Maps.newHashMap();

        public boolean containsPart(String partName) {
            return partitions.containsKey(partName);
        }

        public BackupPartitionInfo getPartInfo(String partName) {
            return partitions.get(partName);
        }

        public void retainPartitions(Collection<String> partNames) {
            if (partNames == null || partNames.isEmpty()) {
                // retain all
                return;
            }
            Iterator<Map.Entry<String, BackupPartitionInfo>> iter = partitions.entrySet().iterator();
            while (iter.hasNext()) {
                if (!partNames.contains(iter.next().getKey())) {
                    iter.remove();
                }
            }
        }
    }

    public static class BackupPartitionInfo {
        @SerializedName("id")
        public long id;
        @SerializedName("version")
        public long version;
        @SerializedName("indexes")
        public Map<String, BackupIndexInfo> indexes = Maps.newHashMap();

        public BackupIndexInfo getIdx(String idxName) {
            return indexes.get(idxName);
        }
    }

    public static class BackupIndexInfo {
        @SerializedName("id")
        public long id;
        @SerializedName("schema_hash")
        public int schemaHash;
        @SerializedName("tablets")
        public Map<Long, List<String>> tablets = Maps.newHashMap();
        @SerializedName("tablets_order")
        public List<Long> tabletsOrder = Lists.newArrayList();
        public List<BackupTabletInfo> sortedTabletInfoList = Lists.newArrayList();

        public List<String> getTabletFiles(long tabletId) {
            return tablets.get(tabletId);
        }

        private List<Long> getSortedTabletIds() {
            if (tabletsOrder == null || tabletsOrder.isEmpty()) {
                // in previous version, we are not saving tablets order(which was a BUG),
                // so we have to sort the tabletIds to restore the original order of tablets.
                List<Long> tmpList = Lists.newArrayList(tablets.keySet());
                tmpList.sort((o1, o2) -> Long.valueOf(o1).compareTo(Long.valueOf(o2)));
                return tmpList;
            } else {
                return tabletsOrder;
            }
        }
    }

    public static class BackupTabletInfo {
        @SerializedName("id")
        public long id;
        @SerializedName("files")
        public List<String> files;

        public BackupTabletInfo(long id, List<String> files) {
            this.id = id;
            this.files = files;
        }
    }

    public static class BackupViewInfo {
        @SerializedName("id")
        public long id;
        @SerializedName("name")
        public String name;
    }

    public static class BackupOdbcTableInfo {
        @SerializedName("id")
        public long id;
        @SerializedName("doris_table_name")
        public String dorisTableName;
        @SerializedName("linked_odbc_database_name")
        public String linkedOdbcDatabaseName;
        @SerializedName("linked_odbc_table_name")
        public String linkedOdbcTableName;
        @SerializedName("resource_name")
        public String resourceName;
        @SerializedName("host")
        public String host;
        @SerializedName("port")
        public String port;
        @SerializedName("user")
        public String user;
        @SerializedName("driver")
        public String driver;
        @SerializedName("odbc_type")
        public String odbcType;
    }

    public static class BackupOdbcResourceInfo {
        @SerializedName("name")
        public String name;
    }

    // eg: __db_10001/__tbl_10002/__part_10003/__idx_10002/__10004
    public String getFilePath(String db, String tbl, String part, String idx, long tabletId) {
        if (!db.equalsIgnoreCase(dbName)) {
            LOG.debug("db name does not equal: {}-{}", dbName, db);
            return null;
        }

        BackupOlapTableInfo tblInfo = backupOlapTableObjects.get(tbl);
        if (tblInfo == null) {
            LOG.debug("tbl {} does not exist", tbl);
            return null;
        }

        BackupPartitionInfo partInfo = tblInfo.getPartInfo(part);
        if (partInfo == null) {
            LOG.debug("part {} does not exist", part);
            return null;
        }

        BackupIndexInfo idxInfo = partInfo.getIdx(idx);
        if (idxInfo == null) {
            LOG.debug("idx {} does not exist", idx);
            return null;
        }

        List<String> pathSeg = Lists.newArrayList();
        pathSeg.add(Repository.PREFIX_DB + dbId);
        pathSeg.add(Repository.PREFIX_TBL + tblInfo.id);
        pathSeg.add(Repository.PREFIX_PART + partInfo.id);
        pathSeg.add(Repository.PREFIX_IDX + idxInfo.id);
        pathSeg.add(Repository.PREFIX_COMMON + tabletId);

        return Joiner.on("/").join(pathSeg);
    }

    // eg: __db_10001/__tbl_10002/__part_10003/__idx_10002/__10004
    public String getFilePath(IdChain ids) {
        List<String> pathSeg = Lists.newArrayList();
        pathSeg.add(Repository.PREFIX_DB + dbId);
        pathSeg.add(Repository.PREFIX_TBL + ids.getTblId());
        pathSeg.add(Repository.PREFIX_PART + ids.getPartId());
        pathSeg.add(Repository.PREFIX_IDX + ids.getIdxId());
        pathSeg.add(Repository.PREFIX_COMMON + ids.getTabletId());

        return Joiner.on("/").join(pathSeg);
    }

    // struct TRemoteTabletSnapshot {
    //     1: optional i64 local_tablet_id
    //     2: optional string local_snapshot_path
    //     3: optional i64 remote_tablet_id
    //     4: optional i64 remote_be_id
    //     5: optional Types.TSchemaHash schema_hash
    //     6: optional Types.TNetworkAddress remote_be_addr
    //     7: optional string remote_snapshot_path
    //     8: optional string token
    // }

    public String getTabletSnapshotPath(Long tabletId) {
        return tabletSnapshotPathMap.get(tabletId);
    }

    public Long getBeId(Long tabletId) {
        return tabletBeMap.get(tabletId);
    }

    public String getToken() {
        return extraInfo.token;
    }

    public TNetworkAddress getBeAddr(Long beId) {
        ExtraInfo.NetworkAddrss addr = extraInfo.beNetworkMap.get(beId);
        if (addr == null) {
            return null;
        }

        return new TNetworkAddress(addr.ip, addr.port);
    }

    // TODO(Drogon): improve this find perfermance
    public Long getSchemaHash(long tableId, long partitionId, long indexId) {
        for (BackupOlapTableInfo backupOlapTableInfo : backupOlapTableObjects.values()) {
            if (backupOlapTableInfo.id != tableId) {
                continue;
            }

            for (BackupPartitionInfo backupPartitionInfo : backupOlapTableInfo.partitions.values()) {
                if (backupPartitionInfo.id != partitionId) {
                    continue;
                }

                for (BackupIndexInfo backupIndexInfo : backupPartitionInfo.indexes.values()) {
                    if (backupIndexInfo.id != indexId) {
                        continue;
                    }

                    return Long.valueOf(backupIndexInfo.schemaHash);
                }
            }
        }
        return null;
    }

    public static BackupJobInfo fromCatalog(long backupTime, String label, String dbName, long dbId,
                                            BackupContent content, BackupMeta backupMeta,
                                            Map<Long, SnapshotInfo> snapshotInfos, Map<Long, Long> tableCommitSeqMap) {

        BackupJobInfo jobInfo = new BackupJobInfo();
        jobInfo.backupTime = backupTime;
        jobInfo.name = label;
        jobInfo.dbName = dbName;
        jobInfo.dbId = dbId;
        jobInfo.metaVersion = FeConstants.meta_version;
        jobInfo.content = content;
        jobInfo.tableCommitSeqMap = tableCommitSeqMap;
        jobInfo.majorVersion = Version.DORIS_BUILD_VERSION_MAJOR;
        jobInfo.minorVersion = Version.DORIS_BUILD_VERSION_MINOR;
        jobInfo.patchVersion = Version.DORIS_BUILD_VERSION_PATCH;

        Collection<Table> tbls = backupMeta.getTables().values();
        // tbls
        for (Table tbl : tbls) {
            if (tbl instanceof OlapTable) {
                OlapTable olapTbl = (OlapTable) tbl;
                BackupOlapTableInfo tableInfo = new BackupOlapTableInfo();
                tableInfo.id = tbl.getId();
                jobInfo.backupOlapTableObjects.put(tbl.getName(), tableInfo);
                // partitions
                for (Partition partition : olapTbl.getPartitions()) {
                    BackupPartitionInfo partitionInfo = new BackupPartitionInfo();
                    partitionInfo.id = partition.getId();
                    partitionInfo.version = partition.getVisibleVersion();
                    tableInfo.partitions.put(partition.getName(), partitionInfo);
                    // indexes
                    for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                        BackupIndexInfo idxInfo = new BackupIndexInfo();
                        idxInfo.id = index.getId();
                        idxInfo.schemaHash = olapTbl.getSchemaHashByIndexId(index.getId());
                        partitionInfo.indexes.put(olapTbl.getIndexNameById(index.getId()), idxInfo);
                        // tablets
                        if (content == BackupContent.METADATA_ONLY) {
                            for (Tablet tablet : index.getTablets()) {
                                idxInfo.tablets.put(tablet.getId(), Lists.newArrayList());
                            }
                        } else {
                            for (Tablet tablet : index.getTablets()) {
                                SnapshotInfo snapshotInfo = snapshotInfos.get(tablet.getId());
                                idxInfo.tablets.put(tablet.getId(),
                                        Lists.newArrayList(snapshotInfo.getFiles()));
                                jobInfo.tabletBeMap.put(tablet.getId(), snapshotInfo.getBeId());
                                jobInfo.tabletSnapshotPathMap.put(tablet.getId(), snapshotInfo.getPath());
                            }
                        }
                        idxInfo.tabletsOrder.addAll(index.getTabletIdsInOrder());
                    }
                }
            } else if (tbl instanceof View) {
                View view = (View) tbl;
                BackupViewInfo backupViewInfo = new BackupViewInfo();
                backupViewInfo.id = view.getId();
                backupViewInfo.name = view.getName();
                jobInfo.newBackupObjects.views.add(backupViewInfo);
            } else if (tbl instanceof OdbcTable) {
                OdbcTable odbcTable = (OdbcTable) tbl;
                BackupOdbcTableInfo backupOdbcTableInfo = new BackupOdbcTableInfo();
                backupOdbcTableInfo.id = odbcTable.getId();
                backupOdbcTableInfo.dorisTableName = odbcTable.getName();
                backupOdbcTableInfo.linkedOdbcDatabaseName = odbcTable.getOdbcDatabaseName();
                backupOdbcTableInfo.linkedOdbcTableName = odbcTable.getOdbcTableName();
                if (odbcTable.getOdbcCatalogResourceName() != null) {
                    backupOdbcTableInfo.resourceName = odbcTable.getOdbcCatalogResourceName();
                } else {
                    backupOdbcTableInfo.host = odbcTable.getHost();
                    backupOdbcTableInfo.port = odbcTable.getPort();
                    backupOdbcTableInfo.user = odbcTable.getUserName();
                    backupOdbcTableInfo.driver = odbcTable.getOdbcDriver();
                    backupOdbcTableInfo.odbcType = odbcTable.getOdbcTableTypeName();
                }
                jobInfo.newBackupObjects.odbcTables.add(backupOdbcTableInfo);
            }
        }
        // resources
        Collection<Resource> resources = backupMeta.getResourceNameMap().values();
        for (Resource resource : resources) {
            if (resource instanceof OdbcCatalogResource) {
                OdbcCatalogResource odbcCatalogResource = (OdbcCatalogResource) resource;
                BackupOdbcResourceInfo backupOdbcResourceInfo = new BackupOdbcResourceInfo();
                backupOdbcResourceInfo.name = odbcCatalogResource.getName();
                jobInfo.newBackupObjects.odbcResources.add(backupOdbcResourceInfo);
            }
        }

        return jobInfo;
    }

    public static BackupJobInfo fromFile(String path) throws IOException {
        byte[] bytes = Files.readAllBytes(Paths.get(path));
        String json = new String(bytes, StandardCharsets.UTF_8);
        return genFromJson(json);
    }

    public static BackupJobInfo genFromJson(String json) {
        /* parse the json string:
         * {
         *   "backup_time": 1522231864000,
         *   "name": "snapshot1",
         *   "database": "db1"
         *   "id": 10000
         *   "backup_result": "succeed",
         *   "meta_version" : 40 // this is optional
         *   "backup_objects": {
         *       "table1": {
         *           "partitions": {
         *               "partition2": {
         *                   "indexes": {
         *                       "rollup1": {
         *                           "id": 10009,
         *                           "schema_hash": 3473401,
         *                           "tablets": {
         *                               "10008": ["__10029_seg1.dat", "__10029_seg2.dat"],
         *                               "10007": ["__10030_seg1.dat", "__10030_seg2.dat"]
         *                           },
         *                           "tablets_order": ["10007", "10008"]
         *                       },
         *                       "table1": {
         *                           "id": 10008,
         *                           "schema_hash": 9845021,
         *                           "tablets": {
         *                               "10004": ["__10027_seg1.dat", "__10027_seg2.dat"],
         *                               "10005": ["__10028_seg1.dat", "__10028_seg2.dat"]
         *                           },
         *                           "tablets_order": ["10004, "10005"]
         *                       }
         *                   },
         *                   "id": 10007
         *                   "version": 10
         *                   "version_hash": 1273047329538
         *               },
         *           },
         *           "id": 10001
         *       }
         *   },
         *   "new_backup_objects": {
         *       "views": [
         *           {"id": 1,
         *            "name": "view1"
         *           }
         *       ],
         *       "odbc_tables": [
         *           {"id": 2,
         *            "doris_table_name": "oracle1",
         *            "linked_odbc_database_name": "external_db1",
         *            "linked_odbc_table_name": "external_table1",
         *            "resource_name": "bj_oracle"
         *           }
         *       ],
         *       "odbc_resources": [
         *           {"name": "bj_oracle"}
         *       ]
         *   }
         * }
         */
        BackupJobInfo jobInfo = GsonUtils.GSON.fromJson(json, BackupJobInfo.class);
        jobInfo.initBackupJobInfoAfterDeserialize();
        return jobInfo;
    }


    public void writeToFile(File jobInfoFile) throws FileNotFoundException {
        PrintWriter printWriter = new PrintWriter(jobInfoFile);
        try {
            printWriter.print(toJson(false));
            printWriter.flush();
        } finally {
            printWriter.close();
        }
    }

    // Only return basic info, table and partitions
    public String getBrief() {
        BriefBackupJobInfo briefBackupJobInfo = BriefBackupJobInfo.fromBackupJobInfo(this);
        Gson gson = GsonUtils.GSON_PRETTY_PRINTING;
        return gson.toJson(briefBackupJobInfo);
    }

    public String toJson(boolean prettyPrinting) {
        Gson gson;
        if (prettyPrinting) {
            gson = GsonUtils.GSON_PRETTY_PRINTING;
        } else {
            gson = GsonUtils.GSON;
        }
        return gson.toJson(this);
    }

    public String getInfo() {
        return getBrief();
    }

    public static BackupJobInfo read(DataInput in) throws IOException {
        return BackupJobInfo.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, toJson(false));
        out.writeInt(tblAlias.size());
        for (Map.Entry<String, String> entry : tblAlias.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }
    }

    public static BackupJobInfo readFields(DataInput in) throws IOException {
        String json = Text.readString(in);
        BackupJobInfo backupJobInfo = genFromJson(json);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String tbl = Text.readString(in);
            String alias = Text.readString(in);
            backupJobInfo.tblAlias.put(tbl, alias);
        }
        return backupJobInfo;
    }

    public String toString() {
        return toJson(true);
    }
}
